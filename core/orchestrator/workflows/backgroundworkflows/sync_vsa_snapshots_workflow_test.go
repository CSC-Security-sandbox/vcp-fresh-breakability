package backgroundworkflows

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestSyncVSASnapshotsWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}
	syncSSWfCheck := &SyncSnapshotWFRunningCheck{}

	// Register activities
	env.RegisterActivity(backgroundActivities)
	env.RegisterActivity(syncSSWfCheck)

	// Mock test data
	pools := []*datamodel.Pool{
		{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-pool-1",
			Account: &datamodel.Account{
				Name: "test-account",
			},
			VendorID: "/projects/test-project/locations/us-central1/pools/test-pool-1",
		},
		{
			BaseModel: datamodel.BaseModel{ID: 2},
			Name:      "test-pool-2",
			Account: &datamodel.Account{
				Name: "test-account",
			},
			VendorID: "/projects/test-project/locations/us-west1/pools/test-pool-1",
		},
	}

	// Mock ListPools activity to return test pools
	env.OnActivity(backgroundActivities.ListPools, mock.Anything).Return(pools, nil).Once()

	// Mock IsSyncSnapshotForPoolRunning to return false (not running) for all pools
	env.OnActivity(syncSSWfCheck.IsSyncSnapshotForPoolRunning, mock.Anything, mock.Anything).Return(false, nil).Times(len(pools))

	// Mock child workflow executions
	for _, pool := range pools {
		env.OnWorkflow(SyncSnapshotsForPoolWorkflow, mock.Anything, pool).Return(nil)
	}

	// Execute workflow
	env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncVSASnapshotsWorkflow_ListPoolsError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}

	// Register activities
	env.RegisterActivity(&backgroundactivities.SyncSnapshotActivity{})

	// Mock ListPools activity to return error
	env.OnActivity(backgroundActivities.ListPools, mock.Anything).Return(nil, assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestSyncVSASnapshotsWorkflow_AlreadyRunningWorkflows(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}
	syncSSWfCheck := &SyncSnapshotWFRunningCheck{}

	// Register activities
	env.RegisterActivity(backgroundActivities)
	env.RegisterActivity(syncSSWfCheck)

	// Mock test data
	pools := []*datamodel.Pool{
		{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-pool-1",
			Account: &datamodel.Account{
				Name: "test-account",
			},
			VendorID: "/projects/test-project/locations/us-central1/pools/test-pool-1",
		},
		{
			BaseModel: datamodel.BaseModel{ID: 2},
			Name:      "test-pool-2",
			Account: &datamodel.Account{
				Name: "test-account",
			},
			VendorID: "/projects/test-project/locations/us-west1/pools/test-pool-1",
		},
	}

	// Mock ListPools activity
	env.OnActivity(backgroundActivities.ListPools, mock.Anything).Return(pools, nil).Once()

	// Mock IsSyncSnapshotForPoolRunning to return true (already running) for all pools
	env.OnActivity(syncSSWfCheck.IsSyncSnapshotForPoolRunning, mock.Anything, mock.Anything).Return(true, nil)

	// Execute workflow
	env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncVSASnapshotsWorkflow_InvalidVendorID(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}
	syncSSWfCheck := &SyncSnapshotWFRunningCheck{}

	// Register activities
	env.RegisterActivity(backgroundActivities)
	env.RegisterActivity(syncSSWfCheck)

	// Mock test data with invalid vendor ID
	pools := []*datamodel.Pool{
		{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-pool-1",
			Account: &datamodel.Account{
				Name: "test-account",
			},
			VendorID: "invalid-location", // Invalid vendor ID
		},
	}

	// Mock ListPools activity
	env.OnActivity(backgroundActivities.ListPools, mock.Anything).Return(pools, nil).Once()

	// Execute workflow
	env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
}

func TestSyncVSASnapshotsWorkflow_CheckRunningError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}
	syncSSWfCheck := &SyncSnapshotWFRunningCheck{}

	// Register activities
	env.RegisterActivity(backgroundActivities)
	env.RegisterActivity(syncSSWfCheck)

	// Mock test data
	pools := []*datamodel.Pool{
		{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-pool-1",
			Account: &datamodel.Account{
				Name: "test-account",
			},
			VendorID: "/projects/test-project/locations/us-central1/pools/test-pool-1",
		},
	}

	// Mock ListPools activity
	env.OnActivity(backgroundActivities.ListPools, mock.Anything).Return(pools, nil).Once()

	// Mock IsSyncSnapshotForPoolRunning to return error
	env.OnActivity(syncSSWfCheck.IsSyncSnapshotForPoolRunning, mock.Anything, mock.Anything).Return(nil, assert.AnError)

	// Mock child workflow execution (should still execute despite check error)
	env.OnWorkflow(SyncSnapshotsForPoolWorkflow, mock.Anything, pools[0]).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncSnapshotsForPoolWorkflow(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	backgroundActivity := &backgroundactivities.SyncSnapshotActivity{}
	// Register activity
	env.RegisterActivity(backgroundActivity)

	// Test data
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock activity responses
	ontapVolSnapshotResp := &backgroundactivities.GetOntapVolumesAndSnapshotsForPoolReturnValue{
		OntapVolumeMap: make(map[string]*vsa.Volume),
		OntapSnapshots: make([]*vsa.Snapshot, 0),
	}

	dbVolSnapshotResp := &backgroundactivities.GetDBVolumeAndSnapshotsForPoolReturnValue{
		DBVolumeMap: make(map[string]*datamodel.Volume),
		DBSnapshots: make([]*datamodel.Snapshot, 0),
	}

	processSnapshotsResp := &backgroundactivities.ProcessSnapshotsReturnValue{
		DeleteIDs:  []int64{},
		NewIDs:     []string{},
		UpdatedIDs: []string{},
	}

	// Mock activities
	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(ontapVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.GetDBVolumeAndSnapshotsForPool, mock.Anything, pool).Return(dbVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.ProcessSnapshots, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(processSnapshotsResp, nil)
	env.OnActivity(backgroundActivity.SyncDeletedSnapshotsToDatabase, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.SyncNewSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.SyncUpdatedSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.SyncWronglyDeletedSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.HydrateSnapshotsToCCFE, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, pool)

	// Verify workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify workflow status
	var status workflows.WorkflowStatus
	encVal, err := env.QueryWorkflow(workflows.StatusQueryName)
	if err != nil {
		t.Fatalf("Failed to query workflow status: %v", err)
	}
	err = encVal.Get(&status)
	if err != nil {
		t.Fatalf("Failed to decode workflow status: %v", err)
	}
	assert.NoError(t, err)
	assert.Equal(t, workflows.WorkflowStatusCompleted, status.Status)
	assert.Equal(t, "test-account", status.CustomerID)
}

func TestSyncSnapshotsForPoolWorkflow_GetOntapVolumesError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	backgroundActivity := &backgroundactivities.SyncSnapshotActivity{}
	// Register activity
	env.RegisterActivity(backgroundActivity)

	// Test data
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock GetOntapVolumesAndSnapshotsForPool to fail
	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(nil, fmt.Errorf("failed to get ONTAP volumes"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, pool)

	assert.True(t, env.IsWorkflowCompleted())

	// Verify workflow status shows failed
	var status workflows.WorkflowStatus
	encVal, err := env.QueryWorkflow(workflows.StatusQueryName)
	if err != nil {
		t.Fatalf("Failed to query workflow status: %v", err)
	}
	err = encVal.Get(&status)
	if err != nil {
		t.Fatalf("Failed to decode workflow status: %v", err)
	}
	assert.NoError(t, err)
	assert.Equal(t, workflows.WorkflowStatusFailed, status.Status)
	assert.Equal(t, "test-account", status.CustomerID)
}

func TestSyncSnapshotsForPoolWorkflow_GetDBSnapshotsError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	backgroundActivity := &backgroundactivities.SyncSnapshotActivity{}
	// Register activity
	env.RegisterActivity(backgroundActivity)

	// Test data
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock successful ONTAP volumes retrieval
	ontapVolSnapshotResp := &backgroundactivities.GetOntapVolumesAndSnapshotsForPoolReturnValue{
		OntapVolumeMap: make(map[string]*vsa.Volume),
		OntapSnapshots: make([]*vsa.Snapshot, 0),
	}
	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(ontapVolSnapshotResp, nil)

	// Mock GetDBVolumeAndSnapshotsForPool to fail
	env.OnActivity(backgroundActivity.GetDBVolumeAndSnapshotsForPool, mock.Anything, pool).Return(nil, fmt.Errorf("failed to get DB snapshots"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, pool)

	assert.True(t, env.IsWorkflowCompleted())

	// Verify workflow status shows failed
	var status workflows.WorkflowStatus
	encVal, err := env.QueryWorkflow(workflows.StatusQueryName)
	if err != nil {
		t.Fatalf("Failed to query workflow status: %v", err)
	}
	err = encVal.Get(&status)
	if err != nil {
		t.Fatalf("Failed to decode workflow status: %v", err)
	}
	assert.NoError(t, err)
	assert.Equal(t, workflows.WorkflowStatusFailed, status.Status)
	assert.Equal(t, "test-account", status.CustomerID)
}

func TestSyncSnapshotsForPoolWorkflow_ProcessSnapshotsError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	backgroundActivity := &backgroundactivities.SyncSnapshotActivity{}
	// Register activity
	env.RegisterActivity(backgroundActivity)

	// Test data
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock successful responses for initial activities
	ontapVolSnapshotResp := &backgroundactivities.GetOntapVolumesAndSnapshotsForPoolReturnValue{
		OntapVolumeMap: make(map[string]*vsa.Volume),
		OntapSnapshots: make([]*vsa.Snapshot, 0),
	}
	dbVolSnapshotResp := &backgroundactivities.GetDBVolumeAndSnapshotsForPoolReturnValue{
		DBVolumeMap: make(map[string]*datamodel.Volume),
		DBSnapshots: make([]*datamodel.Snapshot, 0),
	}

	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(ontapVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.GetDBVolumeAndSnapshotsForPool, mock.Anything, pool).Return(dbVolSnapshotResp, nil)

	// Mock ProcessSnapshots to fail
	env.OnActivity(backgroundActivity.ProcessSnapshots, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("failed to process snapshots"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, pool)

	assert.True(t, env.IsWorkflowCompleted())

	// Verify workflow status shows failed
	var status workflows.WorkflowStatus
	encVal, err := env.QueryWorkflow(workflows.StatusQueryName)
	if err != nil {
		t.Fatalf("Failed to query workflow status: %v", err)
	}
	err = encVal.Get(&status)
	if err != nil {
		t.Fatalf("Failed to decode workflow status: %v", err)
	}
	assert.NoError(t, err)
	assert.Equal(t, workflows.WorkflowStatusFailed, status.Status)
	assert.Equal(t, "test-account", status.CustomerID)
}

func TestSyncSnapshotsForPoolWorkflow_SyncDeletedSnapshotsError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	backgroundActivity := &backgroundactivities.SyncSnapshotActivity{}
	// Register activity
	env.RegisterActivity(backgroundActivity)

	// Test data
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock successful responses for initial activities
	ontapVolSnapshotResp := &backgroundactivities.GetOntapVolumesAndSnapshotsForPoolReturnValue{
		OntapVolumeMap: make(map[string]*vsa.Volume),
		OntapSnapshots: make([]*vsa.Snapshot, 0),
	}
	dbVolSnapshotResp := &backgroundactivities.GetDBVolumeAndSnapshotsForPoolReturnValue{
		DBVolumeMap: make(map[string]*datamodel.Volume),
		DBSnapshots: make([]*datamodel.Snapshot, 0),
	}
	processSnapshotsResp := &backgroundactivities.ProcessSnapshotsReturnValue{
		DeleteIDs:  []int64{1},
		NewIDs:     []string{},
		UpdatedIDs: []string{},
	}

	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(ontapVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.GetDBVolumeAndSnapshotsForPool, mock.Anything, pool).Return(dbVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.ProcessSnapshots, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(processSnapshotsResp, nil)

	// Mock SyncDeletedSnapshotsToDatabase to fail
	env.OnActivity(backgroundActivity.SyncDeletedSnapshotsToDatabase, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("failed to sync deleted snapshots"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, pool)

	assert.True(t, env.IsWorkflowCompleted())

	// Verify workflow status shows failed
	var status workflows.WorkflowStatus
	encVal, err := env.QueryWorkflow(workflows.StatusQueryName)
	if err != nil {
		t.Fatalf("Failed to query workflow status: %v", err)
	}
	err = encVal.Get(&status)
	if err != nil {
		t.Fatalf("Failed to decode workflow status: %v", err)
	}
	assert.NoError(t, err)
	assert.Equal(t, workflows.WorkflowStatusFailed, status.Status)
	assert.Equal(t, "test-account", status.CustomerID)
}

func TestSyncSnapshotsForPoolWorkflow_SyncNewSnapshotsError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	backgroundActivity := &backgroundactivities.SyncSnapshotActivity{}
	// Register activity
	env.RegisterActivity(backgroundActivity)

	// Test data
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock successful responses for initial activities
	ontapVolSnapshotResp := &backgroundactivities.GetOntapVolumesAndSnapshotsForPoolReturnValue{
		OntapVolumeMap: make(map[string]*vsa.Volume),
		OntapSnapshots: make([]*vsa.Snapshot, 0),
	}
	dbVolSnapshotResp := &backgroundactivities.GetDBVolumeAndSnapshotsForPoolReturnValue{
		DBVolumeMap: make(map[string]*datamodel.Volume),
		DBSnapshots: make([]*datamodel.Snapshot, 0),
	}
	processSnapshotsResp := &backgroundactivities.ProcessSnapshotsReturnValue{
		DeleteIDs:  []int64{},
		NewIDs:     []string{"new-snap-1"},
		UpdatedIDs: []string{},
		NewSSMap:   make(map[string]*vsa.Snapshot),
	}

	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(ontapVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.GetDBVolumeAndSnapshotsForPool, mock.Anything, pool).Return(dbVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.ProcessSnapshots, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(processSnapshotsResp, nil)
	env.OnActivity(backgroundActivity.SyncDeletedSnapshotsToDatabase, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)

	// Mock SyncNewSnapshotsToDatabase to fail
	env.OnActivity(backgroundActivity.SyncNewSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("failed to sync new snapshots"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, pool)

	assert.True(t, env.IsWorkflowCompleted())

	// Verify workflow status shows failed
	var status workflows.WorkflowStatus
	encVal, err := env.QueryWorkflow(workflows.StatusQueryName)
	if err != nil {
		t.Fatalf("Failed to query workflow status: %v", err)
	}
	err = encVal.Get(&status)
	if err != nil {
		t.Fatalf("Failed to decode workflow status: %v", err)
	}
	assert.NoError(t, err)
	assert.Equal(t, workflows.WorkflowStatusFailed, status.Status)
	assert.Equal(t, "test-account", status.CustomerID)
}

func TestSyncSnapshotsForPoolWorkflow_SyncUpdatedSnapshotsError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	backgroundActivity := &backgroundactivities.SyncSnapshotActivity{}
	// Register activity
	env.RegisterActivity(backgroundActivity)

	// Test data
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock successful responses for initial activities
	ontapVolSnapshotResp := &backgroundactivities.GetOntapVolumesAndSnapshotsForPoolReturnValue{
		OntapVolumeMap: make(map[string]*vsa.Volume),
		OntapSnapshots: make([]*vsa.Snapshot, 0),
	}
	dbVolSnapshotResp := &backgroundactivities.GetDBVolumeAndSnapshotsForPoolReturnValue{
		DBVolumeMap: make(map[string]*datamodel.Volume),
		DBSnapshots: make([]*datamodel.Snapshot, 0),
	}
	processSnapshotsResp := &backgroundactivities.ProcessSnapshotsReturnValue{
		DeleteIDs:    []int64{},
		NewIDs:       []string{},
		UpdatedIDs:   []string{"updated-snap-1"},
		UpdatedSSMap: make(map[string]*vsa.Snapshot),
	}

	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(ontapVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.GetDBVolumeAndSnapshotsForPool, mock.Anything, pool).Return(dbVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.ProcessSnapshots, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(processSnapshotsResp, nil)
	env.OnActivity(backgroundActivity.SyncDeletedSnapshotsToDatabase, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.SyncNewSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)

	// Mock SyncUpdatedSnapshotsToDatabase to fail
	env.OnActivity(backgroundActivity.SyncUpdatedSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("failed to sync updated snapshots"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, pool)

	assert.True(t, env.IsWorkflowCompleted())

	// Verify workflow status shows failed
	var status workflows.WorkflowStatus
	encVal, err := env.QueryWorkflow(workflows.StatusQueryName)
	if err != nil {
		t.Fatalf("Failed to query workflow status: %v", err)
	}
	err = encVal.Get(&status)
	if err != nil {
		t.Fatalf("Failed to decode workflow status: %v", err)
	}
	assert.NoError(t, err)
	assert.Equal(t, workflows.WorkflowStatusFailed, status.Status)
	assert.Equal(t, "test-account", status.CustomerID)
}

func TestSyncSnapshotsForPoolWorkflow_SyncWronglyDeletedSnapshotsError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	backgroundActivity := &backgroundactivities.SyncSnapshotActivity{}
	// Register activity
	env.RegisterActivity(backgroundActivity)

	// Test data
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock successful responses for initial activities
	ontapVolSnapshotResp := &backgroundactivities.GetOntapVolumesAndSnapshotsForPoolReturnValue{
		OntapVolumeMap: make(map[string]*vsa.Volume),
		OntapSnapshots: make([]*vsa.Snapshot, 0),
	}
	dbVolSnapshotResp := &backgroundactivities.GetDBVolumeAndSnapshotsForPoolReturnValue{
		DBVolumeMap: make(map[string]*datamodel.Volume),
		DBSnapshots: make([]*datamodel.Snapshot, 0),
	}
	processSnapshotsResp := &backgroundactivities.ProcessSnapshotsReturnValue{
		DeleteIDs:                  []int64{},
		NewIDs:                     []string{},
		UpdatedIDs:                 []string{},
		WronglyDeletedIDs:          []string{"wrongly-deleted-1"},
		WronglyDeletedSnapshotsMap: make(map[string]*vsa.Snapshot),
	}

	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(ontapVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.GetDBVolumeAndSnapshotsForPool, mock.Anything, pool).Return(dbVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.ProcessSnapshots, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(processSnapshotsResp, nil)
	env.OnActivity(backgroundActivity.SyncDeletedSnapshotsToDatabase, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.SyncNewSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.SyncUpdatedSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock SyncWronglyDeletedSnapshotsToDatabase to fail
	env.OnActivity(backgroundActivity.SyncWronglyDeletedSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("failed to sync wrongly deleted snapshots"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, pool)

	assert.True(t, env.IsWorkflowCompleted())

	// Verify workflow status shows failed
	var status workflows.WorkflowStatus
	encVal, err := env.QueryWorkflow(workflows.StatusQueryName)
	if err != nil {
		t.Fatalf("Failed to query workflow status: %v", err)
	}
	err = encVal.Get(&status)
	if err != nil {
		t.Fatalf("Failed to decode workflow status: %v", err)
	}
	assert.NoError(t, err)
	assert.Equal(t, workflows.WorkflowStatusFailed, status.Status)
	assert.Equal(t, "test-account", status.CustomerID)
}

func TestSyncSnapshotsForPoolWorkflow_HydrateSnapshotsToCCFEError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	backgroundActivity := &backgroundactivities.SyncSnapshotActivity{}
	// Register activity
	env.RegisterActivity(backgroundActivity)

	// Test data
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock successful responses for initial activities
	ontapVolSnapshotResp := &backgroundactivities.GetOntapVolumesAndSnapshotsForPoolReturnValue{
		OntapVolumeMap: make(map[string]*vsa.Volume),
		OntapSnapshots: make([]*vsa.Snapshot, 0),
	}
	dbVolSnapshotResp := &backgroundactivities.GetDBVolumeAndSnapshotsForPoolReturnValue{
		DBVolumeMap: make(map[string]*datamodel.Volume),
		DBSnapshots: make([]*datamodel.Snapshot, 0),
	}
	processSnapshotsResp := &backgroundactivities.ProcessSnapshotsReturnValue{
		DeleteIDs:  []int64{},
		NewIDs:     []string{"new-snap-1"},
		UpdatedIDs: []string{},
	}

	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(ontapVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.GetDBVolumeAndSnapshotsForPool, mock.Anything, pool).Return(dbVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.ProcessSnapshots, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(processSnapshotsResp, nil)
	env.OnActivity(backgroundActivity.SyncDeletedSnapshotsToDatabase, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.SyncNewSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.SyncUpdatedSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(backgroundActivity.SyncWronglyDeletedSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock HydrateSnapshotsToCCFE to fail
	env.OnActivity(backgroundActivity.HydrateSnapshotsToCCFE, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("failed to hydrate snapshots to CCFE"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, pool)

	assert.True(t, env.IsWorkflowCompleted())

	// Verify workflow status shows failed
	var status workflows.WorkflowStatus
	encVal, err := env.QueryWorkflow(workflows.StatusQueryName)
	if err != nil {
		t.Fatalf("Failed to query workflow status: %v", err)
	}
	err = encVal.Get(&status)
	if err != nil {
		t.Fatalf("Failed to decode workflow status: %v", err)
	}
	assert.NoError(t, err)
	assert.Equal(t, workflows.WorkflowStatusFailed, status.Status)
	assert.Equal(t, "test-account", status.CustomerID)
}

func TestIsSyncSnapshotForPoolRunning(t *testing.T) {
	// Setup
	syncSSWfCheck := &SyncSnapshotWFRunningCheck{}
	ctx := context.Background()

	// Mock fetchTemporalClient and workflows.QueryWorkflowStatus
	originalFetchTemporalClient := fetchTemporalClient
	defer func() { fetchTemporalClient = originalFetchTemporalClient }()

	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockClient
	}

	// Case 1: QueryWorkflowStatus returns error
	workflows.QueryWorkflowStatus = func(ctx context.Context, c client.Client, workflowID, runID string) (*workflows.WorkflowStatus, error) {
		return nil, fmt.Errorf("query error")
	}
	running, err := syncSSWfCheck.IsSyncSnapshotForPoolRunning(ctx, "wf-id")
	assert.False(t, running)
	assert.Error(t, err)

	// Case 2: WorkflowStatusFailed
	workflows.QueryWorkflowStatus = func(ctx context.Context, c client.Client, workflowID, runID string) (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{Status: workflows.WorkflowStatusFailed}, nil
	}
	running, err = syncSSWfCheck.IsSyncSnapshotForPoolRunning(ctx, "wf-id")
	assert.False(t, running)
	assert.NoError(t, err)

	// Case 3: WorkflowStatusCompleted
	workflows.QueryWorkflowStatus = func(ctx context.Context, c client.Client, workflowID, runID string) (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{Status: workflows.WorkflowStatusCompleted}, nil
	}
	running, err = syncSSWfCheck.IsSyncSnapshotForPoolRunning(ctx, "wf-id")
	assert.False(t, running)
	assert.NoError(t, err)

	// Case 4: WorkflowStatusRunning
	workflows.QueryWorkflowStatus = func(ctx context.Context, c client.Client, workflowID, runID string) (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{Status: workflows.WorkflowStatusRunning}, nil
	}
	running, err = syncSSWfCheck.IsSyncSnapshotForPoolRunning(ctx, "wf-id")
	assert.True(t, running)
	assert.NoError(t, err)

	// Case 4: WorkflowStatusCreated
	workflows.QueryWorkflowStatus = func(ctx context.Context, c client.Client, workflowID, runID string) (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{Status: workflows.WorkflowStatusCreated}, nil
	}
	running, err = syncSSWfCheck.IsSyncSnapshotForPoolRunning(ctx, "wf-id")
	assert.True(t, running)
	assert.NoError(t, err)
}
