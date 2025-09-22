package backgroundworkflows

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
	startSSWF := &StartSyncSnapshotForPoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(backgroundActivities)
	env.RegisterActivity(startSSWF)

	// Mock test data
	pools := []*database.PoolIdentifier{
		{
			Name:      "test-pool-1",
			AccountID: 123,
			VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-1",
			UUID:      "test-pool-1",
		},
		{
			Name:      "test-pool-2",
			AccountID: 124,
			VendorID:  "/projects/test-project/locations/us-west1/pools/test-pool-2",
			UUID:      "test-pool-2",
		},
	}

	// Mock ListPools activity to return test pools
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything).Return(pools, nil).Once()

	// Mock StartSyncSnapshotForPoolWFActivity to return nil for all pools
	env.OnActivity(startSSWF.StartSyncSnapshotForPoolWFActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Times(len(pools))

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
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)

	// Mock ListPools activity to return error
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything).Return(nil, assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestSyncVSASnapshotsWorkflow_InvalidVendorID(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}
	startSSWF := &StartSyncSnapshotForPoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(backgroundActivities)
	env.RegisterActivity(startSSWF)

	// Mock test data with invalid vendor ID
	pools := []*database.PoolIdentifier{
		{
			Name:      "test-pool-1",
			AccountID: 123,
			VendorID:  "invalid-vendor-id", // Invalid format
			UUID:      "test-pool-1",
		},
	}

	// Mock ListPools activity
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything).Return(pools, nil).Once()

	// Execute workflow
	env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
}

func TestSyncVSASnapshotsWorkflow_CheckRunningError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}
	startSSWF := &StartSyncSnapshotForPoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(backgroundActivities)
	env.RegisterActivity(startSSWF)

	// Mock test data
	pools := []*database.PoolIdentifier{
		{
			Name:      "test-pool-1",
			AccountID: 123,
			VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-1",
			UUID:      "test-pool-1",
		},
	}

	// Mock ListPools activity
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything).Return(pools, nil).Once()

	// Mock StartSyncSnapshotForPoolWFActivity to return error
	env.OnActivity(startSSWF.StartSyncSnapshotForPoolWFActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)

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
		AccountID: 123,
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
	env.OnActivity(backgroundActivity.FetchPoolByUUID, mock.Anything, pool.UUID, mock.Anything).Return(pool, nil)
	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(ontapVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.GetDBVolumeAndSnapshotsForPool, mock.Anything, pool).Return(dbVolSnapshotResp, nil)
	env.OnActivity(backgroundActivity.ProcessSnapshots, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(processSnapshotsResp, nil)
	env.OnActivity(backgroundActivity.SyncDeletedSnapshotsToDatabase, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.SyncNewSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.SyncUpdatedSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.SyncWronglyDeletedSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, nil)
	env.OnActivity(backgroundActivity.HydrateSnapshotsToCCFE, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, &database.PoolIdentifier{
		Name:      pool.Name,
		AccountID: pool.AccountID,
		VendorID:  pool.VendorID,
		UUID:      pool.UUID,
	})

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
	assert.Equal(t, strconv.FormatInt(pool.AccountID, 10), status.CustomerID)
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
		AccountID: 123,
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock FetchPoolByUUID to return the pool
	env.OnActivity(backgroundActivity.FetchPoolByUUID, mock.Anything, pool.UUID, pool.AccountID).Return(pool, nil)

	// Mock GetOntapVolumesAndSnapshotsForPool to fail
	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(nil, fmt.Errorf("failed to get ONTAP volumes"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, &database.PoolIdentifier{
		Name:      pool.Name,
		AccountID: pool.AccountID,
		VendorID:  pool.VendorID,
		UUID:      pool.UUID,
	})

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
	assert.Equal(t, strconv.FormatInt(pool.AccountID, 10), status.CustomerID)
}

func TestSyncSnapshotsForPoolWorkflow_FetchPoolError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	backgroundActivity := &backgroundactivities.SyncSnapshotActivity{}
	// Register activity
	env.RegisterActivity(backgroundActivity)

	// Test data
	pool := &datamodel.Pool{
		AccountID: 123,
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock FetchPoolByUUID to return the pool
	env.OnActivity(backgroundActivity.FetchPoolByUUID, mock.Anything, pool.UUID, pool.AccountID).Return(nil, fmt.Errorf("failed to fetch pool"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, &database.PoolIdentifier{
		Name:      pool.Name,
		AccountID: pool.AccountID,
		VendorID:  pool.VendorID,
		UUID:      pool.UUID,
	})

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
	assert.Equal(t, strconv.FormatInt(pool.AccountID, 10), status.CustomerID)
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
		AccountID: 123,
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock FetchPoolByUUID to return the pool
	env.OnActivity(backgroundActivity.FetchPoolByUUID, mock.Anything, pool.UUID, pool.AccountID).Return(pool, nil)

	// Mock successful ONTAP volumes retrieval
	ontapVolSnapshotResp := &backgroundactivities.GetOntapVolumesAndSnapshotsForPoolReturnValue{
		OntapVolumeMap: make(map[string]*vsa.Volume),
		OntapSnapshots: make([]*vsa.Snapshot, 0),
	}
	env.OnActivity(backgroundActivity.GetOntapVolumesAndSnapshotsForPool, mock.Anything, pool).Return(ontapVolSnapshotResp, nil)

	// Mock GetDBVolumeAndSnapshotsForPool to fail
	env.OnActivity(backgroundActivity.GetDBVolumeAndSnapshotsForPool, mock.Anything, pool).Return(nil, fmt.Errorf("failed to get DB snapshots"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, &database.PoolIdentifier{
		Name:      pool.Name,
		AccountID: pool.AccountID,
		VendorID:  pool.VendorID,
		UUID:      pool.UUID,
	})

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
	assert.Equal(t, strconv.FormatInt(pool.AccountID, 10), status.CustomerID)
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
		AccountID: 123,
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock FetchPoolByUUID to return the pool
	env.OnActivity(backgroundActivity.FetchPoolByUUID, mock.Anything, pool.UUID, pool.AccountID).Return(pool, nil)

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
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, &database.PoolIdentifier{
		Name:      pool.Name,
		AccountID: pool.AccountID,
		VendorID:  pool.VendorID,
		UUID:      pool.UUID,
	})

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
	assert.Equal(t, strconv.FormatInt(pool.AccountID, 10), status.CustomerID)
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
		AccountID: 123,
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock FetchPoolByUUID to return the pool
	env.OnActivity(backgroundActivity.FetchPoolByUUID, mock.Anything, pool.UUID, pool.AccountID).Return(pool, nil)

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
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, &database.PoolIdentifier{
		Name:      pool.Name,
		AccountID: pool.AccountID,
		VendorID:  pool.VendorID,
		UUID:      pool.UUID,
	})

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
	assert.Equal(t, strconv.FormatInt(pool.AccountID, 10), status.CustomerID)
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
		AccountID: 123,
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock FetchPoolByUUID to return the pool
	env.OnActivity(backgroundActivity.FetchPoolByUUID, mock.Anything, pool.UUID, pool.AccountID).Return(pool, nil)

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
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, &database.PoolIdentifier{
		Name:      pool.Name,
		AccountID: pool.AccountID,
		VendorID:  pool.VendorID,
		UUID:      pool.UUID,
	})

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
	assert.Equal(t, strconv.FormatInt(pool.AccountID, 10), status.CustomerID)
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
		AccountID: 123,
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock FetchPoolByUUID to return the pool
	env.OnActivity(backgroundActivity.FetchPoolByUUID, mock.Anything, pool.UUID, pool.AccountID).Return(pool, nil)

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
	env.OnActivity(backgroundActivity.SyncUpdatedSnapshotsToDatabase, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Snapshot{}, fmt.Errorf("failed to sync updated snapshots"))

	// Execute workflow
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, &database.PoolIdentifier{
		Name:      pool.Name,
		AccountID: pool.AccountID,
		VendorID:  pool.VendorID,
		UUID:      pool.UUID,
	})

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
	assert.Equal(t, strconv.FormatInt(pool.AccountID, 10), status.CustomerID)
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
		AccountID: 123,
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock FetchPoolByUUID to return the pool
	env.OnActivity(backgroundActivity.FetchPoolByUUID, mock.Anything, pool.UUID, pool.AccountID).Return(pool, nil)

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
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, &database.PoolIdentifier{
		Name:      pool.Name,
		AccountID: pool.AccountID,
		VendorID:  pool.VendorID,
		UUID:      pool.UUID,
	})

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
	assert.Equal(t, strconv.FormatInt(pool.AccountID, 10), status.CustomerID)
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
		AccountID: 123,
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock FetchPoolByUUID to return the pool
	env.OnActivity(backgroundActivity.FetchPoolByUUID, mock.Anything, pool.UUID, pool.AccountID).Return(pool, nil)

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
	env.ExecuteWorkflow(SyncSnapshotsForPoolWorkflow, &database.PoolIdentifier{
		Name:      pool.Name,
		AccountID: pool.AccountID,
		VendorID:  pool.VendorID,
		UUID:      pool.UUID,
	})

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
	assert.Equal(t, strconv.FormatInt(pool.AccountID, 10), status.CustomerID)
}

func TestStartSyncSnapshotForPoolWFActivity_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}
	startSyncActivity := &StartSyncSnapshotForPoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(backgroundActivities)
	env.RegisterActivity(startSyncActivity)

	// Test data
	pool := &database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool",
	}

	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything).Return([]*database.PoolIdentifier{
		pool,
	}, nil).Once()

	// Mock fetchTemporalClient and workflows.QueryWorkflowStatus
	originalFetchTemporalClient := fetchTemporalClient
	defer func() { fetchTemporalClient = originalFetchTemporalClient }()

	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockClient
	}
	// Mock workflow execution
	mockClient.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute workflow
	env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify that CheckRunningWorkflow was called
	env.AssertExpectations(t)
}

func TestStartSyncSnapshotForPoolWFActivity_Error(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}
	startSyncActivity := &StartSyncSnapshotForPoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(backgroundActivities)
	env.RegisterActivity(startSyncActivity)

	// Test data
	pool := &database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool",
	}

	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything).Return([]*database.PoolIdentifier{
		pool,
	}, nil).Once()

	// Mock fetchTemporalClient and workflows.QueryWorkflowStatus
	originalFetchTemporalClient := fetchTemporalClient
	defer func() { fetchTemporalClient = originalFetchTemporalClient }()

	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockClient
	}
	// Mock workflow execution
	mockClient.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("failed to execute workflow"))

	// Execute workflow
	env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())

	// Verify that CheckRunningWorkflow was called
	env.AssertExpectations(t)
}
