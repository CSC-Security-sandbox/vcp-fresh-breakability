package backgroundworkflows

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestSyncVSAAutoTieringWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)

	// Mock test data
	pools := []*database.PoolIdentifier{
		{
			Name:      "test-pool-1",
			AccountID: 123,
			VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-1",
			UUID:      "test-pool-1-uuid",
		},
		{
			Name:      "test-pool-2",
			AccountID: 124,
			VendorID:  "/projects/test-project/locations/us-west1/pools/test-pool-2",
			UUID:      "test-pool-2-uuid",
		},
	}

	poolsConsumptionMap := map[string]map[string]float64{
		"test-pool-1-uuid": {
			backgroundactivities.PoolConsumptionHotTier:  500000000000,
			backgroundactivities.PoolConsumptionColdTier: 600000000000,
		},
		"test-pool-2-uuid": {
			backgroundactivities.PoolConsumptionHotTier:  200000000000,
			backgroundactivities.PoolConsumptionColdTier: 100000000000,
		},
	}

	segregatedPools := map[string][]*database.PoolIdentifier{
		backgroundactivities.PoolsToPauseKey: {
			{
				Name:      "test-pool-to-pause",
				AccountID: 123,
				VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-to-pause",
				UUID:      "test-pool-to-pause-uuid",
			},
		},
		backgroundactivities.PoolsToResumeKey: {
			{
				Name:      "test-pool-to-resume",
				AccountID: 124,
				VendorID:  "/projects/test-project/locations/us-west1/pools/test-pool-to-resume",
				UUID:      "test-pool-to-resume-uuid",
			},
		},
		backgroundactivities.PoolsToAutoResizeKey: {
			{
				Name:      "test-pool-to-autoresize",
				AccountID: 125,
				VendorID:  "/projects/test-project/locations/us-east1/pools/test-pool-to-autoresize",
				UUID:      "test-pool-to-autoresize-uuid",
			},
		},
	}

	// Mock activities
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything, mock.Anything).Return(pools, nil).Once()
	env.OnActivity(autoTierActivity.FetchAndSavePoolsTieringInfo, mock.Anything, pools).Return(poolsConsumptionMap, nil).Once()
	env.OnActivity(autoTierActivity.UpdatePoolTieringConsumptionInDB, mock.Anything, poolsConsumptionMap).Return(nil).Once()
	env.OnActivity(autoTierActivity.SegregatePools, mock.Anything, pools, poolsConsumptionMap).Return(segregatedPools, nil).Once()

	// Mock child workflows for pause/resume
	for _, pool := range segregatedPools[backgroundactivities.PoolsToPauseKey] {
		env.OnWorkflow(AutoTieringPauseResumeWorkflow, mock.Anything, *pool, backgroundactivities.PoolsToPauseKey).Return(nil)
	}
	for _, pool := range segregatedPools[backgroundactivities.PoolsToResumeKey] {
		env.OnWorkflow(AutoTieringPauseResumeWorkflow, mock.Anything, *pool, backgroundactivities.PoolsToResumeKey).Return(nil)
	}

	// Mock GetWorkflowLastExecutionTime for auto-resize pools
	lastExecTime := time.Now().Add(-5 * time.Hour) // More than 4 hours ago
	wfLastExecActivity := &activities.WFLastExecutionActivity{}
	env.RegisterActivity(wfLastExecActivity)
	env.OnActivity(wfLastExecActivity.GetWorkflowLastExecutionTime, mock.Anything, mock.Anything).Return(&lastExecTime, nil)

	// Mock child workflow for auto-resize
	for _, pool := range segregatedPools[backgroundactivities.PoolsToAutoResizeKey] {
		env.OnWorkflow(AutoTieringHotTierAutoResizeWorkflow, mock.Anything, pool).Return(nil)
	}

	// Execute workflow
	env.ExecuteWorkflow(SyncVSAAutoTieringWorkflow)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncVSAAutoTieringWorkflow_ListPoolsError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)

	// Mock ListPools activity to return error
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything, mock.Anything).Return(nil, assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(SyncVSAAutoTieringWorkflow)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestSyncVSAAutoTieringWorkflow_GetPoolsTierConsumptionError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)

	pools := []*database.PoolIdentifier{
		{
			Name:      "test-pool-1",
			AccountID: 123,
			VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-1",
			UUID:      "test-pool-1-uuid",
		},
	}

	// Mock activities
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything, mock.Anything).Return(pools, nil).Once()
	env.OnActivity(autoTierActivity.FetchAndSavePoolsTieringInfo, mock.Anything, pools).Return(nil, assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(SyncVSAAutoTieringWorkflow)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestSyncVSAAutoTieringWorkflow_UpdatePoolTieringConsumptionError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)

	pools := []*database.PoolIdentifier{
		{
			Name:      "test-pool-1",
			AccountID: 123,
			VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-1",
			UUID:      "test-pool-1-uuid",
		},
	}

	poolsConsumptionMap := map[string]map[string]float64{
		"test-pool-1-uuid": {
			backgroundactivities.PoolConsumptionHotTier:  500000000000,
			backgroundactivities.PoolConsumptionColdTier: 600000000000,
		},
	}

	segregatedPools := map[string][]*database.PoolIdentifier{
		backgroundactivities.PoolsToPauseKey: {
			{
				Name:      "test-pool-1",
				AccountID: 123,
				VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-1",
				UUID:      "test-pool-1-uuid",
			},
		},
	}

	// Mock activities
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything, mock.Anything).Return(pools, nil).Once()
	env.OnActivity(autoTierActivity.FetchAndSavePoolsTieringInfo, mock.Anything, pools).Return(poolsConsumptionMap, nil).Once()
	env.OnActivity(autoTierActivity.UpdatePoolTieringConsumptionInDB, mock.Anything, poolsConsumptionMap).Return(assert.AnError)
	env.OnActivity(autoTierActivity.SegregatePools, mock.Anything, pools, poolsConsumptionMap).Return(segregatedPools, nil).Once()

	// Mock child workflows for pause/resume
	for _, pool := range segregatedPools[backgroundactivities.PoolsToPauseKey] {
		env.OnWorkflow(AutoTieringPauseResumeWorkflow, mock.Anything, *pool, backgroundactivities.PoolsToPauseKey).Return(nil)
	}
	for _, pool := range segregatedPools[backgroundactivities.PoolsToResumeKey] {
		env.OnWorkflow(AutoTieringPauseResumeWorkflow, mock.Anything, *pool, backgroundactivities.PoolsToResumeKey).Return(nil)
	}

	// Mock GetWorkflowLastExecutionTime for auto-resize pools
	lastExecTime := time.Now().Add(-5 * time.Hour) // More than 4 hours ago
	wfLastExecActivity := &activities.WFLastExecutionActivity{}
	env.RegisterActivity(wfLastExecActivity)
	env.OnActivity(wfLastExecActivity.GetWorkflowLastExecutionTime, mock.Anything, mock.Anything).Return(&lastExecTime, nil)

	// Mock child workflow for auto-resize
	for _, pool := range segregatedPools[backgroundactivities.PoolsToAutoResizeKey] {
		env.OnWorkflow(AutoTieringHotTierAutoResizeWorkflow, mock.Anything, pool).Return(nil)
	}

	// Execute workflow
	env.ExecuteWorkflow(SyncVSAAutoTieringWorkflow)

	// Assert workflow completion with no error
	// Even if tiering consumption update fails, workflow should continue
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncVSAAutoTieringWorkflow_SegregatePoolsError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)

	pools := []*database.PoolIdentifier{
		{
			Name:      "test-pool-1",
			AccountID: 123,
			VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-1",
			UUID:      "test-pool-1-uuid",
		},
	}

	poolsConsumptionMap := map[string]map[string]float64{
		"test-pool-1-uuid": {
			backgroundactivities.PoolConsumptionHotTier:  500000000000,
			backgroundactivities.PoolConsumptionColdTier: 600000000000,
		},
	}

	// Mock activities
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything, mock.Anything).Return(pools, nil).Once()
	env.OnActivity(autoTierActivity.FetchAndSavePoolsTieringInfo, mock.Anything, pools).Return(poolsConsumptionMap, nil).Once()
	env.OnActivity(autoTierActivity.UpdatePoolTieringConsumptionInDB, mock.Anything, poolsConsumptionMap).Return(nil).Once()
	env.OnActivity(autoTierActivity.SegregatePools, mock.Anything, pools, poolsConsumptionMap).Return(nil, assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(SyncVSAAutoTieringWorkflow)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestSyncVSAAutoTieringWorkflow_ChildWorkflowFailure(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)

	pools := []*database.PoolIdentifier{
		{
			Name:      "test-pool-1",
			AccountID: 123,
			VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-1",
			UUID:      "test-pool-1-uuid",
		},
	}

	poolsConsumptionMap := map[string]map[string]float64{
		"test-pool-1-uuid": {
			backgroundactivities.PoolConsumptionHotTier:  500000000000,
			backgroundactivities.PoolConsumptionColdTier: 600000000000,
		},
	}

	segregatedPools := map[string][]*database.PoolIdentifier{
		backgroundactivities.PoolsToPauseKey: {
			{
				Name:      "test-pool-to-pause",
				AccountID: 123,
				VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-to-pause",
				UUID:      "test-pool-to-pause-uuid",
			},
		},
		backgroundactivities.PoolsToResumeKey:     {},
		backgroundactivities.PoolsToAutoResizeKey: {},
	}

	// Mock activities
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything, mock.Anything).Return(pools, nil).Once()
	env.OnActivity(autoTierActivity.FetchAndSavePoolsTieringInfo, mock.Anything, pools).Return(poolsConsumptionMap, nil).Once()
	env.OnActivity(autoTierActivity.UpdatePoolTieringConsumptionInDB, mock.Anything, poolsConsumptionMap).Return(nil).Once()
	env.OnActivity(autoTierActivity.SegregatePools, mock.Anything, pools, poolsConsumptionMap).Return(segregatedPools, nil).Once()

	// Mock child workflow to fail
	for _, pool := range segregatedPools[backgroundactivities.PoolsToPauseKey] {
		env.OnWorkflow(AutoTieringPauseResumeWorkflow, mock.Anything, *pool, backgroundactivities.PoolsToPauseKey).Return(assert.AnError)
	}

	// Execute workflow
	env.ExecuteWorkflow(SyncVSAAutoTieringWorkflow)

	// Assert workflow completion (should continue despite child workflow failure)
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncVSAAutoTieringWorkflow_AutoResizeSkippedDueToRecentExecution(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)

	pools := []*database.PoolIdentifier{
		{
			Name:      "test-pool-1",
			AccountID: 123,
			VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-1",
			UUID:      "test-pool-1-uuid",
		},
	}

	poolsConsumptionMap := map[string]map[string]float64{
		"test-pool-1-uuid": {
			backgroundactivities.PoolConsumptionHotTier:  500000000000,
			backgroundactivities.PoolConsumptionColdTier: 600000000000,
		},
	}

	segregatedPools := map[string][]*database.PoolIdentifier{
		backgroundactivities.PoolsToPauseKey:  {},
		backgroundactivities.PoolsToResumeKey: {},
		backgroundactivities.PoolsToAutoResizeKey: {
			{
				Name:      "test-pool-to-autoresize",
				AccountID: 125,
				VendorID:  "/projects/test-project/locations/us-east1/pools/test-pool-to-autoresize",
				UUID:      "test-pool-to-autoresize-uuid",
			},
		},
	}

	// Mock activities
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything, mock.Anything).Return(pools, nil).Once()
	env.OnActivity(autoTierActivity.FetchAndSavePoolsTieringInfo, mock.Anything, pools).Return(poolsConsumptionMap, nil).Once()
	env.OnActivity(autoTierActivity.UpdatePoolTieringConsumptionInDB, mock.Anything, poolsConsumptionMap).Return(nil).Once()
	env.OnActivity(autoTierActivity.SegregatePools, mock.Anything, pools, poolsConsumptionMap).Return(segregatedPools, nil).Once()

	// Mock GetWorkflowLastExecutionTime for auto-resize pools (recent execution - within 4 hours)
	lastExecTime := time.Now().Add(-2 * time.Hour) // Less than 4 hours ago
	wfLastExecActivity := &activities.WFLastExecutionActivity{}
	env.RegisterActivity(wfLastExecActivity)
	env.OnActivity(wfLastExecActivity.GetWorkflowLastExecutionTime, mock.Anything, mock.Anything).Return(&lastExecTime, nil)

	// Execute workflow
	env.ExecuteWorkflow(SyncVSAAutoTieringWorkflow)

	// Assert workflow completion (auto-resize should be skipped)
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncVSAAutoTieringWorkflow_GetLastExecutionTimeError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)

	pools := []*database.PoolIdentifier{
		{
			Name:      "test-pool-1",
			AccountID: 123,
			VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-1",
			UUID:      "test-pool-1-uuid",
		},
	}

	poolsConsumptionMap := map[string]map[string]float64{
		"test-pool-1-uuid": {
			backgroundactivities.PoolConsumptionHotTier:  500000000000,
			backgroundactivities.PoolConsumptionColdTier: 600000000000,
		},
	}

	segregatedPools := map[string][]*database.PoolIdentifier{
		backgroundactivities.PoolsToPauseKey:  {},
		backgroundactivities.PoolsToResumeKey: {},
		backgroundactivities.PoolsToAutoResizeKey: {
			{
				Name:      "test-pool-to-autoresize",
				AccountID: 125,
				VendorID:  "/projects/test-project/locations/us-east1/pools/test-pool-to-autoresize",
				UUID:      "test-pool-to-autoresize-uuid",
			},
		},
	}

	// Mock activities
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything, mock.Anything).Return(pools, nil).Once()
	env.OnActivity(autoTierActivity.FetchAndSavePoolsTieringInfo, mock.Anything, pools).Return(poolsConsumptionMap, nil).Once()
	env.OnActivity(autoTierActivity.UpdatePoolTieringConsumptionInDB, mock.Anything, poolsConsumptionMap).Return(nil).Once()
	env.OnActivity(autoTierActivity.SegregatePools, mock.Anything, pools, poolsConsumptionMap).Return(segregatedPools, nil).Once()

	// Mock GetWorkflowLastExecutionTime to fail
	wfLastExecActivity := &activities.WFLastExecutionActivity{}
	env.RegisterActivity(wfLastExecActivity)
	env.OnActivity(wfLastExecActivity.GetWorkflowLastExecutionTime, mock.Anything, mock.Anything).Return(nil, assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(SyncVSAAutoTieringWorkflow)

	// Assert workflow completion (should continue despite error)
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestAutoTieringPauseResumeWorkflow_PauseSuccess(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)
	env.RegisterActivity(poolActivity)
	env.RegisterActivity(commonActivities)

	poolIdentifier := database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 123,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			TieringStatus: datamodel.TieringStatusResumed,
		},
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
		},
		DeploymentName: "test-deployment",
	}

	nodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: 1},
			EndpointAddress: "test-endpoint",
		},
	}

	// Mock activities
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(pool, nil)
	env.OnActivity(commonActivities.GetNode, mock.Anything, mock.Anything).Return(nodes, nil)
	env.OnActivity(autoTierActivity.ToggleHotTierBypassModeForPoolVolumes, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(autoTierActivity.UpdateAggregateInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(autoTierActivity.UpdatePoolTieringThresholdAndStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(AutoTieringPauseResumeWorkflow, poolIdentifier, backgroundactivities.PoolsToPauseKey)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestAutoTieringPauseResumeWorkflow_ResumeSuccess(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)
	env.RegisterActivity(poolActivity)
	env.RegisterActivity(commonActivities)

	poolIdentifier := database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 123,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			TieringStatus: datamodel.TieringStatusPaused,
		},
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
		},
		DeploymentName: "test-deployment",
	}

	nodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: 1},
			EndpointAddress: "test-endpoint",
		},
	}

	// Mock activities
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(pool, nil)
	env.OnActivity(commonActivities.GetNode, mock.Anything, mock.Anything).Return(nodes, nil)
	env.OnActivity(autoTierActivity.ToggleHotTierBypassModeForPoolVolumes, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(autoTierActivity.UpdateAggregateInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(autoTierActivity.UpdatePoolTieringThresholdAndStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(AutoTieringPauseResumeWorkflow, poolIdentifier, backgroundactivities.PoolsToResumeKey)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestAutoTieringPauseResumeWorkflow_FetchPoolError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}

	// Register activities
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)

	poolIdentifier := database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	// Mock FetchPoolByUUID to fail
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(nil, assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(AutoTieringPauseResumeWorkflow, poolIdentifier, backgroundactivities.PoolsToPauseKey)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestAutoTieringPauseResumeWorkflow_UpdatingPoolError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}

	// Register activities
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)
	env.RegisterActivity(poolActivity)

	poolIdentifier := database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 123,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			TieringStatus: datamodel.TieringStatusResumed,
		},
	}

	// Mock activities
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(pool, nil)
	env.OnActivity(poolActivity.UpdatingPool, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	env.OnActivity(poolActivity.UpdatePoolFields, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute workflow
	env.ExecuteWorkflow(AutoTieringPauseResumeWorkflow, poolIdentifier, backgroundactivities.PoolsToPauseKey)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestAutoTieringPauseResumeWorkflow_GetNodeError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)
	env.RegisterActivity(poolActivity)
	env.RegisterActivity(commonActivities)

	poolIdentifier := database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 123,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			TieringStatus: datamodel.TieringStatusResumed,
		},
	}

	// Mock activities
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(pool, nil)
	env.OnActivity(poolActivity.UpdatingPool, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity(commonActivities.GetNode, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	env.OnActivity(poolActivity.UpdatePoolFields, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute workflow
	env.ExecuteWorkflow(AutoTieringPauseResumeWorkflow, poolIdentifier, backgroundactivities.PoolsToPauseKey)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestAutoTieringPauseResumeWorkflow_UpdateAggregateError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)
	env.RegisterActivity(poolActivity)
	env.RegisterActivity(commonActivities)

	poolIdentifier := database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 123,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			TieringStatus:            datamodel.TieringStatusResumed,
			TieringFullnessThreshold: 0,
		},
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
		},
		DeploymentName: "test-deployment",
	}

	nodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: 1},
			EndpointAddress: "test-endpoint",
		},
	}

	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(pool, nil)
	env.OnActivity(commonActivities.GetNode, mock.Anything, mock.Anything).Return(nodes, nil)
	env.OnActivity(autoTierActivity.ToggleHotTierBypassModeForPoolVolumes, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(autoTierActivity.UpdateAggregateInOntap, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)
	env.OnActivity(autoTierActivity.UpdatePoolTieringThresholdAndStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(AutoTieringPauseResumeWorkflow, poolIdentifier, backgroundactivities.PoolsToPauseKey)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestAutoTieringPauseResumeWorkflow_UpdatedPoolError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)
	env.RegisterActivity(poolActivity)
	env.RegisterActivity(commonActivities)

	poolIdentifier := database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 123,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			TieringStatus: datamodel.TieringStatusResumed,
		},
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
		},
		DeploymentName: "test-deployment",
	}

	nodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: 1},
			EndpointAddress: "test-endpoint",
		},
	}

	// Mock activities
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(pool, nil)
	env.OnActivity(poolActivity.UpdatingPool, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity(commonActivities.GetNode, mock.Anything, mock.Anything).Return(nodes, nil)
	env.OnActivity(autoTierActivity.ToggleHotTierBypassModeForPoolVolumes, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(autoTierActivity.UpdateAggregateInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(poolActivity.UpdatedPool, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	env.OnActivity(poolActivity.UpdatePoolFields, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute workflow
	env.ExecuteWorkflow(AutoTieringPauseResumeWorkflow, poolIdentifier, backgroundactivities.PoolsToPauseKey)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestAutoTieringHotTierAutoResizeWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)
	env.RegisterActivity(poolActivity)
	env.RegisterActivity(commonActivities)

	// Register workflows
	env.RegisterWorkflow(workflows.UpdatePoolWorkflow)

	poolIdentifier := &database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes: 500000000000, // 500GB
		},
		SizeInBytes: 1000000000000, // 1TB
		Description: "test pool",
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 1000,
			Iops:            100,
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{ID: 1, UUID: "job-uuid"},
		Type:       string(models.JobTypeUpdatePool),
		State:      string(models.JobsStatePROCESSING),
		IsAdminJob: true,
		WorkflowID: "test-workflow-id",
		AccountID:  sql.NullInt64{Int64: pool.AccountID, Valid: true},
	}

	// Mock activities
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(pool, nil)
	env.OnActivity(commonActivities.CreateJob, mock.Anything, mock.Anything).Return(job, nil)
	env.OnActivity(poolActivity.UpdatingPool, mock.Anything, mock.Anything).Return(nil, nil)

	// Mock child workflow
	env.OnWorkflow(workflows.UpdatePoolWorkflow, mock.Anything, mock.Anything, pool, mock.Anything).Return(nil, nil)

	// Execute workflow
	env.ExecuteWorkflow(AutoTieringHotTierAutoResizeWorkflow, poolIdentifier)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Verify workflow status
	var status *time.Time
	encVal, err := env.QueryWorkflow(workflows.StatusQueryName)
	if err != nil {
		t.Fatalf("Failed to query workflow status: %v", err)
	}
	err = encVal.Get(&status)
	assert.NoError(t, err)
	assert.NotNil(t, status)
}

func TestAutoTieringHotTierAutoResizeWorkflow_FetchPoolError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}

	// Register activities
	env.RegisterActivity(syncSnapshotActivity)

	poolIdentifier := &database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	// Mock FetchPoolByUUID to fail
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(nil, assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(AutoTieringHotTierAutoResizeWorkflow, poolIdentifier)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())

	// Verify workflow status
	var status *time.Time
	encVal, err := env.QueryWorkflow(workflows.StatusQueryName)
	if err != nil {
		t.Fatalf("Failed to query workflow status: %v", err)
	}
	err = encVal.Get(&status)
	assert.NoError(t, err)
	assert.NotNil(t, status)
}

func TestAutoTieringHotTierAutoResizeWorkflow_CreateJobError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(syncSnapshotActivity)
	env.RegisterActivity(commonActivities)

	poolIdentifier := &database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes: 500000000000,
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	// Mock activities
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(pool, nil)
	env.OnActivity(commonActivities.CreateJob, mock.Anything, mock.Anything).Return(nil, assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(AutoTieringHotTierAutoResizeWorkflow, poolIdentifier)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestAutoTieringHotTierAutoResizeWorkflow_UpdatingPoolError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(syncSnapshotActivity)
	env.RegisterActivity(poolActivity)
	env.RegisterActivity(commonActivities)

	poolIdentifier := &database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes: 500000000000,
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{ID: 1, UUID: "job-uuid"},
		Type:       string(models.JobTypeUpdatePool),
		State:      string(models.JobsStatePROCESSING),
		IsAdminJob: true,
		WorkflowID: "test-workflow-id",
		AccountID:  sql.NullInt64{Int64: pool.AccountID, Valid: true},
	}

	// Mock activities
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(pool, nil)
	env.OnActivity(commonActivities.CreateJob, mock.Anything, mock.Anything).Return(job, nil)
	env.OnActivity(poolActivity.UpdatingPool, mock.Anything, mock.Anything).Return(nil, assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(AutoTieringHotTierAutoResizeWorkflow, poolIdentifier)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestAutoTieringHotTierAutoResizeWorkflow_UpdatePoolWorkflowError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)
	env.RegisterActivity(poolActivity)
	env.RegisterActivity(commonActivities)

	// Register workflows
	env.RegisterWorkflow(workflows.UpdatePoolWorkflow)

	poolIdentifier := &database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes: 500000000000,
		},
		SizeInBytes: 1000000000000,
		Description: "test pool",
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 1000,
			Iops:            100,
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{ID: 1, UUID: "job-uuid"},
		Type:       string(models.JobTypeUpdatePool),
		State:      string(models.JobsStatePROCESSING),
		IsAdminJob: true,
		WorkflowID: "test-workflow-id",
		AccountID:  sql.NullInt64{Int64: pool.AccountID, Valid: true},
	}

	// Mock activities
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(pool, nil)
	env.OnActivity(commonActivities.CreateJob, mock.Anything, mock.Anything).Return(job, nil)
	env.OnActivity(poolActivity.UpdatingPool, mock.Anything, mock.Anything).Return(nil, nil)

	// Mock child workflow to fail
	env.OnWorkflow(workflows.UpdatePoolWorkflow, mock.Anything, mock.Anything, pool, mock.Anything).Return(nil, assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(AutoTieringHotTierAutoResizeWorkflow, poolIdentifier)

	// Assert workflow completion with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

// Test case for empty pools list
func TestSyncVSAAutoTieringWorkflow_EmptyPoolsList(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)

	// Mock empty pools list
	var emptyPools []*database.PoolIdentifier
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything, mock.Anything).Return(emptyPools, nil).Once()

	// Mock empty consumption map
	emptyConsumptionMap := make(map[string]map[string]float64)
	env.OnActivity(autoTierActivity.FetchAndSavePoolsTieringInfo, mock.Anything, emptyPools).Return(emptyConsumptionMap, nil).Once()
	env.OnActivity(autoTierActivity.UpdatePoolTieringConsumptionInDB, mock.Anything, emptyConsumptionMap).Return(nil).Once()

	// Mock empty segregated pools
	emptySegregatedPools := map[string][]*database.PoolIdentifier{
		backgroundactivities.PoolsToPauseKey:      {},
		backgroundactivities.PoolsToResumeKey:     {},
		backgroundactivities.PoolsToAutoResizeKey: {},
	}
	env.OnActivity(autoTierActivity.SegregatePools, mock.Anything, emptyPools, emptyConsumptionMap).Return(emptySegregatedPools, nil).Once()

	// Execute workflow
	env.ExecuteWorkflow(SyncVSAAutoTieringWorkflow)

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

// Test case for when GetLocationFromVendorID fails
func TestSyncVSAAutoTieringWorkflow_GetLocationFromVendorIDError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(commonActivities)
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)

	pools := []*database.PoolIdentifier{
		{
			Name:      "test-pool-1",
			AccountID: 123,
			VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool-1",
			UUID:      "test-pool-1-uuid",
		},
	}

	poolsConsumptionMap := map[string]map[string]float64{
		"test-pool-1-uuid": {
			backgroundactivities.PoolConsumptionHotTier:  500000000000,
			backgroundactivities.PoolConsumptionColdTier: 600000000000,
		},
	}

	// Create a pool with invalid VendorID format that will cause GetLocationFromVendorID to fail
	segregatedPools := map[string][]*database.PoolIdentifier{
		backgroundactivities.PoolsToPauseKey:  {},
		backgroundactivities.PoolsToResumeKey: {},
		backgroundactivities.PoolsToAutoResizeKey: {
			{
				Name:      "test-pool-invalid-vendor-id",
				AccountID: 125,
				VendorID:  "invalid-vendor-id-format", // This will cause GetLocationFromVendorID to fail
				UUID:      "test-pool-invalid-vendor-id-uuid",
			},
		},
	}

	// Mock activities
	env.OnActivity(commonActivities.ListPoolsUUID, mock.Anything, mock.Anything).Return(pools, nil).Once()
	env.OnActivity(autoTierActivity.FetchAndSavePoolsTieringInfo, mock.Anything, pools).Return(poolsConsumptionMap, nil).Once()
	env.OnActivity(autoTierActivity.UpdatePoolTieringConsumptionInDB, mock.Anything, poolsConsumptionMap).Return(nil).Once()
	env.OnActivity(autoTierActivity.SegregatePools, mock.Anything, pools, poolsConsumptionMap).Return(segregatedPools, nil).Once()

	// Execute workflow
	env.ExecuteWorkflow(SyncVSAAutoTieringWorkflow)

	// Assert workflow completion with error due to invalid vendor ID
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

// Test case for AutoTieringPauseResumeWorkflow with invalid operation
func TestAutoTieringPauseResumeWorkflow_InvalidOperation(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	autoTierActivity := &backgroundactivities.AutoTierSyncActivity{}
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}
	poolActivity := &activities.PoolActivity{}
	commonActivities := &activities.CommonActivities{}

	// Register activities
	env.RegisterActivity(autoTierActivity)
	env.RegisterActivity(syncSnapshotActivity)
	env.RegisterActivity(poolActivity)
	env.RegisterActivity(commonActivities)

	poolIdentifier := database.PoolIdentifier{
		Name:      "test-pool",
		AccountID: 123,
		VendorID:  "/projects/test-project/locations/us-central1/pools/test-pool",
		UUID:      "test-pool-uuid",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 123,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			TieringStatus: datamodel.TieringStatusResumed,
		},
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
		},
		DeploymentName: "test-deployment",
	}

	nodes := []*datamodel.Node{
		{
			BaseModel:       datamodel.BaseModel{ID: 1},
			EndpointAddress: "test-endpoint",
		},
	}

	// Mock activities
	env.OnActivity(syncSnapshotActivity.FetchPoolByUUID, mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(pool, nil)
	env.OnActivity(commonActivities.GetNode, mock.Anything, mock.Anything).Return(nodes, nil)
	env.OnActivity(autoTierActivity.ToggleHotTierBypassModeForPoolVolumes, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(autoTierActivity.UpdateAggregateInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(autoTierActivity.UpdatePoolTieringThresholdAndStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow with invalid operation (neither pause nor resume)
	env.ExecuteWorkflow(AutoTieringPauseResumeWorkflow, poolIdentifier, "invalid-operation")

	// Assert workflow completion
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}
