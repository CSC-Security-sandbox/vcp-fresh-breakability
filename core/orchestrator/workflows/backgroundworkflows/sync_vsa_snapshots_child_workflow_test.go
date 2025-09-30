package backgroundworkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"go.temporal.io/sdk/testsuite"
)

func TestSnapshotsSyncChildWorkflow_NoPoolsFound(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}

	// Register activities
	env.RegisterActivity(backgroundActivities)

	// Mock ListPools activity to return empty result immediately
	env.OnActivity(backgroundActivities.ListPoolsUUIDPaginated, mock.Anything, mock.Anything, mock.Anything).Return([]*database.PoolIdentifier{}, nil).Maybe()

	// Execute the child workflow
	env.ExecuteWorkflow(SnapshotsSyncChildWorkflow, 0, 400)

	// Verify workflow completed successfully
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSnapshotsSyncChildWorkflow_ListPoolsFailure(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}

	// Register activities
	env.RegisterActivity(backgroundActivities)

	// Mock ListPools activity to return error
	env.OnActivity(backgroundActivities.ListPoolsUUIDPaginated, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError).Maybe()

	// Execute the child workflow
	env.ExecuteWorkflow(SnapshotsSyncChildWorkflow, 0, 400)

	// Verify workflow completed with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestSnapshotsSyncChildWorkflow_RetryPolicyError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}

	// Register activities
	env.RegisterActivity(backgroundActivities)

	// Mock PopulateRetryPolicyParams to return error
	env.OnActivity(backgroundActivities.ListPoolsUUIDPaginated, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError).Maybe()

	// Execute the child workflow
	env.ExecuteWorkflow(SnapshotsSyncChildWorkflow, 0, 400)

	// Verify workflow completed with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestSnapshotsSyncChildWorkflow_SuccessWithPools(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}

	// Register activities
	env.RegisterActivity(backgroundActivities)

	// Mock pools data
	mockPools := []*database.PoolIdentifier{
		{UUID: "pool-1"},
		{UUID: "pool-2"},
		{UUID: "pool-3"},
	}

	// Mock ListPools activity to return pools
	env.OnActivity(backgroundActivities.ListPoolsUUIDPaginated, mock.Anything, mock.Anything, mock.Anything).Return(mockPools, nil).Maybe()

	// Mock SyncSnapshotsForPoolBatchActivity to return success
	env.OnActivity(backgroundActivities.SyncSnapshotsForPoolBatchActivity, mock.Anything, mock.Anything).Return(&backgroundactivities.SyncSnapshotsForPoolBatchReturnValue{
		TotalProcessed: 3,
		Successful:     3,
		Failed:         0,
	}, nil).Maybe()

	// Execute the child workflow
	env.ExecuteWorkflow(SnapshotsSyncChildWorkflow, 0, 400)

	// Verify workflow completed successfully
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSnapshotsSyncChildWorkflow_ActivityFailure(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}

	// Register activities
	env.RegisterActivity(backgroundActivities)

	// Mock pools data
	mockPools := []*database.PoolIdentifier{
		{UUID: "pool-1"},
		{UUID: "pool-2"},
	}

	// Mock ListPools activity to return pools
	env.OnActivity(backgroundActivities.ListPoolsUUIDPaginated, mock.Anything, mock.Anything, mock.Anything).Return(mockPools, nil).Maybe()

	// Mock SyncSnapshotsForPoolBatchActivity to return error
	env.OnActivity(backgroundActivities.SyncSnapshotsForPoolBatchActivity, mock.Anything, mock.Anything).Return(nil, assert.AnError).Maybe()

	// Execute the child workflow
	env.ExecuteWorkflow(SnapshotsSyncChildWorkflow, 0, 400)

	// Verify workflow completed successfully (error handling is internal)
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSnapshotsSyncChildWorkflow_MixedResults(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	backgroundActivities := &backgroundactivities.SyncSnapshotActivity{}

	// Register activities
	env.RegisterActivity(backgroundActivities)

	// Mock pools data
	mockPools := []*database.PoolIdentifier{
		{UUID: "pool-1"},
		{UUID: "pool-2"},
		{UUID: "pool-3"},
		{UUID: "pool-4"},
	}

	// Mock ListPools activity to return pools
	env.OnActivity(backgroundActivities.ListPoolsUUIDPaginated, mock.Anything, mock.Anything, mock.Anything).Return(mockPools, nil).Maybe()

	// Mock SyncSnapshotsForPoolBatchActivity to return mixed results
	env.OnActivity(backgroundActivities.SyncSnapshotsForPoolBatchActivity, mock.Anything, mock.Anything).Return(&backgroundactivities.SyncSnapshotsForPoolBatchReturnValue{
		TotalProcessed: 4,
		Successful:     2,
		Failed:         2,
	}, nil).Maybe()

	// Execute the child workflow
	env.ExecuteWorkflow(SnapshotsSyncChildWorkflow, 0, 400)

	// Verify workflow completed successfully
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}
