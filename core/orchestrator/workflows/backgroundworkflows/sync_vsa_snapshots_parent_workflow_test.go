package backgroundworkflows

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"
)

// Mock SyncSnapshotActivity for testing
type MockSyncSnapshotActivity struct {
	GetTotalPoolCountReturnValue int
	GetTotalPoolCountReturnError error
}

func (m *MockSyncSnapshotActivity) GetTotalPoolCount(ctx context.Context) (int, error) {
	return m.GetTotalPoolCountReturnValue, m.GetTotalPoolCountReturnError
}

func TestSnapshotsSyncParentWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Create mock activity
	mockActivity := &MockSyncSnapshotActivity{
		GetTotalPoolCountReturnValue: 1000,
		GetTotalPoolCountReturnError: nil,
	}

	// Register activities
	env.RegisterActivity(mockActivity)

	// Register child workflow
	env.RegisterWorkflow(SnapshotsSyncChildWorkflow)

	// Mock child workflow execution
	env.OnWorkflow("SnapshotsSyncChildWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(
		&GenericChildWorkflowResult{
			TotalItemsProcessed: 3000, // ParentWorkflowBatchSize
			SuccessfulItems:     3000,
			FailedItems:         0,
		}, nil)

	// Execute the parent workflow
	env.ExecuteWorkflow(SnapshotsSyncParentWorkflow)

	// Verify workflow completed successfully
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSnapshotsSyncParentWorkflow_GetTotalCountFailure(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Create mock activity
	mockActivity := &MockSyncSnapshotActivity{
		GetTotalPoolCountReturnValue: 0,
		GetTotalPoolCountReturnError: assert.AnError,
	}

	// Register activities
	env.RegisterActivity(mockActivity)

	// Execute the parent workflow
	env.ExecuteWorkflow(SnapshotsSyncParentWorkflow)

	// Verify workflow completed with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestSnapshotsSyncParentWorkflow_ChildWorkflowFailure(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Create mock activity
	mockActivity := &MockSyncSnapshotActivity{
		GetTotalPoolCountReturnValue: 1000,
		GetTotalPoolCountReturnError: nil,
	}

	// Register activities
	env.RegisterActivity(mockActivity)

	// Register child workflow
	env.RegisterWorkflow(SnapshotsSyncChildWorkflow)

	// Mock child workflow to fail
	env.OnWorkflow("SnapshotsSyncChildWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(
		nil, assert.AnError)

	// Execute the parent workflow
	env.ExecuteWorkflow(SnapshotsSyncParentWorkflow)

	// Verify workflow completed successfully (parent continues even if child fails)
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSnapshotsSyncParentWorkflow_ZeroPools(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Create mock activity
	mockActivity := &MockSyncSnapshotActivity{
		GetTotalPoolCountReturnValue: 0,
		GetTotalPoolCountReturnError: nil,
	}

	// Register activities
	env.RegisterActivity(mockActivity)

	// Execute the parent workflow
	env.ExecuteWorkflow(SnapshotsSyncParentWorkflow)

	// Verify workflow completed successfully
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSnapshotsSyncParentWorkflow_LargeDataset(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Create mock activity
	mockActivity := &MockSyncSnapshotActivity{
		GetTotalPoolCountReturnValue: 10000,
		GetTotalPoolCountReturnError: nil,
	}

	// Register activities
	env.RegisterActivity(mockActivity)

	// Register child workflow
	env.RegisterWorkflow(SnapshotsSyncChildWorkflow)

	// Mock child workflow execution for multiple child workflows
	env.OnWorkflow("SnapshotsSyncChildWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(
		&GenericChildWorkflowResult{
			TotalItemsProcessed: 3000,
			SuccessfulItems:     3000,
			FailedItems:         0,
		}, nil)

	// Execute the parent workflow
	env.ExecuteWorkflow(SnapshotsSyncParentWorkflow)

	// Verify workflow completed successfully
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}
