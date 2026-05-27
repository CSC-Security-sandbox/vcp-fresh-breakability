package backgroundworkflows

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"go.temporal.io/sdk/testsuite"
)

func TestResourceCleanupParentWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Create mock activity
	mockActivity := &MockResourceDeleteActivity{
		GetTotalResourceCountReturnValue: 1000,
		GetTotalResourceCountReturnError: nil,
	}

	// Register activities
	env.RegisterActivity(mockActivity)

	// Register child workflow
	env.RegisterWorkflow(ResourceCleanupChildWorkflow)

	// Mock child workflow execution
	env.OnWorkflow("ResourceCleanupChildWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(
		&GenericChildWorkflowResult{
			TotalItemsProcessed: 1000, // ResourceCleanupParentWorkflowBatchSize
			SuccessfulItems:     1000,
			FailedItems:         0,
		}, nil)

	// Execute the parent workflow
	env.ExecuteWorkflow(ResourceCleanupParentWorkflow)

	// Verify workflow completed successfully
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestResourceCleanupParentWorkflow_GetTotalCountFailure(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Create mock activity that returns error
	mockActivity := &MockResourceDeleteActivity{
		GetTotalResourceCountReturnValue: 0,
		GetTotalResourceCountReturnError: assert.AnError,
	}

	// Register activities
	env.RegisterActivity(mockActivity)

	// Execute the parent workflow
	env.ExecuteWorkflow(ResourceCleanupParentWorkflow)

	// Verify workflow failed
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestResourceCleanupParentWorkflow_ZeroResources(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Create mock activity that returns zero count
	mockActivity := &MockResourceDeleteActivity{
		GetTotalResourceCountReturnValue: 0,
		GetTotalResourceCountReturnError: nil,
	}

	// Register activities
	env.RegisterActivity(mockActivity)

	// Execute the parent workflow
	env.ExecuteWorkflow(ResourceCleanupParentWorkflow)

	// Verify workflow completed successfully
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

// MockResourceDeleteActivity is a mock for testing
type MockResourceDeleteActivity struct {
	GetTotalResourceCountReturnValue   int
	GetTotalResourceCountReturnError   error
	ListResourcesPaginatedReturnValue  []*datamodel.PendingResourceDeletions
	ListResourcesPaginatedReturnError  error
	CleanupPendingResourcesReturnValue *backgroundactivities.ResourceCleanupBatchReturnValue
	CleanupPendingResourcesReturnError error
}

func (m *MockResourceDeleteActivity) GetTotalResourceCount(ctx context.Context) (int, error) {
	return m.GetTotalResourceCountReturnValue, m.GetTotalResourceCountReturnError
}

func (m *MockResourceDeleteActivity) ListResourcesPaginated(ctx context.Context, offset, limit int) ([]*datamodel.PendingResourceDeletions, error) {
	return m.ListResourcesPaginatedReturnValue, m.ListResourcesPaginatedReturnError
}

func (m *MockResourceDeleteActivity) CleanupPendingResources(ctx context.Context, resources []*datamodel.PendingResourceDeletions) (*backgroundactivities.ResourceCleanupBatchReturnValue, error) {
	return m.CleanupPendingResourcesReturnValue, m.CleanupPendingResourcesReturnError
}
