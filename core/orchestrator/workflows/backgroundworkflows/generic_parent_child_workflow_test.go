package backgroundworkflows

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"go.temporal.io/sdk/workflow"
)

// Mock activities for testing
type MockGetTotalCountActivity struct {
	ReturnValue int
	ReturnError error
}

func (m *MockGetTotalCountActivity) GetTotalCount(ctx context.Context) (int, error) {
	return m.ReturnValue, m.ReturnError
}

type MockListDataActivity struct {
	ReturnValue []*database.PoolIdentifier
	ReturnError error
}

func (m *MockListDataActivity) ListData(ctx context.Context, offset, limit int) ([]*database.PoolIdentifier, error) {
	return m.ReturnValue, m.ReturnError
}

type MockProcessBatchActivity struct {
	ReturnValue *backgroundactivities.SyncSnapshotsForPoolBatchReturnValue
	ReturnError error
}

func (m *MockProcessBatchActivity) ProcessBatch(ctx context.Context, data []*database.PoolIdentifier) (*backgroundactivities.SyncSnapshotsForPoolBatchReturnValue, error) {
	return m.ReturnValue, m.ReturnError
}

// Mock child workflow function
func MockChildWorkflow(ctx workflow.Context, offset, limit int) (*GenericChildWorkflowResult, error) {
	// This is a mock implementation for testing
	return &GenericChildWorkflowResult{
		TotalItemsProcessed: limit,
		SuccessfulItems:     limit,
		FailedItems:         0,
	}, nil
}
