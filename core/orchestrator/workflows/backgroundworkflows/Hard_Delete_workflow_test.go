package backgroundworkflows

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/resource_events_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

type mockTemporalClient struct {
	mock.Mock
	client.Client
}

func (m *mockTemporalClient) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, wf interface{}, args ...interface{}) (client.WorkflowRun, error) {
	if len(args) > 0 {
		called := m.Called(ctx, options, wf, args[0])
		if run := called.Get(0); run != nil {
			return run.(client.WorkflowRun), called.Error(1)
		}
		return nil, called.Error(1)
	}
	called := m.Called(ctx, options, wf)
	if run := called.Get(0); run != nil {
		return run.(client.WorkflowRun), called.Error(1)
	}
	return nil, called.Error(1)
}

func TestHardDeleteResourcesAndAccountWorkflow_Disabled(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	orig := scheduleHardDelete
	scheduleHardDelete = false
	defer func() { scheduleHardDelete = orig }()

	env.ExecuteWorkflow(HardDeleteResourcesAndAccountWorkflow)
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncForHardDeleteWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "acc-10"},
		Name:      "acc-10",
	}

	mockStorage := database.NewMockStorage(t)
	finishActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	hardDeleteActivity := &backgroundactivities.HardDeleteResourcesAndAccountActivity{SE: mockStorage}

	// Register all activities used by the workflow
	env.RegisterActivity(hardDeleteActivity.AccountAudit)
	env.RegisterActivity(finishActivity.VerifySoftDeletedResourcesForAccount)
	env.RegisterActivity(finishActivity.HardDeleteResourcesInOrder)

	// Mock AccountAudit to return the test account
	env.OnActivity(hardDeleteActivity.AccountAudit, mock.Anything).Return([]*datamodel.Account{account}, nil)
	env.OnActivity(finishActivity.VerifySoftDeletedResourcesForAccount, mock.Anything, account.Name).Return(true, nil)
	env.OnActivity(finishActivity.HardDeleteResourcesInOrder, mock.Anything, account.Name).Return(nil)

	env.ExecuteWorkflow(SyncForHardDeleteWorkflow)
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	qr, err := env.QueryWorkflow(workflows.StatusQueryName)
	assert.NoError(t, err)
	var status workflows.WorkflowStatus
	assert.NoError(t, qr.Get(&status))
	assert.Equal(t, workflows.WorkflowStatusCompleted, status.Status)
	env.AssertExpectations(t)
}

func TestSyncForHardDeleteWorkflow_ActivityError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 11, UUID: "acc-11"},
		Name:      "acc-11",
	}

	mockStorage := database.NewMockStorage(t)
	finishActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	hardDeleteActivity := &backgroundactivities.HardDeleteResourcesAndAccountActivity{SE: mockStorage}

	// Register all activities used by the workflow
	env.RegisterActivity(hardDeleteActivity.AccountAudit)
	env.RegisterActivity(finishActivity.VerifySoftDeletedResourcesForAccount)
	env.RegisterActivity(finishActivity.HardDeleteResourcesInOrder)

	// Mock AccountAudit to return the test account
	env.OnActivity(hardDeleteActivity.AccountAudit, mock.Anything).Return([]*datamodel.Account{account}, nil)
	env.OnActivity(finishActivity.VerifySoftDeletedResourcesForAccount, mock.Anything, account.Name).Return(true, nil)
	env.OnActivity(finishActivity.HardDeleteResourcesInOrder, mock.Anything, account.Name).Return(errors.New("hard delete failed"))

	env.ExecuteWorkflow(SyncForHardDeleteWorkflow)
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "hard delete failed")

	qr, err := env.QueryWorkflow(workflows.StatusQueryName)
	assert.NoError(t, err)
	var status workflows.WorkflowStatus
	assert.NoError(t, qr.Get(&status))
	assert.Equal(t, workflows.WorkflowStatusFailed, status.Status)
	env.AssertExpectations(t)
}

func TestSyncForHardDeleteWorkflow_NilAccount(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(SyncForHardDeleteWorkflow)
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestHardDeleteResourcesAndAccountFunc_Error(t *testing.T) {
	mockClient := &mockTemporalClient{}
	mockClient.
		On("ExecuteWorkflow",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("temporal start failure"))

	origFetch := fetchTemporalClient
	fetchTemporalClient = func(ctx context.Context) client.Client { return mockClient }
	defer func() { fetchTemporalClient = origFetch }()

	h := &HardDeleteResourcesAndAccountworkflow{}
	err := h.HardDeleteResourcesAndAccountFunc(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "temporal start failure", err.Error())
	mockClient.AssertExpectations(t)
}

// Test for custom error handling
func TestSyncForHardDeleteWorkflow_CustomError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 30, UUID: "acc-30"},
		Name:      "acc-30",
	}

	mockStorage := database.NewMockStorage(t)
	finishActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	hardDeleteActivity := &backgroundactivities.HardDeleteResourcesAndAccountActivity{SE: mockStorage}

	customErr := errors.New("internal failure")

	env.RegisterActivity(hardDeleteActivity.AccountAudit)
	env.RegisterActivity(finishActivity.VerifySoftDeletedResourcesForAccount)
	env.RegisterActivity(finishActivity.HardDeleteResourcesInOrder)

	// Mock AccountAudit to return the test account
	env.OnActivity(hardDeleteActivity.AccountAudit, mock.Anything).Return([]*datamodel.Account{account}, nil)
	env.OnActivity(finishActivity.VerifySoftDeletedResourcesForAccount, mock.Anything, account.Name).Return(true, nil)
	env.OnActivity(finishActivity.HardDeleteResourcesInOrder, mock.Anything, account.Name).Return(customErr)

	env.ExecuteWorkflow(SyncForHardDeleteWorkflow)
	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "internal failure")
	env.AssertExpectations(t)
}
