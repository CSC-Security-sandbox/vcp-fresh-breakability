package workflows

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func setupReconcileHarvestWorkflowEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	origStartToClose := StartToCloseTimeout
	origRetryInterval := RetryInterval
	origRetryMaxAttempts := RetryMaxAttempts
	origRetryMaxInterval := RetryMaxInterval
	origRetryBackoff := RetryBackoff
	origHeartbeat := ActivityHeartBeatTimeout

	StartToCloseTimeout = "10s"
	RetryInterval = "1s"
	RetryMaxAttempts = 1
	RetryMaxInterval = "2s"
	RetryBackoff = "1.0"
	ActivityHeartBeatTimeout = "5s"

	t.Cleanup(func() {
		StartToCloseTimeout = origStartToClose
		RetryInterval = origRetryInterval
		RetryMaxAttempts = origRetryMaxAttempts
		RetryMaxInterval = origRetryMaxInterval
		RetryBackoff = origRetryBackoff
		ActivityHeartBeatTimeout = origHeartbeat
	})

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	env.SetHeader(&commonpb.Header{
		Fields: map[string]*commonpb.Payload{"logParam": encodedValue},
	})
	env.RegisterWorkflow(ReconcileHarvestNodeGroupMapWorkflow)
	return env
}

func TestReconcileHarvestNodeGroupMapWorkflow_Success_EmptyList(t *testing.T) {
	env := setupReconcileHarvestWorkflowEnv(t)
	mockStorage := database.NewMockStorage(t)
	reconcileActivity := &activities.UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	env.RegisterActivity(reconcileActivity)

	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 100).Return([]*datamodel.NodeNodeGroupMap{}, nil)

	env.ExecuteWorkflow(ReconcileHarvestNodeGroupMapWorkflow, nil)
	require.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	mockStorage.AssertExpectations(t)
}

func TestReconcileHarvestNodeGroupMapWorkflow_Success_WithParamsPageSize(t *testing.T) {
	env := setupReconcileHarvestWorkflowEnv(t)
	mockStorage := database.NewMockStorage(t)
	reconcileActivity := &activities.UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	env.RegisterActivity(reconcileActivity)

	params := &ReconcileHarvestNodeGroupMapParams{PageSize: 50}
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 50).Return([]*datamodel.NodeNodeGroupMap{}, nil)

	env.ExecuteWorkflow(ReconcileHarvestNodeGroupMapWorkflow, params)
	require.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	mockStorage.AssertExpectations(t)
}

func TestReconcileHarvestNodeGroupMapWorkflow_Success_WithMaps_ReconcilesBatch(t *testing.T) {
	env := setupReconcileHarvestWorkflowEnv(t)
	mockStorage := database.NewMockStorage(t)
	reconcileActivity := &activities.UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	env.RegisterActivity(reconcileActivity)

	createdAt := time.Now()
	m := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1, CreatedAt: createdAt, UpdatedAt: createdAt},
		NodeID:    10, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1", LeaseName: ""},
	}
	nodeDeleted := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 10}, State: models.LifeCycleStateDeleted}
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 100).Return([]*datamodel.NodeNodeGroupMap{m}, nil)
	mockStorage.On("GetNodeByID", mock.Anything, int64(10)).Return(nodeDeleted, nil)
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(1)).Return(nil)

	env.ExecuteWorkflow(ReconcileHarvestNodeGroupMapWorkflow, nil)
	require.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	mockStorage.AssertExpectations(t)
}

func TestReconcileHarvestNodeGroupMapWorkflow_ListAllMapsFails_ReturnsError(t *testing.T) {
	env := setupReconcileHarvestWorkflowEnv(t)
	mockStorage := database.NewMockStorage(t)
	reconcileActivity := &activities.UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	env.RegisterActivity(reconcileActivity)

	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 100).Return(([]*datamodel.NodeNodeGroupMap)(nil), assert.AnError)

	env.ExecuteWorkflow(ReconcileHarvestNodeGroupMapWorkflow, nil)
	require.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	mockStorage.AssertExpectations(t)
}

// Batch activity skips failed DeleteNodeNodeGroupMap calls (logs and continues), so the workflow still succeeds.
func TestReconcileHarvestNodeGroupMapWorkflow_BatchSkipsFailedDeletes_WorkflowSucceeds(t *testing.T) {
	env := setupReconcileHarvestWorkflowEnv(t)
	mockStorage := database.NewMockStorage(t)
	reconcileActivity := &activities.UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	env.RegisterActivity(reconcileActivity)

	createdAt := time.Now()
	m := &datamodel.NodeNodeGroupMap{
		BaseModel: datamodel.BaseModel{ID: 1, CreatedAt: createdAt, UpdatedAt: createdAt},
		NodeID:    10, NodeGroupID: 100,
		NodeGroup: &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 100}, Name: "g1"},
	}
	nodeDeleted := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 10}, State: models.LifeCycleStateDeleted}
	mockStorage.On("ListNodeNodeGroupMapAfterID", mock.Anything, false, int64(0), 100).Return([]*datamodel.NodeNodeGroupMap{m}, nil)
	mockStorage.On("GetNodeByID", mock.Anything, int64(10)).Return(nodeDeleted, nil)
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, int64(1)).Return(assert.AnError)

	env.ExecuteWorkflow(ReconcileHarvestNodeGroupMapWorkflow, nil)
	require.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	mockStorage.AssertExpectations(t)
}

func TestReconcileHarvestNodeGroupMapWorkflow_PopulateRetryPolicyParamsError_ReturnsError(t *testing.T) {
	origStartToClose := StartToCloseTimeout
	StartToCloseTimeout = "invalid-duration"
	t.Cleanup(func() { StartToCloseTimeout = origStartToClose })

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	env.SetHeader(&commonpb.Header{
		Fields: map[string]*commonpb.Payload{"logParam": encodedValue},
	})
	env.RegisterWorkflow(ReconcileHarvestNodeGroupMapWorkflow)
	mockStorage := database.NewMockStorage(t)
	reconcileActivity := &activities.UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	env.RegisterActivity(reconcileActivity)

	env.ExecuteWorkflow(ReconcileHarvestNodeGroupMapWorkflow, nil)
	require.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	mockStorage.AssertExpectations(t)
}
