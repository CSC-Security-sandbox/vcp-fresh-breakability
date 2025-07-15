package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"go.temporal.io/sdk/testsuite"
)

func TestRegisterNodeToHarvestFarmWorkflowInput(t *testing.T) {
	input := RegisterNodeToHarvestFarmWorkflowInput{PoolID: 123, MaxNodesPerGroup: 5}
	assert.Equal(t, int64(123), input.PoolID)
	assert.Equal(t, 5, input.MaxNodesPerGroup)
}

func newTestActivityWithMockStorage(mockStorage *database.MockStorage) *activities.RegisterNodeToHarvestFarmActivity {
	return &activities.RegisterNodeToHarvestFarmActivity{
		SE: mockStorage,
	}
}

func TestRegisterNodeToHarvestFarmWorkflow_ExecutesSuccessfully(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	input := RegisterNodeToHarvestFarmWorkflowInput{PoolID: 1, MaxNodesPerGroup: 5}

	mockStorage := &database.MockStorage{}
	activity := newTestActivityWithMockStorage(mockStorage)
	env.RegisterActivity(activity)

	node1 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 101}}
	node2 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 102}}

	mockStorage.On("GetNodesByPoolID", mock.Anything, input.PoolID).Return([]*datamodel.Node{node1, node2}, nil)
	mockStorage.On("AssignTwoNodesToTwoGroups", mock.Anything, node1, node2, input.MaxNodesPerGroup).Return([]*datamodel.NodeNodeGroupMap{{NodeID: node1.ID}, {NodeID: node2.ID}}, nil)

	// Mock validate and create lease activity
	env.OnActivity(activity.ValidateAndCreateKubernetesLease, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(RegisterNodeToHarvestFarmWorkflow, input)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	mockStorage.AssertExpectations(t)
}

func TestRegisterNodeToHarvestFarmWorkflow_ActivityFails(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	input := RegisterNodeToHarvestFarmWorkflowInput{PoolID: 1, MaxNodesPerGroup: 5}

	mockStorage := &database.MockStorage{}
	activity := newTestActivityWithMockStorage(mockStorage)
	env.RegisterActivity(activity.RegisterNodeToHarvestFarm)

	// Simulate error in GetNodesByPoolID
	mockStorage.On("GetNodesByPoolID", mock.Anything, input.PoolID).Return(nil, assert.AnError)
	// Do not mock AssignTwoNodesToTwoGroups, as it should not be called if GetNodesByPoolID fails

	env.ExecuteWorkflow(RegisterNodeToHarvestFarmWorkflow, input)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	mockStorage.AssertExpectations(t)
}

func TestRegisterNodeToHarvestFarmWorkflow_InvalidInput(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	input := RegisterNodeToHarvestFarmWorkflowInput{PoolID: 1, MaxNodesPerGroup: 0}

	mockStorage := &database.MockStorage{}
	activity := newTestActivityWithMockStorage(mockStorage)
	env.RegisterActivity(activity.RegisterNodeToHarvestFarm)

	node1 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 101}}
	node2 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 102}}
	mockStorage.On("GetNodesByPoolID", mock.Anything, input.PoolID).Return([]*datamodel.Node{node1, node2}, nil)
	mockStorage.On("AssignTwoNodesToTwoGroups", mock.Anything, node1, node2, input.MaxNodesPerGroup).Return(nil, assert.AnError)

	env.OnActivity(activity.ValidateAndCreateKubernetesLease, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(RegisterNodeToHarvestFarmWorkflow, input)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	mockStorage.AssertExpectations(t)
}

func TestRegisterNodeToHarvestFarmWorkflow_SameNodeIDs(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	input := RegisterNodeToHarvestFarmWorkflowInput{PoolID: 1, MaxNodesPerGroup: 5}

	mockStorage := &database.MockStorage{}
	activity := newTestActivityWithMockStorage(mockStorage)
	env.RegisterActivity(activity.RegisterNodeToHarvestFarm)

	node1 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 101}}
	node2 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 101}} // same ID
	mockStorage.On("GetNodesByPoolID", mock.Anything, input.PoolID).Return([]*datamodel.Node{node1, node2}, nil)
	mockStorage.On("AssignTwoNodesToTwoGroups", mock.Anything, node1, node2, input.MaxNodesPerGroup).Return(nil, assert.AnError)

	env.ExecuteWorkflow(RegisterNodeToHarvestFarmWorkflow, input)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	mockStorage.AssertExpectations(t)
}

func TestRegisterNodeToHarvestFarmWorkflow_LargeGroupSize(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	// Test with a large group size
	input := RegisterNodeToHarvestFarmWorkflowInput{PoolID: 1, MaxNodesPerGroup: 1000}

	mockStorage := &database.MockStorage{}
	activity := newTestActivityWithMockStorage(mockStorage)
	env.RegisterActivity(activity)

	node1 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 101}}
	node2 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 102}}
	mockStorage.On("GetNodesByPoolID", mock.Anything, input.PoolID).Return([]*datamodel.Node{node1, node2}, nil)
	mockStorage.On("AssignTwoNodesToTwoGroups", mock.Anything, node1, node2, input.MaxNodesPerGroup).Return([]*datamodel.NodeNodeGroupMap{{NodeID: node1.ID}, {NodeID: node2.ID}}, nil)

	env.OnActivity(activity.ValidateAndCreateKubernetesLease, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(RegisterNodeToHarvestFarmWorkflow, input)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	mockStorage.AssertExpectations(t)
}

func TestRegisterNodeToHarvestFarmWorkflow_LeaseCreateFailure(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	input := RegisterNodeToHarvestFarmWorkflowInput{PoolID: 1, MaxNodesPerGroup: 5}

	mockStorage := &database.MockStorage{}
	activity := newTestActivityWithMockStorage(mockStorage)
	env.RegisterActivity(activity)

	node1 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 101}}
	node2 := &datamodel.Node{BaseModel: datamodel.BaseModel{ID: 102}}

	mockStorage.On("GetNodesByPoolID", mock.Anything, input.PoolID).Return([]*datamodel.Node{node1, node2}, nil)
	mockStorage.On("AssignTwoNodesToTwoGroups", mock.Anything, node1, node2, input.MaxNodesPerGroup).Return([]*datamodel.NodeNodeGroupMap{{NodeID: node1.ID}, {NodeID: node2.ID}}, nil)

	env.ExecuteWorkflow(RegisterNodeToHarvestFarmWorkflow, input)

	wfErr := env.GetWorkflowError()
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, wfErr)
	assert.Contains(t, wfErr.Error(), "failed to fetch node group details from nodeGroup Map table")
	mockStorage.AssertExpectations(t)
}
