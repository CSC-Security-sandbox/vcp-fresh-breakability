package workflows

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"gorm.io/gorm"
)

type UnRegisterUnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *UnRegisterUnitTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	s.env.SetHeader(mockHeader)

	// Register workflow
	s.env.RegisterWorkflow(UnRegisterNodeFromHarvestFarmWorkflow)
}

func (s *UnRegisterUnitTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func TestUnRegisterUnitTestSuite(t *testing.T) {
	suite.Run(t, new(UnRegisterUnitTestSuite))
}

const (
	nodeCount int = 2
)

func getUnRegisterNodes() []*datamodel.Node {
	var nodes []*datamodel.Node
	createdAt := time.Now()
	for i := 0; i < nodeCount; i++ {
		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:        int64(i),
				CreatedAt: createdAt,
				UpdatedAt: createdAt,
				DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true},
				UUID:      "test-node-uuid-" + strconv.Itoa(i)},
			State: models.LifeCycleStateDeleted,
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func getNodeGroupMap(isDelete bool) []*datamodel.NodeNodeGroupMap {
	var nodeGroupMap []*datamodel.NodeNodeGroupMap
	createdAt := time.Now()
	for i := 0; i < nodeCount; i++ {
		groupMap := &datamodel.NodeNodeGroupMap{
			BaseModel: datamodel.BaseModel{
				ID:        int64(i),
				CreatedAt: createdAt,
				UpdatedAt: createdAt,
				UUID:      "test-nodegroup-map-uuid-" + strconv.Itoa(i)},
			NodeID:      int64(i),
			NodeGroupID: int64(i),
			NodeGroup: &datamodel.NodeGroup{
				BaseModel: datamodel.BaseModel{ID: int64(i), CreatedAt: createdAt, UpdatedAt: createdAt},
				LeaseName: "test-harvest-lease-" + strconv.Itoa(i),
				Name:      "test-harvest-name-" + strconv.Itoa(i),
			},
		}
		if isDelete {
			groupMap.DeletedAt = &gorm.DeletedAt{Time: createdAt, Valid: true}
		}
		nodeGroupMap = append(nodeGroupMap, groupMap)
	}
	return nodeGroupMap
}

func (s *UnRegisterUnitTestSuite) Test_UnRegisterNodeFromHarvestFarmWorkflowSuccess() {
	mockStorage := database.NewMockStorage(s.T())
	unRegisterHarvestActivity := &activities.UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	s.env.RegisterActivity(unRegisterHarvestActivity)

	nodesInfo := getUnRegisterNodes()
	nodeGroupMapInfo := getNodeGroupMap(false)

	testParams := &unRegisterNodeFromHarvestFarmParams{PoolID: int64(1)}

	mockStorage.On("GetNodesByPoolID", mock.Anything, testParams.PoolID).Return(nodesInfo, nil)
	for index, nodeInfo := range nodesInfo {
		mockStorage.On("GetNodeNodeGroupMapByNodeID", mock.Anything, nodeInfo.ID).Return(nodeGroupMapInfo[index], nil)
		mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, nodeInfo.ID).Return(nil)
		mockStorage.On("GetNodeGroupMapNodeCount", mock.Anything, nodeGroupMapInfo[index].NodeGroupID).Return(int64(1), nil)
	}
	// Mock activities
	s.env.OnActivity(unRegisterHarvestActivity.DeletePollersFromHarvestFarm, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(UnRegisterNodeFromHarvestFarmWorkflow, testParams)
	// Assert workflow success
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnRegisterUnitTestSuite) Test_UnRegisterNodeToHarvestFailWithNoNodes() {
	mockStorage := database.NewMockStorage(s.T())
	unRegisterHarvestActivity := &activities.UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	s.env.RegisterActivity(unRegisterHarvestActivity)

	nodesInfo := []*datamodel.Node{}

	testParams := &unRegisterNodeFromHarvestFarmParams{PoolID: int64(1)}

	mockStorage.On("GetNodesByPoolID", mock.Anything, testParams.PoolID).Return(nodesInfo, nil)

	// Execute workflow
	s.env.ExecuteWorkflow(UnRegisterNodeFromHarvestFarmWorkflow, testParams)
	// Assert workflow failure
	wfErr := s.env.GetWorkflowError()
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), wfErr)
	assert.Contains(s.T(), wfErr.Error(), nodesInfoNotAvailable)
}

func (s *UnRegisterUnitTestSuite) Test_UnRegisterNodeToHarvestFailWithNoNodeGroups() {
	mockStorage := database.NewMockStorage(s.T())
	unRegisterHarvestActivity := &activities.UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	s.env.RegisterActivity(unRegisterHarvestActivity)

	nodesInfo := getUnRegisterNodes()
	nodeGroupMapInfo := getNodeGroupMap(true)

	testParams := &unRegisterNodeFromHarvestFarmParams{PoolID: int64(1)}

	mockStorage.On("GetNodesByPoolID", mock.Anything, testParams.PoolID).Return(nodesInfo, nil)
	for i, nodeInfo := range nodesInfo {
		mockStorage.On("GetNodeNodeGroupMapByNodeID", mock.Anything, nodeInfo.ID).Return(nodeGroupMapInfo[i], nil)
	}

	// Execute workflow
	s.env.ExecuteWorkflow(UnRegisterNodeFromHarvestFarmWorkflow, testParams)
	// Assert workflow failure
	wfErr := s.env.GetWorkflowError()
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), wfErr)
	assert.Contains(s.T(), wfErr.Error(), nodeGroupMapNotAvailable)
}

func (s *UnRegisterUnitTestSuite) Test_UnRegisterNodeToHarvestFarmFailToDeleteNodeGroup() {
	mockStorage := database.NewMockStorage(s.T())
	unRegisterHarvestActivity := &activities.UnRegisterNodeFromHarvestActivity{SE: mockStorage}
	s.env.RegisterActivity(unRegisterHarvestActivity)

	nodesInfo := getUnRegisterNodes()
	nodeGroupMapInfo := getNodeGroupMap(false)

	testParams := &unRegisterNodeFromHarvestFarmParams{PoolID: int64(1)}

	mockStorage.On("GetNodesByPoolID", mock.Anything, testParams.PoolID).Return(nodesInfo, nil)
	for index, nodeInfo := range nodesInfo {
		mockStorage.On("GetNodeNodeGroupMapByNodeID", mock.Anything, nodeInfo.ID).Return(nodeGroupMapInfo[index], nil)
	}
	mockStorage.On("DeleteNodeNodeGroupMap", mock.Anything, nodesInfo[0].ID).Return(errors.New(nodeGroupMapNotAvailable))
	// Mock activities
	s.env.OnActivity(unRegisterHarvestActivity.DeletePollersFromHarvestFarm, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(UnRegisterNodeFromHarvestFarmWorkflow, testParams)
	// Assert workflow failure
	wfErr := s.env.GetWorkflowError()
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), wfErr)
	assert.Contains(s.T(), wfErr.Error(), nodeGroupMapNotAvailable)
}
