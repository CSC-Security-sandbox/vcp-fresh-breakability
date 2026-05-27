package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
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

type HarvestUpgradeTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *HarvestUpgradeTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(HarvestPollerUpgradeWorkFlow)
}

func (s *HarvestUpgradeTestSuite) AfterTest(suiteName, testName string) {
	s.env.AssertExpectations(s.T())
}

func TestHarvestUpgradeTestSuite(t *testing.T) {
	suite.Run(t, new(HarvestUpgradeTestSuite))
}

func (s *HarvestUpgradeTestSuite) TestHarvestPollerUpgradeWorkFlow_Success() {
	// Arrange
	params := &HarvestPollerUpgradeParams{}
	mockStorage := database.NewMockStorage(s.T())
	harvestRefreshActivity := &activities.HarvestNodesRefreshActivity{SE: mockStorage}
	s.env.RegisterActivity(harvestRefreshActivity)

	nodeGroupsMap := []*datamodel.NodeNodeGroupMap{
		{NodeID: 1, NodeGroupID: 1},
		{NodeID: 2, NodeGroupID: 2},
	}
	// Mock activities
	s.env.OnActivity(harvestRefreshActivity.GetNodeGroupMaps,
		mock.Anything, mock.Anything).Return(nodeGroupsMap, nil)

	s.env.OnActivity(harvestRefreshActivity.RefreshHarvestNodes, mock.Anything, mock.Anything).
		Return(nil)

	// Act
	s.env.ExecuteWorkflow(HarvestPollerUpgradeWorkFlow, params)

	// Assert
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *HarvestUpgradeTestSuite) TestHarvestPollerUpgradeWorkFlow_GetNodeGroupMapsError() {
	// Arrange
	params := &HarvestPollerUpgradeParams{}
	mockStorage := database.NewMockStorage(s.T())
	harvestRefreshActivity := &activities.HarvestNodesRefreshActivity{SE: mockStorage}
	s.env.RegisterActivity(harvestRefreshActivity)

	// Mock activities
	s.env.OnActivity(harvestRefreshActivity.GetNodeGroupMaps,
		mock.Anything, mock.Anything).Return(nil, errors.New("database error"))

	// Mock alert activity for defer
	s.env.OnActivity(harvestRefreshActivity.AlertHarvestRefreshFailure,
		mock.Anything, mock.Anything).Return(nil)

	// Act
	s.env.ExecuteWorkflow(HarvestPollerUpgradeWorkFlow, params)

	// Assert
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
}

func (s *HarvestUpgradeTestSuite) TestHarvestPollerUpgradeWorkFlow_EmptyNodeGroupMaps() {
	// Arrange
	params := &HarvestPollerUpgradeParams{}
	mockStorage := database.NewMockStorage(s.T())
	harvestRefreshActivity := &activities.HarvestNodesRefreshActivity{SE: mockStorage}
	s.env.RegisterActivity(harvestRefreshActivity)

	// Mock activities - return empty slice
	s.env.OnActivity(harvestRefreshActivity.GetNodeGroupMaps,
		mock.Anything, mock.Anything).Return([]*datamodel.NodeNodeGroupMap{}, nil)

	// Act
	s.env.ExecuteWorkflow(HarvestPollerUpgradeWorkFlow, params)

	// Assert
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *HarvestUpgradeTestSuite) TestHarvestPollerUpgradeWorkFlow_RefreshHarvestNodesError() {
	// Arrange
	params := &HarvestPollerUpgradeParams{}
	mockStorage := database.NewMockStorage(s.T())
	harvestRefreshActivity := &activities.HarvestNodesRefreshActivity{SE: mockStorage}
	s.env.RegisterActivity(harvestRefreshActivity)

	nodeGroupsMap := []*datamodel.NodeNodeGroupMap{
		{NodeID: 1, NodeGroupID: 1},
		{NodeID: 2, NodeGroupID: 2},
	}

	// Mock activities
	s.env.OnActivity(harvestRefreshActivity.GetNodeGroupMaps,
		mock.Anything, mock.Anything).Return(nodeGroupsMap, nil)

	s.env.OnActivity(harvestRefreshActivity.RefreshHarvestNodes, mock.Anything, mock.Anything).
		Return(errors.New("refresh failed"))

	// Mock alert activity for defer
	s.env.OnActivity(harvestRefreshActivity.AlertHarvestRefreshFailure,
		mock.Anything, mock.Anything).Return(nil)

	// Act
	s.env.ExecuteWorkflow(HarvestPollerUpgradeWorkFlow, params)

	// Assert
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
}
