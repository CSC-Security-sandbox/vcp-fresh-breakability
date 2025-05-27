package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type SnapshotUnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *SnapshotUnitTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(CreateSnapshotWorkflow)
}

func (s *SnapshotUnitTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *SnapshotUnitTestSuite) TestCreateSnapshotWorkflowWorkflowExecutesSuccessfully() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	snapshotCreateActivity := activities.SnapshotCreateActivity{SE: mockStorage}

	params := &common.CreateSnapshotParams{
		AccountName: "test-account",
	}
	snapshot := &datamodel.Snapshot{
		Name: "test-snapshot",
		Volume: &datamodel.Volume{
			PoolID: 1,
			Pool: &datamodel.Pool{
				Username: "test-user",
				Password: "test-password",
			},
		},
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			SizeInBytes:            0,
			LogicalSizeUsedInBytes: 0,
		},
	}

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&snapshotCreateActivity)

	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(&datamodel.Node{EndpointAddress: "127.0.0.1"}, nil)
	s.env.OnActivity(snapshotCreateActivity.CreateSnapshotInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.SnapshotProviderResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "snapshot-uuid"}, SizeInBytes: 1024, LogicalSizeInBytes: 1024}, nil)
	s.env.OnActivity(snapshotCreateActivity.UpdateSnapshotDetails, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateSnapshotWorkflow, params, snapshot)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *SnapshotUnitTestSuite) TestCreateSnapshotWorkflowFailsOnActivityError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	snapshotCreateActivity := activities.SnapshotCreateActivity{SE: mockStorage}

	params := &common.CreateSnapshotParams{
		AccountName: "test-account",
	}
	snapshot := &datamodel.Snapshot{
		Name: "test-snapshot",
		Volume: &datamodel.Volume{
			PoolID: 1,
			Pool: &datamodel.Pool{
				Username: "test-user",
				Password: "test-password",
			},
		},
	}

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&snapshotCreateActivity)

	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(&datamodel.Node{EndpointAddress: "127.0.0.1"}, nil)
	s.env.OnActivity(snapshotCreateActivity.CreateSnapshotInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("snapshot creation failed"))
	s.env.OnActivity(snapshotCreateActivity.UpdateSnapshotDetails, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateSnapshotWorkflow, params, snapshot)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "snapshot creation failed")
	s.env.AssertExpectations(s.T())
}

func (s *SnapshotUnitTestSuite) TestSnapshotCreateWorkflowRollbackOnFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	snapshotCreateActivity := activities.SnapshotCreateActivity{SE: mockStorage}

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&snapshotCreateActivity)

	params := &common.CreateSnapshotParams{
		AccountName: "test-account",
	}
	snapshot := &datamodel.Snapshot{
		Name: "test-snapshot",
		Volume: &datamodel.Volume{
			PoolID: 1,
			Pool: &datamodel.Pool{
				Username: "test-user",
				Password: "test-password",
			},
		},
	}

	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(&datamodel.Node{EndpointAddress: "127.0.0.1"}, nil)
	s.env.OnActivity(snapshotCreateActivity.CreateSnapshotInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("snapshot creation failed"))
	s.env.OnActivity(snapshotCreateActivity.UpdateSnapshotDetails, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateSnapshotWorkflow, params, snapshot)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "snapshot creation failed")
	s.env.AssertExpectations(s.T())
}

func TestSnapshotUnitTestSuite(t *testing.T) {
	suite.Run(t, new(SnapshotUnitTestSuite))
}
