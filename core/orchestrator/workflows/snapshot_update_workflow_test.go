package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type updateSnapshotTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *updateSnapshotTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(UpdateSnapshotWorkflow)
}

func (s *updateSnapshotTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *updateSnapshotTestSuite) Test_UpdateWorkflow_Success() {
	commonActivity := activities.CommonActivities{}
	updateSnapshotActivity := activities.SnapshotUpdateActivity{}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: int64(1)},
		Account:   &datamodel.Account{Name: "test-account"},
		Volume:    &datamodel.Volume{Name: "test-volume"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateSnapshotActivity.UpdateSnapshot)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateSnapshotActivity.UpdateSnapshot, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateSnapshotWorkflow, snapshot)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *updateSnapshotTestSuite) Test_UpdateWorkflow_UpdateJobStatus_Error() {
	commonActivity := activities.CommonActivities{}
	updateSnapshotActivity := activities.SnapshotUpdateActivity{}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: int64(1)},
		Account:   &datamodel.Account{Name: "test-account"},
		Volume:    &datamodel.Volume{Name: "test-volume"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateSnapshotActivity.UpdateSnapshot)

	// Mock UpdateJobStatus to return error
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateSnapshotWorkflow, snapshot)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *updateSnapshotTestSuite) Test_UpdateWorkflow_UpdateJobStatus_Setup_Error() {
	commonActivity := activities.CommonActivities{}
	updateSnapshotActivity := activities.SnapshotUpdateActivity{}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: int64(1)},
		Volume:    &datamodel.Volume{Name: "test-volume"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateSnapshotActivity.UpdateSnapshot)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateSnapshotWorkflow, snapshot)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *updateSnapshotTestSuite) Test_UpdateWorkflow_UpdateSnapshot_Error() {
	commonActivity := activities.CommonActivities{}
	updateSnapshotActivity := activities.SnapshotUpdateActivity{}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: int64(1)},
		Account:   &datamodel.Account{Name: "test-account"},
		Volume:    &datamodel.Volume{Name: "test-volume"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateSnapshotActivity.UpdateSnapshot)

	// Mock UpdateJobStatus to return nil, UpdateSnapshot to return error
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateSnapshotActivity.UpdateSnapshot, mock.Anything, mock.Anything).Return(assert.AnError)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateSnapshotWorkflow, snapshot)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func TestUpdateSnapshotTestSuite(t *testing.T) {
	suite.Run(t, new(updateSnapshotTestSuite))
}
