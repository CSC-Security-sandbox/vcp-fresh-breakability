package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateSnapshotActivity := activities.SnapshotUpdateActivity{SE: mockStorage}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: int64(1)},
		Account:   &datamodel.Account{Name: "test-account"},
		Volume:    &datamodel.Volume{Name: "test-volume"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateSnapshotActivity.UpdateSnapshot)

	// Mock UpdateJob method calls
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", "PROCESSING", 0, "").Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", "DONE", 0, "").Return(nil)

	// Mock UpdateSnapshot activity
	s.env.OnActivity(updateSnapshotActivity.UpdateSnapshot, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateSnapshotWorkflow, snapshot)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *updateSnapshotTestSuite) Test_UpdateWorkflow_UpdateJobStatus_Error() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateSnapshotActivity := activities.SnapshotUpdateActivity{SE: mockStorage}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: int64(1)},
		Account:   &datamodel.Account{Name: "test-account"},
		Volume:    &datamodel.Volume{Name: "test-volume"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateSnapshotActivity.UpdateSnapshot)

	// Mock UpdateJob to return error for PROCESSING state
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", "PROCESSING", 0, "").Return(assert.AnError)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateSnapshotWorkflow, snapshot)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *updateSnapshotTestSuite) Test_UpdateWorkflow_UpdateJobStatus_Setup_Error() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateSnapshotActivity := activities.SnapshotUpdateActivity{SE: mockStorage}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: int64(1)},
		Account:   &datamodel.Account{Name: ""}, // Empty account name to trigger setup error
		Volume:    &datamodel.Volume{Name: "test-volume"},
	}

	// Mock UpdateJob method calls
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", "PROCESSING", 0, "").Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", "ERROR", 0, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateSnapshotActivity.UpdateSnapshot)

	// Mock UpdateSnapshot activity to return error
	s.env.OnActivity(updateSnapshotActivity.UpdateSnapshot, mock.Anything, mock.Anything).Return(assert.AnError)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateSnapshotWorkflow, snapshot)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *updateSnapshotTestSuite) Test_UpdateWorkflow_UpdateJobStatus_Processing_Error() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateSnapshotActivity := activities.SnapshotUpdateActivity{SE: mockStorage}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: int64(1)},
		Account:   &datamodel.Account{Name: "test-account"},
		Volume:    &datamodel.Volume{Name: "test-volume"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateSnapshotActivity.UpdateSnapshot)

	// Mock UpdateJob to return error for PROCESSING state
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", "PROCESSING", 0, "").Return(assert.AnError)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateSnapshotWorkflow, snapshot)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *updateSnapshotTestSuite) Test_UpdateWorkflow_CompletesDespiteFinalJobStatusUpdateError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateSnapshotActivity := activities.SnapshotUpdateActivity{SE: mockStorage}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: int64(1)},
		Account:   &datamodel.Account{Name: "test-account"},
		Volume:    &datamodel.Volume{Name: "test-volume"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateSnapshotActivity.UpdateSnapshot)

	// Mock UpdateJob method calls
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", "PROCESSING", 0, "").Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", "DONE", 0, "").Return(assert.AnError)

	// Mock UpdateSnapshot activity
	s.env.OnActivity(updateSnapshotActivity.UpdateSnapshot, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateSnapshotWorkflow, snapshot)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *updateSnapshotTestSuite) Test_UpdateWorkflow_UpdateSnapshot_Error() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateSnapshotActivity := activities.SnapshotUpdateActivity{SE: mockStorage}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: int64(1)},
		Account:   &datamodel.Account{Name: "test-account"},
		Volume:    &datamodel.Volume{Name: "test-volume"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateSnapshotActivity.UpdateSnapshot)

	// Mock UpdateJob method calls
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", "PROCESSING", 0, "").Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", "ERROR", 0, mock.Anything).Return(nil)

	// Mock UpdateSnapshot to return error
	s.env.OnActivity(updateSnapshotActivity.UpdateSnapshot, mock.Anything, mock.Anything).Return(assert.AnError)

	// Execute workflow
	s.env.ExecuteWorkflow(UpdateSnapshotWorkflow, snapshot)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func TestUpdateSnapshotTestSuite(t *testing.T) {
	suite.Run(t, new(updateSnapshotTestSuite))
}
