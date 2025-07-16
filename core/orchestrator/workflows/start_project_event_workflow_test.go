package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/resource_events_activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type StartProjectEventOffStateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *StartProjectEventOffStateTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(StartProjectEventOffStateWorkflow)
}

func (s *StartProjectEventOffStateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_SuccessWhenCVPHostIsEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOff,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_UpdateJobFailsAfterWorkflowExecution() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)

	// Mock activities
	result := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_FirstUpdateJobFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 10)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_StartProjectEventForSDEActivityFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to start SDE Activity"), nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOffStateTestSuite) Test_StartProjectEventOffStateWorkflow_PollStartProjectEventSDEOperationActivity() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)

	// Mock activities
	result := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to Poll SDE Activity"))

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOffStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func TestStartProjectEventOffStateWorkflow(t *testing.T) {
	suite.Run(t, new(StartProjectEventOffStateTestSuite))
}

type StartProjectEventOnStateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *StartProjectEventOnStateTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(StartProjectEventOnStateWorkflow)
}

func (s *StartProjectEventOnStateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_SuccessWhenCVPHostIsEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_UpdateJobFailsAfterWorkflowExecution() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)

	// Mock activities
	result := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_FirstUpdateJobFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 10)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_StartProjectEventForSDEActivityFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to start SDE Activity"), nil)

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *StartProjectEventOnStateTestSuite) Test_StartProjectEventOnStateWorkflow_PollStartProjectEventSDEOperationActivity() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(startProjectEventActivity.StartProjectEventForSDEActivity)
	s.env.RegisterActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity)

	// Mock activities
	result := &commonparams.StartProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(startProjectEventActivity.StartProjectEventForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(startProjectEventActivity.PollStartProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to Poll SDE Activity"))

	// Execute workflow
	params := &commonparams.StartProjectEventParams{
		LocationID:    "locationID",
		ProjectNumber: "ProjectNumber",
		State:         models.StateOn,
	}
	s.env.ExecuteWorkflow(StartProjectEventOnStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func TestStartProjectEventOnStateWorkflow(t *testing.T) {
	suite.Run(t, new(StartProjectEventOnStateTestSuite))
}
