package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/resource_events_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type HandleResourceEventOnStateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *HandleResourceEventOnStateTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(UpdateResourceStateONWorkflow)
}

func (s *HandleResourceEventOnStateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *HandleResourceEventOnStateTestSuite) Test_UpdateResourceStateONWorkflow_WithPollingSuccess() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	result := &common.HandleResourceEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOn,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateONWorkflow, param)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOnStateTestSuite) Test_UpdateResourceStateONWorkflow_InvalidResourceType() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(true, nil)

	// Execute workflow with invalid resource type
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOn,
		ResourceId:     "resource-id",
		ResourceType:   "invalid-resource-type",
	}
	s.env.ExecuteWorkflow(UpdateResourceStateONWorkflow, param)

	// Assert workflow completed with error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOnStateTestSuite) Test_UpdateResourceStateONWorkflow_EmptyResourceId() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Execute workflow with empty resource ID
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOn,
		ResourceId:     "",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateONWorkflow, param)

	// Assert workflow completed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOnStateTestSuite) Test_UpdateResourceStateONWorkflow_NotImplemented() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(true, nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOn,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateONWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOnStateTestSuite) Test_UpdateResourceStateONWorkflow_SuccessWhenCVPHostIsEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOn,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateONWorkflow, param)
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOnStateTestSuite) Test_UpdateResourceStateONWorkflow_UpdateJobFailsAfterWorkflowExecution() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	result := &common.HandleResourceEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOn,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateONWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOnStateTestSuite) Test_UpdateResourceStateONWorkflow_HandleResourceEventForSDEActivityFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to start SDE Activity"), nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOn,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateONWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOnStateTestSuite) Test_UpdateResourceStateONWorkflow_WhenResourceIsNotFoundInVCPAndIsSDEResource() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, errors.NewNotFoundErr("volume", nil))
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOn,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateONWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func TestHandleResourceEventOnStateWorkflow(t *testing.T) {
	suite.Run(t, new(HandleResourceEventOnStateTestSuite))
}

type HandleResourceEventOffStateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *HandleResourceEventOffStateTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(UpdateResourceStateOFFWorkflow)
}

func (s *HandleResourceEventOffStateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *HandleResourceEventOffStateTestSuite) Test_UpdateResourceStateOFFWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(true, nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOffStateTestSuite) Test_UpdateResourceStateOffWorkflow_NotImplemented() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(true, nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOffStateTestSuite) Test_UpdateResourceStateOffWorkflow_SuccessWhenCVPHostIsEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateOFFWorkflow, param)
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOffStateTestSuite) Test_UpdateResourceStateOffWorkflow_UpdateJobFailsAfterWorkflowExecution() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	result := &common.HandleResourceEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOffStateTestSuite) Test_UpdateResourceStateOFFWorkflow_WithPollingSuccess() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	result := &common.HandleResourceEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateOFFWorkflow, param)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOffStateTestSuite) Test_UpdateResourceStateOffWorkflow_HandleResourceEventForSDEActivityFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to start SDE Activity"), nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventOffStateTestSuite) Test_UpdateResourceStateOffWorkflow_WhenResourceIsNotFoundInVCPAndIsSDEResource() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, errors.NewNotFoundErr("volume", nil))
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func TestHandleResourceEventOffStateWorkflow(t *testing.T) {
	suite.Run(t, new(HandleResourceEventOffStateTestSuite))
}

type HandleResourceEventCommonResourceOffStateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *HandleResourceEventCommonResourceOffStateTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(UpdateResourceStateCommonResourceOFFWorkflow)
}

func (s *HandleResourceEventCommonResourceOffStateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *HandleResourceEventCommonResourceOffStateTestSuite) Test_UpdateResourceStateCommonResourceOFFWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeKmsConfig,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateCommonResourceOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventCommonResourceOffStateTestSuite) Test_UpdateResourceStateCommonResourceOffWorkflow_SuccessWhenCVPHostIsEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateCommonResourceOFFWorkflow, param)
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventCommonResourceOffStateTestSuite) Test_UpdateResourceStateCommonResourceOffWorkflow_UpdateJobFailsAfterWorkflowExecution() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	result := &common.HandleResourceEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateCommonResourceOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventCommonResourceOffStateTestSuite) Test_UpdateResourceStateCommonResourceOffWorkflow_HandleResourceEventForSDEActivityFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to start SDE Activity"), nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateCommonResourceOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventCommonResourceOffStateTestSuite) Test_UpdateResourceStateCommonResourceOffWorkflow_WhenResourceIsNotFoundInVCPAndIsSDEResource() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, errors.NewNotFoundErr("volume", nil))
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateCommonResourceOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func TestHandleResourceEventCommonOffStateWorkflow(t *testing.T) {
	suite.Run(t, new(HandleResourceEventCommonResourceOffStateTestSuite))
}

type HandleResourceEventCommonResourceOnStateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *HandleResourceEventCommonResourceOnStateTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(UpdateResourceStateCommonResourceONWorkflow)
}

func (s *HandleResourceEventCommonResourceOnStateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *HandleResourceEventCommonResourceOnStateTestSuite) Test_UpdateResourceStateCommonResourceONWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeKmsConfig,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateCommonResourceOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventCommonResourceOnStateTestSuite) Test_UpdateResourceStateCommonResourceOnWorkflow_SuccessWhenCVPHostIsEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateCommonResourceOFFWorkflow, param)
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventCommonResourceOnStateTestSuite) Test_UpdateResourceStateCommonResourceOnWorkflow_UpdateJobFailsAfterWorkflowExecution() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	result := &common.HandleResourceEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateCommonResourceOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventCommonResourceOnStateTestSuite) Test_UpdateResourceStateCommonResourceOnWorkflow_HandleResourceEventForSDEActivityFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to start SDE Activity"), nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateCommonResourceOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventCommonResourceOnStateTestSuite) Test_UpdateResourceStateCommonResourceONWorkflow_PartiallyCompletedOperation() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities - operation already partially completed
	result := &common.HandleResourceEventResult{
		Done: nillable.GetBoolPtr(true),
		Name: nillable.GetStringPtr("operationID"),
	}
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOn,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeKmsConfig,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateCommonResourceONWorkflow, param)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *HandleResourceEventCommonResourceOnStateTestSuite) Test_UpdateResourceStateCommonResourceOnWorkflow_WhenResourceIsNotFoundInVCPAndIsSDEResource() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	hreActivity := resource_events_activities.ResourceEventsActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(hreActivity.HandleResourceEventCheckForVCPActivity)
	s.env.RegisterActivity(hreActivity.HandleResourceEventsForSDEActivity)
	s.env.RegisterActivity(hreActivity.PollHandleResourceEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(hreActivity.HandleResourceEventCheckForVCPActivity, mock.Anything, mock.Anything).Return(false, errors.NewNotFoundErr("volume", nil))
	s.env.OnActivity(hreActivity.HandleResourceEventsForSDEActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(hreActivity.PollHandleResourceEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	param := &common.UpdateResourceStateParams{
		ProjectNumber:  "123456789",
		XCorrelationID: "test-correlation-id",
		LocationId:     "us-central1",
		State:          models.StateOff,
		ResourceId:     "resource-id",
		ResourceType:   common.ResourceStateV1ResourceTypeVolume,
	}
	s.env.ExecuteWorkflow(UpdateResourceStateCommonResourceOFFWorkflow, param)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		s.T().Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func TestHandleResourceEventCommonOnStateWorkflow(t *testing.T) {
	suite.Run(t, new(HandleResourceEventCommonResourceOnStateTestSuite))
}
