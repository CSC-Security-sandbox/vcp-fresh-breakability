package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
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

type FinishProjectEventDeleteStateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *FinishProjectEventDeleteStateTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(FinishProjectEventDeleteStateWorkflow)
}

func (s *FinishProjectEventDeleteStateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)

	// Mock finish project event activity
	finishResult := &commonparams.FinishProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operations/test-operation-123"),
	}
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil).Once()
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	var kmsConfigs []*datamodel.KmsConfig
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return(kmsConfigs, nil)

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_SuccessWhenCVPHostIsEmpty() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_UpdateJobFailsAfterWorkflowExecution() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)

	// Mock finish project event activity
	finishResult := &commonparams.FinishProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operations/test-operation-123"),
	}
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil).Once()
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	var kmsConfigs []*datamodel.KmsConfig
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return(kmsConfigs, nil)

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed but with an update job error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_FirstUpdateJobFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)

	// Mock activities
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch job details from SDE"))

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 10)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_FinishProjectEventForSDEActivityFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Updated: Expect "DONE" for both calls since the defer always sets DONE
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "DONE", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)

	// Mock finish project event activity to fail
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(nil, errors.New("failed to start SDE Activity"))

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWorkflow_PollFinishProjectEventSDEOperationActivityFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	// Updated: Expect "DONE" instead of "ERROR" for the second call
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "DONE", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)

	// Mock finish project event activity
	finishResult := &commonparams.FinishProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operations/test-operation-123"),
	}
	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil).Once()
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to Poll SDE Activity"))

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWhenNoKMS() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	mockStorage.EXPECT().UpdateJob(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock finish project event activity
	finishResult := &commonparams.FinishProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operations/test-operation-123"),
	}

	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil).Once()
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Create empty KmsConfig list
	kmsConfigs := []*datamodel.KmsConfig{}
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return(kmsConfigs, nil).Once()

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify DeleteKmsConfig was NOT called since no KMS configs exist
	s.env.AssertNotCalled(s.T(), "DeleteKmsConfig")
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWhenOneKMSExists() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	mockStorage.EXPECT().UpdateJob(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock finish project event activity
	finishResult := &commonparams.FinishProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operations/test-operation-123"),
	}

	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil).Once()
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Create KmsConfig list with one config
	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{
			UUID: "kms-config-123",
		},
		CustomerProjectID: "test-project-number",
		Name:              "test-kms-config",
		State:             "ACTIVE",
	}
	kmsConfigs := []*datamodel.KmsConfig{kmsConfig}
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return(kmsConfigs, nil)

	// Mock DeleteKmsConfig activity to be called once
	deleteParams := &commonparams.DeleteKmsConfigParams{KmsConfigID: kmsConfig.UUID}
	s.env.OnActivity(kmsActivities.DeleteKmsConfig, mock.Anything, kmsConfig, deleteParams).Return(nil).Once()

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify DeleteKmsConfig was called exactly once with correct parameters
	s.env.AssertCalled(s.T(), "DeleteKmsConfig", mock.Anything, mock.Anything, mock.Anything)
	s.env.AssertNumberOfCalls(s.T(), "DeleteKmsConfig", 1)
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWhenKMSConfigIsNil() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	mockStorage.EXPECT().UpdateJob(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock finish project event activity
	finishResult := &commonparams.FinishProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operations/test-operation-123"),
	}

	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil).Once()
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Create KmsConfig list with one nil config
	kmsConfigs := []*datamodel.KmsConfig{nil}
	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return(kmsConfigs, nil).Once()

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify DeleteKmsConfig was NOT called since KMS config is nil
	s.env.AssertNotCalled(s.T(), "DeleteKmsConfig")
}

func (s *FinishProjectEventDeleteStateTestSuite) Test_FinishProjectEventDeleteStateWhenDeleteKMSFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{SE: mockStorage}
	kmsActivities := &kms_activities.KmsConfigActivity{SE: mockStorage}
	cvp.CVP_HOST = "someHost"
	defer func() {
		cvp.CVP_HOST = ""
	}()

	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity)
	s.env.RegisterActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity)
	s.env.RegisterActivity(kmsActivities.ListKmsConfigActivity)
	s.env.RegisterActivity(kmsActivities.DeleteKmsConfig)

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock finish project event activity
	finishResult := &commonparams.FinishProjectEventResult{
		Done: nillable.GetBoolPtr(false),
		Name: nillable.GetStringPtr("operations/test-operation-123"),
	}

	s.env.OnActivity(finishProjectEventActivity.FinishProjectEventForSDEActivity, mock.Anything, mock.Anything).Return(finishResult, nil).Once()
	s.env.OnActivity(finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Create KmsConfig list with one config
	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{
			UUID: "kms-config-123",
		},
		CustomerProjectID: "test-project-number",
		Name:              "test-kms-config",
		State:             "ACTIVE",
	}
	kmsConfigs := []*datamodel.KmsConfig{kmsConfig}

	s.env.OnActivity(kmsActivities.ListKmsConfigActivity, mock.Anything, mock.Anything).Return(kmsConfigs, nil).Once()

	// Mock DeleteKmsConfig activity to fail
	deleteParams := &commonparams.DeleteKmsConfigParams{KmsConfigID: kmsConfig.UUID}
	s.env.OnActivity(kmsActivities.DeleteKmsConfig, mock.Anything, kmsConfig, deleteParams).Return(vsaerrors.WrapAsNonRetryableTemporalApplicationError(errors.New("failed to delete KMS config")))

	// Execute workflow
	params := &commonparams.FinishProjectEventParams{
		State:          models.StateDelete,
		LocationId:     "test-location-id",
		ProjectNumber:  "test-project-number",
		XCorrelationID: "test-correlation-id",
	}
	s.env.ExecuteWorkflow(FinishProjectEventDeleteStateWorkflow, params)

	// Assert workflow failed due to KMS deletion error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to delete KMS config")

	// Verify DeleteKmsConfig was called exactly once
	s.env.AssertCalled(s.T(), "DeleteKmsConfig", mock.Anything, mock.Anything, mock.Anything)
	s.env.AssertNumberOfCalls(s.T(), "DeleteKmsConfig", 10)
	s.env.AssertExpectations(s.T())
}

func TestFinishProjectEventDeleteStateWorkflow(t *testing.T) {
	suite.Run(t, new(FinishProjectEventDeleteStateTestSuite))
}
