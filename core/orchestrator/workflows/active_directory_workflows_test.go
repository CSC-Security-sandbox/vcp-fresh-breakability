package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const cvpHost = "sde.example.com"

func TestCreateActiveDirectoryWorkflow(t *testing.T) {
	t.Run("VcpSuccess", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryCreateActivity{})

		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &common.CreateActiveDirectoryParams{
			AccountId:          "123",
			ResourceId:         "test-ad",
			Domain:             "test.example.com",
			DNS:                "8.8.8.8",
			NetBIOS:            "TESTNETBIOS",
			OrganizationalUnit: "OU=test",
			Username:           "admin",
			Password:           "password123",
		}
		adRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid-123",
			},
			AccountId: 123,
		}

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		env.OnActivity("CreateVcpActiveDirectory", mock.Anything, adRecord).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, params, adRecord)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("SdeSuccess", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryCreateActivity{})

		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &common.CreateActiveDirectoryParams{
			AccountId:          "123",
			ResourceId:         "test-ad",
			Domain:             "test.example.com",
			DNS:                "8.8.8.8",
			NetBIOS:            "TESTNETBIOS",
			OrganizationalUnit: "OU=test",
			Username:           "admin",
			Password:           "password123",
		}
		adRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid-456",
			},
			AccountId: 456,
		}

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		env.OnActivity("CreateSdeActiveDirectory", mock.Anything, params).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, params, adRecord)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("VcpActivityFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryCreateActivity{})

		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &common.CreateActiveDirectoryParams{
			AccountId:          "123",
			ResourceId:         "test-ad",
			Domain:             "test.example.com",
			DNS:                "8.8.8.8",
			NetBIOS:            "TESTNETBIOS",
			OrganizationalUnit: "OU=test",
			Username:           "admin",
			Password:           "password123",
		}

		adRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid-fail",
			},
			AccountId: 789,
		}

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		expectedError := vsaerrors.New("Error")
		env.OnActivity("CreateVcpActiveDirectory", mock.Anything, adRecord).Return(expectedError)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, params, adRecord)
	})

	t.Run("SdeActivityFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryCreateActivity{})

		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &common.CreateActiveDirectoryParams{
			AccountId:          "123",
			ResourceId:         "test-ad",
			Domain:             "test.example.com",
			DNS:                "8.8.8.8",
			NetBIOS:            "TESTNETBIOS",
			OrganizationalUnit: "OU=test",
			Username:           "admin",
			Password:           "password123",
		}
		adRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid-sde-fail",
			},
			AccountId: 999,
		}

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		expectedError := vsaerrors.New("Error")
		env.OnActivity("CreateSdeActiveDirectory", mock.Anything, params).Return(expectedError)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, params, adRecord)
	})

	t.Run("SetupFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		// Pass invalid params to cause setup failure
		invalidParams := &common.CreateActiveDirectoryParams{}
		adRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AccountId: 123,
		}

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, invalidParams, adRecord)
	})

	t.Run("UpdateJobStatusFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryCreateActivity{})

		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &common.CreateActiveDirectoryParams{
			AccountId:  "123",
			ResourceId: "test-ad",
		}
		adRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AccountId: 123,
		}

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.New("job status update failed"))

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, params, adRecord)
	})
}

func TestActiveDirectoryCreateWorkflow_Setup(t *testing.T) {
	t.Run("SuccessfulSetup", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		params := &common.CreateActiveDirectoryParams{
			AccountId:  "123",
			ResourceId: "test-ad",
		}

		var setupErr error
		var wf *ActiveDirectoryCreateWorkflow
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf = &ActiveDirectoryCreateWorkflow{}
			setupErr = wf.Setup(ctx, params)
			return setupErr
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf = &ActiveDirectoryCreateWorkflow{}
			setupErr = wf.Setup(ctx, params)
			return setupErr
		})

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, setupErr)
		assert.NoError(t, env.GetWorkflowError())
		assert.Equal(t, WorkflowStatusCreated, wf.Status)
		assert.Equal(t, "123", wf.CustomerID)
		assert.Equal(t, "default-test-workflow-id", wf.ID)
	})

	t.Run("QueryHandlerTest", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		params := &common.CreateActiveDirectoryParams{
			AccountId:  "456",
			ResourceId: "test-ad-query",
		}

		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			err := wf.Setup(ctx, params)
			if err != nil {
				return err
			}
			wf.Status = WorkflowStatusRunning
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			err := wf.Setup(ctx, params)
			if err != nil {
				return err
			}
			wf.Status = WorkflowStatusRunning
			return nil
		})

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())

		queryResult, err := env.QueryWorkflow("status")
		assert.NoError(t, err)
		assert.NotNil(t, queryResult)

		var status WorkflowStatus
		err = queryResult.Get(&status)
		assert.NoError(t, err)
		assert.Equal(t, "456", status.CustomerID)
		assert.Equal(t, "default-test-workflow-id", status.ID)
	})
}

func TestActiveDirectoryCreateWorkflow_Run(t *testing.T) {
	t.Run("SuccessfulVcpRun", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryCreateActivity{})

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		params := &common.CreateActiveDirectoryParams{
			AccountId:  "123",
			ResourceId: "test-ad",
		}
		adRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			AccountId: 123,
		}

		env.OnActivity("CreateVcpActiveDirectory", mock.Anything, adRecord).Return(nil)

		var runResult interface{}
		var runErr *vsaerrors.CustomError
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		assert.Nil(t, runErr)
		assert.Nil(t, runResult)
		env.AssertExpectations(t)
	})

	t.Run("SuccessfulSdeRun", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryCreateActivity{})

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		params := &common.CreateActiveDirectoryParams{
			AccountId:  "456",
			ResourceId: "test-ad-sde",
		}
		adRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid-sde",
			},
			AccountId: 456,
		}

		env.OnActivity("CreateSdeActiveDirectory", mock.Anything, params).Return(nil)

		var runResult interface{}
		var runErr *vsaerrors.CustomError
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		assert.Nil(t, runErr)
		assert.Nil(t, runResult)
		env.AssertExpectations(t)
	})

	t.Run("ActivityExecutionFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryCreateActivity{})

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		params := &common.CreateActiveDirectoryParams{
			AccountId:  "789",
			ResourceId: "test-ad-activity-fail",
		}
		adRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid-activity-fail",
			},
			AccountId: 789,
		}

		activityError := vsaerrors.New("activity execution failed")
		env.OnActivity("CreateVcpActiveDirectory", mock.Anything, adRecord).Return(activityError)

		var runResult interface{}
		var runErr *vsaerrors.CustomError
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		assert.NotNil(t, runErr)
		assert.Nil(t, runResult)
		env.AssertExpectations(t)
	})
}

func TestUpdateActiveDirectoryWorkflow(t *testing.T) {
	t.Run("VcpSuccess", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryUpdateActivity{})

		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "123",
			Domain:    &[]string{"updated.example.com"}[0],
			DNS:       &[]string{"8.8.4.4"}[0],
		}
		adRecord := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				ID:   1,
				UUID: "test-ad-uuid-update-123",
			},
			State: "available",
		}

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		env.OnActivity("PushUpdatesDownstreamActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVcpActiveDirectory", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(UpdateActiveDirectoryWorkflow, params, adRecord)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("SdeSuccess", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryUpdateActivity{})

		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "456",
			Domain:    &[]string{"sde-updated.example.com"}[0],
		}
		adRecord := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				ID:   2,
				UUID: "test-ad-uuid-update-sde-456",
			},
			State: "available",
		}

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		sdeResult := &cvpModels.OperationV1beta{
			Name: "update-ad-operation",
		}

		env.OnActivity("UpdateSdeActiveDirectory", mock.Anything, params).Return(sdeResult, nil)
		env.OnActivity("PollSdeUpdateActivity", mock.Anything, params, sdeResult).Return(nil)
		env.OnActivity("MarkVcpAdToUpdatingActivity", mock.Anything, params, adRecord).Return(nil)
		env.OnActivity("PushUpdatesDownstreamActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVcpActiveDirectory", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(UpdateActiveDirectoryWorkflow, params, adRecord)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("SdeUpdateFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryUpdateActivity{})

		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "789",
		}
		adRecord := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				ID:   3,
				UUID: "test-ad-uuid-update-fail-789",
			},
			State: "available",
		}

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		expectedError := vsaerrors.New("SDE update failed")
		env.OnActivity("UpdateSdeActiveDirectory", mock.Anything, params, adRecord).Return(expectedError)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(UpdateActiveDirectoryWorkflow, params, adRecord)
	})

	t.Run("SdePollingFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryUpdateActivity{})

		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "999",
		}
		adRecord := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				ID:   4,
				UUID: "test-ad-uuid-poll-fail-999",
			},
			State: "available",
		}

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		sdeResult := &cvpModels.OperationV1beta{
			Name: "update-ad-operation-fail",
		}
		expectedError := vsaerrors.New("Polling failed")

		env.OnActivity("UpdateSdeActiveDirectory", mock.Anything, params, adRecord).Return(sdeResult)
		env.OnActivity("PollSdeUpdateActivity", mock.Anything, params, sdeResult).Return(expectedError)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(UpdateActiveDirectoryWorkflow, params, adRecord)
	})

	t.Run("VcpUpdateFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryUpdateActivity{})

		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "111",
		}
		adRecord := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				ID:   5,
				UUID: "test-ad-uuid-vcp-fail-111",
			},
			State: "available",
		}

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		expectedError := vsaerrors.New("VCP update failed")
		env.OnActivity("PushUpdatesDownstreamActivity", mock.Anything, mock.Anything, mock.Anything).Return(expectedError)
		env.OnActivity("MarkVcpAdToErrorActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(UpdateActiveDirectoryWorkflow, params, adRecord)
	})

	t.Run("SetupFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		// Pass invalid params to cause setup failure
		invalidParams := &common.UpdateActiveDirectoryParams{}
		adRecord := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				ID:   6,
				UUID: "test-ad-uuid-setup-fail",
			},
			State: "available",
		}

		env.ExecuteWorkflow(UpdateActiveDirectoryWorkflow, invalidParams, adRecord)
	})

	t.Run("UpdateJobStatusFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryUpdateActivity{})

		commonActivity := activities.CommonActivities{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "222",
		}
		adRecord := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				ID:   7,
				UUID: "test-ad-uuid-job-fail",
			},
			State: "available",
		}

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.New("job status update failed"))

		env.ExecuteWorkflow(UpdateActiveDirectoryWorkflow, params, adRecord)
	})
}

func TestActiveDirectoryUpdateWorkflow_Setup(t *testing.T) {
	t.Run("SuccessfulSetup", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "123",
		}

		var setupErr error
		var wf *ActiveDirectoryUpdateWorkflow
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf = &ActiveDirectoryUpdateWorkflow{}
			setupErr = wf.Setup(ctx, params)
			return setupErr
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf = &ActiveDirectoryUpdateWorkflow{}
			setupErr = wf.Setup(ctx, params)
			return setupErr
		})

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, setupErr)
		assert.NoError(t, env.GetWorkflowError())
		assert.Equal(t, WorkflowStatusCreated, wf.Status)
		assert.Equal(t, "123", wf.CustomerID)
		assert.Equal(t, "default-test-workflow-id", wf.ID)
	})

	t.Run("QueryHandlerTest", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "456",
		}

		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryUpdateWorkflow{}
			err := wf.Setup(ctx, params)
			if err != nil {
				return err
			}
			wf.Status = WorkflowStatusRunning
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryUpdateWorkflow{}
			err := wf.Setup(ctx, params)
			if err != nil {
				return err
			}
			wf.Status = WorkflowStatusRunning
			return nil
		})

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())

		queryResult, err := env.QueryWorkflow("status")
		assert.NoError(t, err)
		assert.NotNil(t, queryResult)

		var status WorkflowStatus
		err = queryResult.Get(&status)
		assert.NoError(t, err)
		assert.Equal(t, "456", status.CustomerID)
		assert.Equal(t, "default-test-workflow-id", status.ID)
	})
}

func TestActiveDirectoryUpdateWorkflow_Run(t *testing.T) {
	t.Run("SuccessfulVcpRun", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryUpdateActivity{})

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "123",
		}
		adRecord := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				ID:   1,
				UUID: "test-uuid-run",
			},
			State: "available",
		}

		env.OnActivity("PushUpdatesDownstreamActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVcpActiveDirectory", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		var runResult interface{}
		var runErr *vsaerrors.CustomError
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryUpdateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryUpdateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		assert.Nil(t, runErr)
		assert.Nil(t, runResult)
		env.AssertExpectations(t)
	})

	t.Run("SuccessfulSdeRun", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryUpdateActivity{})

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "456",
		}
		adRecord := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				ID:   2,
				UUID: "test-uuid-sde-run",
			},
			State: "available",
		}

		sdeResult := &cvpModels.OperationV1beta{
			Name: "update-ad-sde-operation",
		}

		env.OnActivity("UpdateSdeActiveDirectory", mock.Anything, params).Return(sdeResult, nil)
		env.OnActivity("PollSdeUpdateActivity", mock.Anything, params, sdeResult).Return(nil)
		env.OnActivity("MarkVcpAdToUpdatingActivity", mock.Anything, params, adRecord).Return(nil)
		env.OnActivity("PushUpdatesDownstreamActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVcpActiveDirectory", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		var runResult interface{}
		var runErr *vsaerrors.CustomError
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryUpdateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryUpdateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		assert.Nil(t, runErr)
		assert.Nil(t, runResult)
		env.AssertExpectations(t)
	})

	t.Run("VcpUpdateExecutionFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryUpdateActivity{})

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "789",
		}
		adRecord := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				ID:   3,
				UUID: "test-uuid-vcp-run-fail",
			},
			State: "available",
		}

		activityError := vsaerrors.New("VCP update execution failed")
		env.OnActivity("PushUpdatesDownstreamActivity", mock.Anything, mock.Anything, mock.Anything).Return(activityError)
		env.OnActivity("MarkVcpAdToErrorActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		var runResult interface{}
		var runErr *vsaerrors.CustomError
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryUpdateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryUpdateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		assert.NotNil(t, runErr)
		assert.Nil(t, runResult)
		env.AssertExpectations(t)
	})

	t.Run("SdeResultNilFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryUpdateActivity{})

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		params := &common.UpdateActiveDirectoryParams{
			AccountId: "999",
		}
		adRecord := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				ID:   4,
				UUID: "test-uuid-nil-result",
			},
			State: "available",
		}

		// Return nil result from SDE update
		env.OnActivity("UpdateSdeActiveDirectory", mock.Anything, params).Return(nil)

		var runResult interface{}
		var runErr *vsaerrors.CustomError
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryUpdateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryUpdateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adRecord)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		assert.NotNil(t, runErr)
		assert.Nil(t, runResult)
		env.AssertExpectations(t)
	})
}

// TestDeleteActiveDirectoryWorkflow tests the DeleteActiveDirectoryWorkflow
func TestDeleteActiveDirectoryWorkflow(t *testing.T) {
	t.Run("Success_VcpOnly_ADExists", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	accountID := int64(123)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-123",
		ProjectNumber:       "test-project-123",
	}

	originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		checkResult := &active_directory_activities.CheckDeletionAllowedResult{
			ADExists:        true,
			DeletionAllowed: true,
		}

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(checkResult, nil)
	env.OnActivity("DeleteVcpActiveDirectory", mock.Anything, params).Return(nil)
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DeleteActiveDirectoryWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("Success_SDE_ADExistsAtBothPlaces", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	accountID := int64(456)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-456",
		ProjectNumber:       "test-project-456",
	}

	originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		checkResult := &active_directory_activities.CheckDeletionAllowedResult{
			ADExists:        true,
			DeletionAllowed: true,
		}

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(checkResult, nil)
	env.OnActivity("DeleteSdeActiveDirectory", mock.Anything, params).Return(nil)
	env.OnActivity("DeleteVcpActiveDirectory", mock.Anything, params).Return(nil)
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DeleteActiveDirectoryWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("Success_SDE_ADNotFoundAtVCP", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	accountID := int64(789)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-789",
		ProjectNumber:       "test-project-789",
	}

	originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		// When AD doesn't exist at VCP, DeletionAllowed is true (no SVMs using it)
		checkResult := &active_directory_activities.CheckDeletionAllowedResult{
			ADExists:        false,
			DeletionAllowed: true,
		}

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(checkResult, nil)
	env.OnActivity("DeleteSdeActiveDirectory", mock.Anything, params).Return(nil)
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DeleteActiveDirectoryWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("Failure_CheckDeletionNotAllowed", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	accountID := int64(999)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-in-use",
		ProjectNumber:       "test-project-999",
	}

	originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		checkResult := active_directory_activities.CheckDeletionAllowedResult{
			ADExists:        true,
			DeletionAllowed: false,
		}

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(checkResult, nil)
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DeleteActiveDirectoryWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("Failure_CheckDeletionError", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	accountID := int64(888)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-check-error",
		ProjectNumber:       "test-project-888",
	}

	originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(nil, errors.New("database error"))
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DeleteActiveDirectoryWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("Failure_DeleteVcpError", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	accountID := int64(777)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-vcp-error",
		ProjectNumber:       "test-project-777",
	}

	originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		checkResult := &active_directory_activities.CheckDeletionAllowedResult{
			ADExists:        true,
			DeletionAllowed: true,
		}

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(checkResult, nil)
	env.OnActivity("DeleteVcpActiveDirectory", mock.Anything, params).Return(errors.New("VCP deletion failed"))
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DeleteActiveDirectoryWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("Failure_DeleteSdeError", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	accountID := int64(666)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-sde-error",
		ProjectNumber:       "test-project-666",
	}

	originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

	checkResult := &active_directory_activities.CheckDeletionAllowedResult{
		ADExists:        true,
		DeletionAllowed: true,
	}

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(checkResult, nil)
	env.OnActivity("DeleteSdeActiveDirectory", mock.Anything, params).Return(errors.New("SDE deletion failed"))
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DeleteActiveDirectoryWorkflow, params)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
})

t.Run("Failure_UpdateJobStatusError", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	accountID := int64(555)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-job-error",
		ProjectNumber:       "test-project-555",
	}

	originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.NewUserInputValidationErr("job status update failed"))

	env.ExecuteWorkflow(DeleteActiveDirectoryWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("Failure_DeleteVcpWhenSDEEnabled", func(t *testing.T) {
		// Test to cover lines 285-286: DeleteVcpActiveDirectory fails when SDE is enabled
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	accountID := int64(222)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-vcp-fail-sde",
		ProjectNumber:       "test-project-222",
	}

	originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost // Enable SDE
		defer func() { cvp.CVP_HOST = originalHost }()

		checkResult := &active_directory_activities.CheckDeletionAllowedResult{
			ADExists:        true,
			DeletionAllowed: true,
		}

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(checkResult, nil)
	env.OnActivity("DeleteSdeActiveDirectory", mock.Anything, params).Return(nil)
	env.OnActivity("DeleteVcpActiveDirectory", mock.Anything, params).Return(errors.New("VCP deletion failed in SDE mode"))
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DeleteActiveDirectoryWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		assert.Contains(t, env.GetWorkflowError().Error(), "VCP deletion failed in SDE mode")
		env.AssertExpectations(t)
	})

	t.Run("Failure_DeleteSdeWhenADNotFoundAtVCP", func(t *testing.T) {
		// Test to cover lines 255-256: DeleteSdeActiveDirectory fails when AD not found at VCP
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		mockStorage := database.NewMockStorage(t)

		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(commonActivity.UpdateJobStatus)

	accountID := int64(111)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-sde-fail-notfound",
		ProjectNumber:       "test-project-111",
	}

	originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost // Enable SDE
		defer func() { cvp.CVP_HOST = originalHost }()

	checkResult := &active_directory_activities.CheckDeletionAllowedResult{
		ADExists:        false, // AD not found at VCP
		DeletionAllowed: true,
	}

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(checkResult, nil)
	env.OnActivity("DeleteSdeActiveDirectory", mock.Anything, params).Return(errors.New("SDE deletion failed"))
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DeleteActiveDirectoryWorkflow, params)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "SDE deletion failed")
	env.AssertExpectations(t)
})
}

// TestActiveDirectoryDeleteWorkflow_Setup tests the Setup method
func TestActiveDirectoryDeleteWorkflow_Setup(t *testing.T) {
	t.Run("SuccessfulSetup", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
	env.SetHeader(mockHeader)

	accountID := int64(123)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-123",
		ProjectNumber:       "test-project-123",
	}

	var setupErr error
		var wf *ActiveDirectoryDeleteWorkflow
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf = &ActiveDirectoryDeleteWorkflow{}
			setupErr = wf.Setup(ctx, params)
			return setupErr
		})

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		wf = &ActiveDirectoryDeleteWorkflow{}
		setupErr = wf.Setup(ctx, params)
		return setupErr
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, setupErr)
	assert.NoError(t, env.GetWorkflowError())
	assert.Equal(t, WorkflowStatusCreated, wf.Status)
	assert.Equal(t, "test-project-123", wf.CustomerID)
	assert.Equal(t, "default-test-workflow-id", wf.ID)
})

	t.Run("QueryHandlerTest", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
	env.SetHeader(mockHeader)

	accountID := int64(456)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-query",
		ProjectNumber:       "test-project-456",
	}

	env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryDeleteWorkflow{}
			err := wf.Setup(ctx, params)
			if err != nil {
				return err
			}
			wf.Status = WorkflowStatusRunning
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryDeleteWorkflow{}
			err := wf.Setup(ctx, params)
			if err != nil {
				return err
			}
			wf.Status = WorkflowStatusRunning
			return nil
		})

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	queryResult, err := env.QueryWorkflow("status")
	assert.NoError(t, err)
	assert.NotNil(t, queryResult)

	var status WorkflowStatus
	err = queryResult.Get(&status)
	assert.NoError(t, err)
	assert.Equal(t, "test-project-456", status.CustomerID)
	assert.Equal(t, "default-test-workflow-id", status.ID)
})
}

// TestActiveDirectoryDeleteWorkflow_Run tests the Run method
func TestActiveDirectoryDeleteWorkflow_Run(t *testing.T) {
	t.Run("SuccessfulVcpOnlyRun", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	originalHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = originalHost }()

	accountID := int64(123)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-run",
		ProjectNumber:       "test-project-123",
	}

	checkResult := &active_directory_activities.CheckDeletionAllowedResult{
		ADExists:        true,
		DeletionAllowed: true,
	}

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(checkResult, nil)
	env.OnActivity("DeleteVcpActiveDirectory", mock.Anything, params).Return(nil)

	var runResult interface{}
	var runErr *vsaerrors.CustomError
	env.RegisterWorkflow(func(ctx workflow.Context) error {
		wf := &ActiveDirectoryDeleteWorkflow{}
		runResult, runErr = wf.Run(ctx, params)
		if runErr != nil {
			return runErr
		}
		return nil
	})

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		wf := &ActiveDirectoryDeleteWorkflow{}
		runResult, runErr = wf.Run(ctx, params)
		if runErr != nil {
			return runErr
		}
		return nil
	})

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		assert.Nil(t, runErr)
		assert.Nil(t, runResult)
		env.AssertExpectations(t)
	})

	t.Run("SuccessfulSdeRun", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	originalHost := cvp.CVP_HOST
	cvp.CVP_HOST = cvpHost
	defer func() { cvp.CVP_HOST = originalHost }()

	accountID := int64(456)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-sde-run",
		ProjectNumber:       "test-project-456",
	}

	checkResult := &active_directory_activities.CheckDeletionAllowedResult{
		ADExists:        true,
		DeletionAllowed: true,
	}

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(checkResult, nil)
	env.OnActivity("DeleteSdeActiveDirectory", mock.Anything, params).Return(nil)
	env.OnActivity("DeleteVcpActiveDirectory", mock.Anything, params).Return(nil)

	var runResult interface{}
	var runErr *vsaerrors.CustomError
	env.RegisterWorkflow(func(ctx workflow.Context) error {
		wf := &ActiveDirectoryDeleteWorkflow{}
		runResult, runErr = wf.Run(ctx, params)
		if runErr != nil {
			return runErr
		}
		return nil
	})

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		wf := &ActiveDirectoryDeleteWorkflow{}
		runResult, runErr = wf.Run(ctx, params)
		if runErr != nil {
			return runErr
		}
		return nil
	})

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		assert.Nil(t, runErr)
		assert.Nil(t, runResult)
		env.AssertExpectations(t)
	})

	t.Run("Failure_ActivityError", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logger": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&active_directory_activities.ActiveDirectoryDeleteActivity{})

	originalHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = originalHost }()

	accountID := int64(789)
	params := &common.DeleteActiveDirectoryParams{
		AccountId:           accountID,
		ActiveDirectoryUUID: "ad-uuid-run-error",
		ProjectNumber:       "test-project-789",
	}

	env.OnActivity("CheckDeletionAllowed", mock.Anything, params).Return(nil, errors.New("check deletion failed"))

	var runResult interface{}
	var runErr *vsaerrors.CustomError
	env.RegisterWorkflow(func(ctx workflow.Context) error {
		wf := &ActiveDirectoryDeleteWorkflow{}
		runResult, runErr = wf.Run(ctx, params)
		if runErr != nil {
			return runErr
		}
		return nil
	})

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		wf := &ActiveDirectoryDeleteWorkflow{}
		runResult, runErr = wf.Run(ctx, params)
		if runErr != nil {
			return runErr
		}
		return nil
	})

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		assert.NotNil(t, runErr)
		assert.Nil(t, runResult)
		env.AssertExpectations(t)
	})
}
