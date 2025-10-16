package workflows

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"testing"
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
		adUUID := "test-ad-uuid-123"
		accountId := int64(123)

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		env.OnActivity("CreateVcpActiveDirectory", mock.Anything, params, adUUID, accountId).Return(nil, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, params, adUUID, accountId)

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
		adUUID := "test-ad-uuid-456"
		accountId := int64(456)

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		env.OnActivity("CreateSdeActiveDirectory", mock.Anything, params).Return(nil, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, params, adUUID, accountId)

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
		adUUID := "test-ad-uuid-fail"
		accountId := int64(789)

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		expectedError := vsaerrors.NewVCPError
		env.OnActivity("CreateVcpActiveDirectory", mock.Anything, params, adUUID, accountId).Return(expectedError)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, params, adUUID, accountId)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
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
		adUUID := "test-ad-uuid-sde-fail"
		accountId := int64(999)

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = cvpHost
		defer func() { cvp.CVP_HOST = originalHost }()

		expectedError := vsaerrors.NewVCPError
		env.OnActivity("CreateSdeActiveDirectory", mock.Anything, params).Return(expectedError)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, params, adUUID, accountId)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
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
		adUUID := "test-ad-uuid"
		accountId := int64(123)

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, invalidParams, adUUID, accountId)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
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
		adUUID := "test-ad-uuid"
		accountId := int64(123)

		originalHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalHost }()

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.New("job status update failed"))

		env.ExecuteWorkflow(CreateActiveDirectoryWorkflow, params, adUUID, accountId)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
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
		adUUID := "test-uuid"
		accountId := int64(123)

		env.OnActivity("CreateVcpActiveDirectory", mock.Anything, params, adUUID, accountId).Return(nil, nil)

		var runResult interface{}
		var runErr *vsaerrors.CustomError
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adUUID, accountId)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adUUID, accountId)
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
		adUUID := "test-uuid-sde"
		accountId := int64(456)

		env.OnActivity("CreateSdeActiveDirectory", mock.Anything, params).Return(nil, nil)

		var runResult interface{}
		var runErr *vsaerrors.CustomError
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adUUID, accountId)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adUUID, accountId)
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
		adUUID := "test-uuid-activity-fail"
		accountId := int64(789)

		activityError := vsaerrors.NewVCPError
		env.OnActivity("CreateVcpActiveDirectory", mock.Anything, params, adUUID, accountId).Return(activityError)

		var runResult interface{}
		var runErr *vsaerrors.CustomError
		env.RegisterWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adUUID, accountId)
			if runErr != nil {
				return runErr
			}
			return nil
		})

		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			wf := &ActiveDirectoryCreateWorkflow{}
			runResult, runErr = wf.Run(ctx, params, adUUID, accountId)
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
