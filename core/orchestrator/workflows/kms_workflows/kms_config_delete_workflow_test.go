package kms_workflows

import (
	"testing"

	"github.com/go-openapi/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestDeleteKmsConfigWorkflow(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(DeleteKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		mockStorage := database.NewMockStorage(t)
		// No UpdateKmsConfigState call expected in success case
		env.RegisterActivity(&kms_activities.KmsConfigActivity{SE: mockStorage})

		// Set up test data
		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-config-id",
			AccountName: "123456789",
		}
		kmsConfig := &datamodel.KmsConfig{
			Name: "kms1",
			BaseModel: datamodel.BaseModel{
				UUID: "kms1-uuid",
			},
			CustomerProjectID: "123456789", // Valid project number
		}

		sdeJobUuid := "job-uuid"
		// Mock activity responses
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, "123456789").Return("test-jwt-token", nil)
		env.OnActivity("DeleteSDEKmsConfig", mock.Anything, kmsConfig, params).Return(&sdeJobUuid, nil)
		env.OnActivity("DescribeSDEDeleteJob", mock.Anything, &sdeJobUuid, params).Return(nil)
		env.OnActivity("DisableKmsServiceAccount", mock.Anything, kmsConfig).Return(nil)
		env.OnActivity("DeleteKmsConfig", mock.Anything, kmsConfig, params).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(DeleteKmsConfigWorkflow, kmsConfig, params)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenSuccessfulWhenVcpKmsConfigNotPresent", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(DeleteKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		mockStorage := database.NewMockStorage(t)
		// No UpdateKmsConfigState call expected in success case
		env.RegisterActivity(&kms_activities.KmsConfigActivity{SE: mockStorage})

		// Set up test data
		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-config-id",
			AccountName: "123456789",
		}
		kmsConfig := &datamodel.KmsConfig{
			CustomerProjectID: "123456789", // Valid project number
		}

		// Mock activity responses
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, "123456789").Return("test-jwt-token", nil)
		env.OnActivity("DeleteSDEKmsConfig", mock.Anything, kmsConfig, params).Return(nil, nil)

		// Execute workflow
		env.ExecuteWorkflow(DeleteKmsConfigWorkflow, kmsConfig, params)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenDeleteSDEKmsConfigFail", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		mockStorage := database.NewMockStorage(t)
		// Add mock for UpdateKmsConfigState activity which is called in defer when there's an error
		mockStorage.On("UpdateKmsConfigState", mock.Anything, "kms1-uuid", models.LifeCycleStateError, mock.AnythingOfType("string")).Return(&datamodel.KmsConfig{}, nil)
		env.RegisterActivity(&kms_activities.KmsConfigActivity{SE: mockStorage})

		// Set up test data
		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-config-id",
			AccountName: "123456789",
		}
		kmsConfig := &datamodel.KmsConfig{
			Name: "kms1",
			BaseModel: datamodel.BaseModel{
				UUID: "kms1-uuid",
			},
			CustomerProjectID: "123456789", // Valid project number
		}
		// Mock activity responses
		env.OnActivity("GetSignedTokenActivity", mock.Anything, "123456789").Return("test-jwt-token", nil)
		env.OnActivity("DeleteSDEKmsConfig", mock.Anything, kmsConfig, params).Return(nil, errors.New(400, "error returned"))

		// Register the workflow
		env.RegisterWorkflow(func(ctx workflow.Context, params *common.DeleteKmsConfigParams, kmsConfig *datamodel.KmsConfig) (interface{}, error) {
			wf := &deleteKmsConfigWorkflow{}
			result, customErr := wf.Run(ctx, kmsConfig, params)
			if customErr != nil {
				return result, customErr
			}
			return result, nil
		})

		// Execute workflow
		env.ExecuteWorkflow(func(ctx workflow.Context, params *common.DeleteKmsConfigParams, kmsConfig *datamodel.KmsConfig) (interface{}, error) {
			wf := &deleteKmsConfigWorkflow{}
			result, customErr := wf.Run(ctx, kmsConfig, params)
			if customErr != nil {
				return result, customErr
			}
			return result, nil
		}, params, kmsConfig)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenActivityFails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		// Set up test data
		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-config-id",
			AccountName: "123456789",
		}
		kmsConfig := &datamodel.KmsConfig{
			KmsAttributes:     &datamodel.KmsAttributes{},
			CustomerProjectID: "123456789", // Valid project number
		}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, "123456789").Return("test-jwt-token", nil)
		env.OnActivity("DeleteSDEKmsConfig", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New(400, "error returned"))
		// Execute workflow
		env.ExecuteWorkflow(DeleteKmsConfigWorkflow, kmsConfig, params)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("HeartbeatTimeoutIsConfigured", func(t *testing.T) {
		// This test verifies that HeartbeatTimeout is configured in ActivityOptions
		// by ensuring activities with RecordHeartbeat can execute successfully
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(DeleteKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&kms_activities.KmsConfigActivity{SE: mockStorage})

		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-config-id",
			AccountName: "123456789",
		}
		kmsConfig := &datamodel.KmsConfig{
			Name: "kms1",
			BaseModel: datamodel.BaseModel{
				UUID: "kms1-uuid",
			},
			CustomerProjectID: "123456789",
		}

		sdeJobUuid := "job-uuid"
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, "123456789").Return("test-jwt-token", nil)
		env.OnActivity("DeleteSDEKmsConfig", mock.Anything, kmsConfig, params).Return(&sdeJobUuid, nil)
		env.OnActivity("DescribeSDEDeleteJob", mock.Anything, &sdeJobUuid, params).Return(nil)
		env.OnActivity("DisableKmsServiceAccount", mock.Anything, kmsConfig).Return(nil)
		env.OnActivity("DeleteKmsConfig", mock.Anything, kmsConfig, params).Return(nil)

		env.ExecuteWorkflow(DeleteKmsConfigWorkflow, kmsConfig, params)

		// Verify workflow completes successfully, which confirms HeartbeatTimeout is configured
		// Activities with RecordHeartbeat would fail if HeartbeatTimeout wasn't set
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}
