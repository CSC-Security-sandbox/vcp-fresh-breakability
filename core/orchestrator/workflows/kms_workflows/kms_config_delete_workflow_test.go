package kms_workflows

import (
	"testing"

	"github.com/go-openapi/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
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

		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		// Set up test data
		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-config-id",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{
			Name: "kms1",
			BaseModel: datamodel.BaseModel{
				UUID: "kms1-uuid",
			},
		}

		sdeJobUuid := "job-uuid"
		// Mock activity responses
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
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

		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		// Set up test data
		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-config-id",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{}

		// Mock activity responses
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
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
		// UpdateKmsConfigState should be called in the defer function when there's an error
		mockStorage.On("UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.KmsConfig{}, nil)
		env.RegisterActivity(&kms_activities.KmsConfigActivity{SE: mockStorage})

		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		// Set up test data
		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-config-id",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{
			Name: "kms1",
			BaseModel: datamodel.BaseModel{
				UUID: "kms1-uuid",
			},
		}
		// Mock activity responses
		env.OnActivity("DeleteSDEKmsConfig", mock.Anything, kmsConfig, params).Return(nil, errors.New(400, "error returned"))

		// Register the workflow
		env.RegisterWorkflow(func(ctx workflow.Context, params *common.DeleteKmsConfigParams, kmsConfig *datamodel.KmsConfig) (interface{}, error) {
			wf := &deleteKmsConfigWorkflow{}
			return wf.Run(ctx, kmsConfig, params)
		})

		// Execute workflow
		env.ExecuteWorkflow(func(ctx workflow.Context, params *common.DeleteKmsConfigParams, kmsConfig *datamodel.KmsConfig) (interface{}, error) {
			wf := &deleteKmsConfigWorkflow{}
			return wf.Run(ctx, kmsConfig, params)
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

		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		// Set up test data
		params := &common.DeleteKmsConfigParams{
			KmsConfigID: "test-config-id",
			AccountName: "test-account",
		}
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		kmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSDEKmsConfig", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New(400, "error returned"))
		// Execute workflow
		env.ExecuteWorkflow(DeleteKmsConfigWorkflow, kmsConfig, params)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}
