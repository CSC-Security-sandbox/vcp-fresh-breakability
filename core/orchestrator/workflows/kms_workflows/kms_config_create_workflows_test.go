package kms_workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestCreateKmsConfig(t *testing.T) {
	t.Run("WhenCreateKmsConfigSDEActivityFails", func(t *testing.T) {
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
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateKmsConfigSDEActivity", mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenPollKmsConfigOperationActivityFails", func(t *testing.T) {
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
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{}
		response := &kms_configurations.V1betaCreateKmsConfigurationAccepted{}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateKmsConfigSDEActivity", mock.Anything, mock.Anything).Return(response, nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil,
			temporal.NewNonRetryableApplicationError("some", "error", nil))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenDescribeKmsConfigurationActivityFails", func(t *testing.T) {
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
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{}
		response := &kms_configurations.V1betaCreateKmsConfigurationAccepted{}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateKmsConfigSDEActivity", mock.Anything, mock.Anything).Return(response, nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("DescribeKmsConfigurationActivity", mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreateVSAKmsConfigSAKeyActivityFails", func(t *testing.T) {
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
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{}
		response := &kms_configurations.V1betaCreateKmsConfigurationAccepted{}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateKmsConfigSDEActivity", mock.Anything, mock.Anything).Return(response, nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("DescribeKmsConfigurationActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenGrantRoleActivityFails", func(t *testing.T) {
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
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{}
		response := &kms_configurations.V1betaCreateKmsConfigurationAccepted{}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateKmsConfigSDEActivity", mock.Anything, mock.Anything).Return(response, nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("DescribeKmsConfigurationActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(errors.New("some error"))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreatedKmsConfigActivityFails", func(t *testing.T) {
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
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{}
		response := &kms_configurations.V1betaCreateKmsConfigurationAccepted{}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateKmsConfigSDEActivity", mock.Anything, mock.Anything).Return(response, nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("DescribeKmsConfigurationActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreatedKmsConfigActivity", mock.Anything, mock.Anything).Return(errors.New("some error"))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenSuccess", func(t *testing.T) {
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
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{}
		response := &kms_configurations.V1betaCreateKmsConfigurationAccepted{}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateKmsConfigSDEActivity", mock.Anything, mock.Anything).Return(response, nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("DescribeKmsConfigurationActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreatedKmsConfigActivity", mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

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
