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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestUpdateKmsConfigWorkflow(t *testing.T) {
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
		env.RegisterWorkflow(UpdateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		// Set up test data
		params := &common.UpdateKmsConfigParams{
			Name:            "test-pool",
			AccountName:     "test-account",
			KeyRing:         "key-ring1",
			KeyName:         "key1",
			KeyRingLocation: "test-region",
		}
		kmsConfig := &datamodel.KmsConfig{
			Name: "kms1",
			BaseModel: datamodel.BaseModel{
				UUID: "kms1-uuid",
			},
		}

		// Mock activity responses
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateSDEKmsConfig", mock.Anything, kmsConfig, params).Return(nil)
		env.OnActivity("UpdateKmsConfig", mock.Anything, kmsConfig, params).Return(nil)

		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		// Execute workflow
		env.ExecuteWorkflow(UpdateKmsConfigWorkflow, kmsConfig, params)

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
		env.RegisterWorkflow(UpdateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		// Set up test data
		params := &common.UpdateKmsConfigParams{
			Name:            "test-pool",
			AccountName:     "test-account",
			KeyRing:         "key-ring1",
			KeyName:         "key1",
			KeyRingLocation: "test-region",
		}
		kmsConfig := &datamodel.KmsConfig{}

		// Mock activity responses
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateSDEKmsConfig", mock.Anything, kmsConfig, params).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(UpdateKmsConfigWorkflow, kmsConfig, params)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenUpdateSDEKmsConfigFail", func(tt *testing.T) {
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

		auth.GetSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		// Set up test data
		params := &common.UpdateKmsConfigParams{
			Name:            "test-pool",
			AccountName:     "test-account",
			KeyRing:         "key-ring1",
			KeyName:         "key1",
			KeyRingLocation: "test-region",
		}
		kmsConfig := &datamodel.KmsConfig{
			Name: "kms1",
			BaseModel: datamodel.BaseModel{
				UUID: "kms1-uuid",
			},
		}
		// Mock activity responses
		env.OnActivity("UpdateSDEKmsConfig", mock.Anything, kmsConfig, params).Return(errors.New(400, "error returned"))

		// Register the workflow
		env.RegisterWorkflow(func(ctx workflow.Context, params *common.UpdateKmsConfigParams, kmsConfig *datamodel.KmsConfig) (interface{}, error) {
			wf := &updateKmsConfigWorkflow{}
			return wf.Run(ctx, kmsConfig, params)
		})

		// Execute workflow
		env.ExecuteWorkflow(func(ctx workflow.Context, params *common.UpdateKmsConfigParams, kmsConfig *datamodel.KmsConfig) (interface{}, error) {
			wf := &updateKmsConfigWorkflow{}
			return wf.Run(ctx, kmsConfig, params)
		}, params, kmsConfig)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}
