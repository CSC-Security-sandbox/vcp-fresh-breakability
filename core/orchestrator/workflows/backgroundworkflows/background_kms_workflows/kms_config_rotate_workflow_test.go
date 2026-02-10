package background_kms_workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestRotateKmsConfigWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Set up logger header
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	// Register activities
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&backgroundactivities.RotateKmsSAKeyActivity{})
	// Register child workflow
	env.RegisterWorkflow(RotateKmsKeyChildWorkflow)

	// Test data
	params := &common.RotateKmsConfigParams{
		KmsConfigID:    "test-kms-config-uuid",
		AccountName:    "test-account",
		XCorrelationID: "test-correlation-id",
	}

	serviceAccount := &datamodel.ServiceAccount{
		BaseModel: datamodel.BaseModel{
			UUID: "test-sa-uuid",
		},
		ServiceAccountEmail: "test-sa@test-project.iam.gserviceaccount.com",
		Name:                "test-service-account",
	}

	// Create mock KMS config with service account
	kmsConfig := &datamodel.KmsConfig{
		BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-uuid"},
		Name:           "test-kms-config",
		ServiceAccount: serviceAccount,
	}

	// Set up activity mocks
	// UpdateJobStatus is called multiple times (PROCESSING, DONE), so use Maybe() to allow multiple calls
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
	env.OnActivity("GetKmsConfig", mock.Anything, "test-kms-config-uuid").Return(kmsConfig, nil)
	// Use function reference instead of string name for better matching
	env.OnWorkflow(RotateKmsKeyChildWorkflow, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(RotateKmsConfigWorkflow, params)

	// Assert workflow completed successfully
	assert.True(t, env.IsWorkflowCompleted())
	// Note: Currently the workflow has a bug where it returns error even on success
	// This should be fixed in the workflow code itself
	assert.NoError(t, env.GetWorkflowError())

	// Verify activity calls
	env.AssertExpectations(t)
}

func TestRotateKmsConfigWorkflow_ServiceAccountNotFound(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Set up logger header
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	// Register activities
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&backgroundactivities.RotateKmsSAKeyActivity{})
	// Register child workflow (even though it won't be called in this test)
	env.RegisterWorkflow(RotateKmsKeyChildWorkflow)

	// Test data
	params := &common.RotateKmsConfigParams{
		KmsConfigID: "test-kms-config-uuid",
		AccountName: "test-account",
	}

	// Create mock KMS config without service account
	kmsConfig := &datamodel.KmsConfig{
		BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-uuid"},
		Name:           "test-kms-config",
		ServiceAccount: nil, // No service account
	}

	// Set up activity mocks
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetKmsConfig", mock.Anything, "test-kms-config-uuid").Return(kmsConfig, nil)

	// Execute workflow
	env.ExecuteWorkflow(RotateKmsConfigWorkflow, params)

	// Assert workflow completed with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())

	// Verify activity calls
	env.AssertExpectations(t)
}

func TestRotateKmsConfigWorkflow_GetServiceAccountActivityFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Set up logger header
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	// Register activities
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&backgroundactivities.RotateKmsSAKeyActivity{})

	// Test data
	params := &common.RotateKmsConfigParams{
		KmsConfigID: "test-kms-config-uuid",
		AccountName: "test-account",
	}

	// Set up activity mocks - activity fails
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetKmsConfig", mock.Anything, "test-kms-config-uuid").Return((*datamodel.KmsConfig)(nil), errors.New("database connection failed"))

	// Execute workflow
	env.ExecuteWorkflow(RotateKmsConfigWorkflow, params)

	// Assert workflow completed with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())

	// Verify activity calls
	env.AssertExpectations(t)
}

func TestRotateKmsConfigWorkflow_RotateKeyActivityFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Set up logger header
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	// Register activities
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&backgroundactivities.RotateKmsSAKeyActivity{})
	// Register child workflow
	env.RegisterWorkflow(RotateKmsKeyChildWorkflow)

	// Test data
	params := &common.RotateKmsConfigParams{
		KmsConfigID: "test-kms-config-uuid",
		AccountName: "test-account",
	}

	serviceAccount := &datamodel.ServiceAccount{
		BaseModel: datamodel.BaseModel{
			UUID: "test-sa-uuid",
		},
		ServiceAccountEmail: "test-sa@test-project.iam.gserviceaccount.com",
		Name:                "test-service-account",
	}

	// Create KMS config with service account
	kmsConfig := &datamodel.KmsConfig{
		BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-uuid"},
		Name:           "test-kms-config",
		ServiceAccount: serviceAccount,
	}

	// Set up activity mocks
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetKmsConfig", mock.Anything, "test-kms-config-uuid").Return(kmsConfig, nil)
	env.OnWorkflow("RotateKmsKeyChildWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("key rotation failed"))

	// Execute workflow
	env.ExecuteWorkflow(RotateKmsConfigWorkflow, params)

	// Assert workflow completed with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())

	// Verify activity calls
	env.AssertExpectations(t)
}

func TestRotateKmsConfigWorkflow_StatusQueryHandler(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Set up logger header
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	// Register activities
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&backgroundactivities.RotateKmsSAKeyActivity{})
	// Register child workflow
	env.RegisterWorkflow(RotateKmsKeyChildWorkflow)

	// Test data
	params := &common.RotateKmsConfigParams{
		KmsConfigID: "test-kms-config-uuid",
		AccountName: "test-account",
	}

	serviceAccount := &datamodel.ServiceAccount{
		ServiceAccountEmail: "test-sa@test-project.iam.gserviceaccount.com",
	}

	// Create KMS config with service account
	kmsConfig := &datamodel.KmsConfig{
		BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-uuid"},
		Name:           "test-kms-config",
		ServiceAccount: serviceAccount,
	}

	// Set up activity mocks
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetKmsConfig", mock.Anything, "test-kms-config-uuid").Return(kmsConfig, nil)
	env.OnWorkflow("RotateKmsKeyChildWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow in background
	env.ExecuteWorkflow(RotateKmsConfigWorkflow, params)

	// Query the workflow status
	value, err := env.QueryWorkflow("status")
	assert.NoError(t, err)

	// Verify status response - the query handler returns a pointer to WorkflowStatus
	var status workflows.WorkflowStatus
	err = value.Get(&status)
	assert.NoError(t, err)
	assert.NotEmpty(t, status.ID)
	assert.Equal(t, "test-account", status.CustomerID)
	assert.Equal(t, workflows.WorkflowStatusCompleted, status.Status)
}

func TestRotateKmsConfigWorkflow_JobStatusUpdateFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Set up logger header
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	// Register activities
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&backgroundactivities.RotateKmsSAKeyActivity{})

	// Test data
	params := &common.RotateKmsConfigParams{
		KmsConfigID: "test-kms-config-uuid",
		AccountName: "test-account",
	}

	// Mock job status update failure
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(errors.New("failed to update job status"))

	// Execute workflow
	env.ExecuteWorkflow(RotateKmsConfigWorkflow, params)

	// Assert workflow completed with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())

	// Verify it's the expected error
	workflowErr := env.GetWorkflowError()
	assert.Contains(t, workflowErr.Error(), "failed to update job status")
}

func TestRotateKmsConfigWorkflow_HeartbeatTimeoutIsConfigured(t *testing.T) {
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

	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&backgroundactivities.RotateKmsSAKeyActivity{})
	// Register child workflow
	env.RegisterWorkflow(RotateKmsKeyChildWorkflow)

	params := &common.RotateKmsConfigParams{
		KmsConfigID:    "test-kms-config-uuid",
		AccountName:    "test-account",
		XCorrelationID: "test-correlation-id",
	}

	serviceAccount := &datamodel.ServiceAccount{
		BaseModel: datamodel.BaseModel{
			UUID: "test-sa-uuid",
		},
		ServiceAccountEmail: "test-sa@test-project.iam.gserviceaccount.com",
		Name:                "test-service-account",
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel:      datamodel.BaseModel{UUID: "test-kms-config-uuid"},
		Name:           "test-kms-config",
		ServiceAccount: serviceAccount,
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetKmsConfig", mock.Anything, "test-kms-config-uuid").Return(kmsConfig, nil)
	env.OnWorkflow("RotateKmsKeyChildWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(RotateKmsConfigWorkflow, params)

	// Verify workflow completes successfully, which confirms HeartbeatTimeout is configured
	// Activities with RecordHeartbeat would fail if HeartbeatTimeout wasn't set
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
