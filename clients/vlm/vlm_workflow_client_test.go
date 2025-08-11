package vlm

import (
	"errors"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestCreateVSAClusterDeployment(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, request *CreateVSAClusterDeploymentRequest) error {
			return nil
		},
		workflow.RegisterOptions{Name: CreateVSAClusterDeploymentWorkflowName},
	)

	createVSAClusterDeploymentRequest := &CreateVSAClusterDeploymentRequest{
		VLMConfig: VLMConfig{
			Deployment: DeploymentConfig{
				DeploymentID: "test-deployment-id",
			},
		},
	}

	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := vlmManager.CreateVSAClusterDeployment(ctx, createVSAClusterDeploymentRequest)
		return err
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestCreateVSAClusterDeployment_Error(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	// Register a workflow that returns an error
	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, request *CreateVSAClusterDeploymentRequest) error {
			return errors.New("child workflow failed")
		},
		workflow.RegisterOptions{Name: CreateVSAClusterDeploymentWorkflowName},
	)

	createVSAClusterDeploymentRequest := &CreateVSAClusterDeploymentRequest{
		VLMConfig: VLMConfig{
			Deployment: DeploymentConfig{
				DeploymentID: "test-deployment-id",
			},
		},
	}

	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := vlmManager.CreateVSAClusterDeployment(ctx, createVSAClusterDeploymentRequest)
		return err
	})

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
}

func TestCreateVSASVM(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, req *CreateSVMRequest) error {
			return nil
		},
		workflow.RegisterOptions{Name: CreateVSASVMWorkflowName},
	)

	createSVMRequest := &CreateSVMRequest{}
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := vlmManager.CreateVSASVM(ctx, createSVMRequest)
		return err
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestCreateVSASVM_Error(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, req *CreateSVMRequest) error {
			return errors.New("child workflow failed")
		},
		workflow.RegisterOptions{Name: CreateVSASVMWorkflowName},
	)

	createSVMRequest := &CreateSVMRequest{}
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := vlmManager.CreateVSASVM(ctx, createSVMRequest)
		return err
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestCreateVSASVM_ErrorNotAlreadyExists(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, req *CreateSVMRequest) error {
			return errors.New("some other error")
		},
		workflow.RegisterOptions{Name: CreateVSASVMWorkflowName},
	)

	createSVMRequest := &CreateSVMRequest{}
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := vlmManager.CreateVSASVM(ctx, createSVMRequest)
		return err
	})

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
}

func TestCreateVSASVM_ErrorAlreadyExistsInUseByDifferentVM(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)
	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, req *CreateSVMRequest) error {
			return errors.New("already exists and is in use by a different VM")
		},
		workflow.RegisterOptions{Name: CreateVSASVMWorkflowName},
	)

	createSVMRequest := &CreateSVMRequest{}
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := vlmManager.CreateVSASVM(ctx, createSVMRequest)
		return err
	})

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.NoError(t, err)
}

func TestDeleteVSAClusterDeployment(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, req *DeleteVSAClusterDeploymentRequest) error {
			return nil
		},
		workflow.RegisterOptions{Name: DeleteVSAClusterDeploymentWorkflowName},
	)

	deleteReq := &DeleteVSAClusterDeploymentRequest{
		ProjectID:    "test-project-id",
		DeploymentID: "test-deployment-id",
	}
	ontapVersion := "1.0.0"
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return vlmManager.DeleteVSAClusterDeployment(ctx, deleteReq, ontapVersion)
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	expectedTaskQueue := VSALifecycleManagerQueuePrefix + "-" + ontapVersion
	assert.Equal(t, "vsa-lifecycle-manager-1.0.0", expectedTaskQueue, "Task queue should contain ONTAP version")
}

func TestDeleteVSAClusterDeployment_Error(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, req *DeleteVSAClusterDeploymentRequest) error {
			return errors.New("child workflow failed")
		},
		workflow.RegisterOptions{Name: DeleteVSAClusterDeploymentWorkflowName},
	)

	deleteReq := &DeleteVSAClusterDeploymentRequest{
		ProjectID:    "test-project-id",
		DeploymentID: "test-deployment-id",
	}
	ontapVersion := "1.0.0"
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return vlmManager.DeleteVSAClusterDeployment(ctx, deleteReq, ontapVersion)
	})

	assert.True(t, env.IsWorkflowCompleted())
	expectedTaskQueue := VSALifecycleManagerQueuePrefix + "-" + ontapVersion
	assert.Equal(t, "vsa-lifecycle-manager-1.0.0", expectedTaskQueue, "Task queue should contain ONTAP version")
	assert.Error(t, env.GetWorkflowError())
}

func TestDeleteVSAClusterDeployment_EmptyDeploymentID(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	deleteReq := &DeleteVSAClusterDeploymentRequest{
		ProjectID:    "test-project-id",
		DeploymentID: "",
	}
	ontapVersion := "1.0.0"
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return vlmManager.DeleteVSAClusterDeployment(ctx, deleteReq, ontapVersion)
	})

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	expectedTaskQueue := VSALifecycleManagerQueuePrefix + "-" + ontapVersion
	assert.Equal(t, "vsa-lifecycle-manager-1.0.0", expectedTaskQueue, "Task queue should contain ONTAP version")
	assert.Error(t, err)
}

// Add new test cases for the new ProjectID validation logic
func TestDeleteVSAClusterDeployment_EmptyProjectID(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	deleteReq := &DeleteVSAClusterDeploymentRequest{
		ProjectID:    "",
		DeploymentID: "test-deployment-id",
	}
	ontapVersion := "1.0.0"
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return vlmManager.DeleteVSAClusterDeployment(ctx, deleteReq, ontapVersion)
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError(), "Should return nil when ProjectID is empty")
}

func TestDeleteVSAClusterDeployment_BothEmpty(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	deleteReq := &DeleteVSAClusterDeploymentRequest{
		ProjectID:    "",
		DeploymentID: "",
	}
	ontapVersion := "1.0.0"
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return vlmManager.DeleteVSAClusterDeployment(ctx, deleteReq, ontapVersion)
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError(), "Should return nil when ProjectID is empty, regardless of DeploymentID")
}

func TestPopulateRetryPolicyParams_InvalidStartToCloseTimeout(t *testing.T) {
	orig := VlmWorkflowStartToCloseTimeout
	VlmWorkflowStartToCloseTimeout = "invalid"
	defer func() { VlmWorkflowStartToCloseTimeout = orig }()

	policy, err := PopulateRetryPolicyParams()
	assert.Nil(t, policy)
	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "invalid")
}

func TestPopulateRetryPolicyParams_InvalidRetryInterval(t *testing.T) {
	orig := VlmWorkflowRetryInterval
	VlmWorkflowRetryInterval = "invalid"
	defer func() { VlmWorkflowRetryInterval = orig }()

	policy, err := PopulateRetryPolicyParams()
	assert.Nil(t, policy)
	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "invalid")
}

func TestPopulateRetryPolicyParams_InvalidRetryMaxInterval(t *testing.T) {
	orig := VlmWorkflowRetryMaxInterval
	VlmWorkflowRetryMaxInterval = "invalid"
	defer func() { VlmWorkflowRetryMaxInterval = orig }()

	policy, err := PopulateRetryPolicyParams()
	assert.Nil(t, policy)
	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "invalid")
}

func TestPopulateRetryPolicyParams_InvalidRetryBackoff(t *testing.T) {
	orig := VlmWorkflowRetryBackoff
	VlmWorkflowRetryBackoff = "invalid"
	defer func() { VlmWorkflowRetryBackoff = orig }()

	policy, err := PopulateRetryPolicyParams()
	assert.Nil(t, policy)
	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "invalid")
}

func TestUpdateVSAClusterDeployment(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, request *UpdateVSAClusterDeploymentRequest) error {
			return nil
		},
		workflow.RegisterOptions{Name: UpdateVSAClusterDeploymentWorkflowName},
	)

	updateVSAClusterDeploymentRequest := &UpdateVSAClusterDeploymentRequest{}
	ontapVersion := "1.0.0"
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := vlmManager.UpdateVSAClusterDeployment(ctx, updateVSAClusterDeploymentRequest, ontapVersion)
		return err
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	expectedTaskQueue := VSALifecycleManagerQueuePrefix + "-" + ontapVersion
	assert.Equal(t, "vsa-lifecycle-manager-1.0.0", expectedTaskQueue, "Task queue should contain ONTAP version")
}

func TestUpdateVSAClusterDeployment_Error(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{
		"requestCorrelationID": "test-correlation-id",
	})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, request *UpdateVSAClusterDeploymentRequest) error {
			return errors.New("child workflow failed")
		},
		workflow.RegisterOptions{Name: UpdateVSAClusterDeploymentWorkflowName},
	)

	updateVSAClusterDeploymentRequest := &UpdateVSAClusterDeploymentRequest{}
	ontapVersion := "1.0.0"
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := vlmManager.UpdateVSAClusterDeployment(ctx, updateVSAClusterDeploymentRequest, ontapVersion)
		return err
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	expectedTaskQueue := VSALifecycleManagerQueuePrefix + "-" + ontapVersion
	assert.Equal(t, "vsa-lifecycle-manager-1.0.0", expectedTaskQueue, "Task queue should contain ONTAP version")
}

func TestUpdateVSAClusterDeployment_Error_CorrelationID_NotFound(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, request *UpdateVSAClusterDeploymentRequest) error {
			return nil
		},
		workflow.RegisterOptions{Name: UpdateVSAClusterDeploymentWorkflowName},
	)

	updateVSAClusterDeploymentRequest := &UpdateVSAClusterDeploymentRequest{}
	ontapVersion := "1.0.0"
	vlmManager := NewVSAClientWorkflowManager()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := vlmManager.UpdateVSAClusterDeployment(ctx, updateVSAClusterDeploymentRequest, ontapVersion)
		return err
	})

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "correlation ID not found")
	expectedTaskQueue := VSALifecycleManagerQueuePrefix + "-" + ontapVersion
	assert.Equal(t, "vsa-lifecycle-manager-1.0.0", expectedTaskQueue, "Task queue should contain ONTAP version")
}
