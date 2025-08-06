package vlm

import (
	"errors"
	"fmt"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"strconv"
	"strings"
	"time"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var NewVSAClientWorkflowManager = _newVSAClientWorkflowManager

var (
	VSALifecycleManagerQueuePrefix = env.GetString("VSA_LIFECYCLE_MANAGER_QUEUE_PREFIX", "vsa-lifecycle-manager")
	OntapVersion                   = env.GetString("ONTAP_VERSION", "9.17.1")
	VSALifecycleManagerQueue       = fmt.Sprintf("%s-%s", VSALifecycleManagerQueuePrefix, OntapVersion)

	VlmWorkflowStartToCloseTimeout = env.GetString("VLMWORKFLOW_START_TO_CLOSE_WORKFLOW_TIMEOUT", "20m")
	VlmWorkflowRetryInterval       = env.GetString("VLMWORKFLOW_RETRY_INTERVAL", "1m")
	VlmWorkflowRetryMaxAttempts    = env.GetInt("VLMWORKFLOW_RETRY_MAX_ATTEMPTS", 3)
	VlmWorkflowRetryMaxInterval    = env.GetString("VLMWORKFLOW_RETRY_MAX_INTERVAL", "5m")
	VlmWorkflowRetryBackoff        = env.GetString("VLMWORKFLOW_RETRY_BACKOFF_COEFFICIENT", "2.0")
)

type WorkflowRetryPolicy struct {
	InitialInterval     time.Duration
	BackoffCoefficient  float64
	MaximumInterval     time.Duration
	MaximumAttempts     int
	StartToCloseTimeout time.Duration
}

type VlmWorkflowClient interface {
	CreateVSAClusterDeployment(ctx workflow.Context, createVSAClusterDeploymentRequest *CreateVSAClusterDeploymentRequest) (*CreateVSAClusterDeploymentResponse, error)
	CreateVSASVM(ctx workflow.Context, createSVMRequest *CreateSVMRequest) (*CreateSVMResponse, error)
	DeleteVSAClusterDeployment(ctx workflow.Context, deleteVSAClusterDeploymentRequest *DeleteVSAClusterDeploymentRequest, ontapVersion string) error
	UpdateVSAClusterDeployment(ctx workflow.Context, updateVSAClusterDeploymentRequest *UpdateVSAClusterDeploymentRequest, ontapVersion string) (*UpdateVSAClusterDeploymentResponse, error)
}

type VSAClientWorkflowManager struct {
}

func _newVSAClientWorkflowManager() *VSAClientWorkflowManager {
	return &VSAClientWorkflowManager{}
}

func getVLMWorkerQueue(logger log.Logger, account string) string {
	ontapVersion := OntapVersion
	if utils.IsFileProtocolSupported(account) {
		// not made it has configurable as this will be removed after AGA
		ontapVersion = "9.18.1" // file protocol is supported in 9.18.1 and later
		logger.Info("using 9.18.1 as ontap version for file protocol support", "account", account)
	}
	return fmt.Sprintf("%s-%s", VSALifecycleManagerQueuePrefix, ontapVersion)
}

func (vlmManager *VSAClientWorkflowManager) CreateVSAClusterDeployment(ctx workflow.Context, createVSAClusterDeploymentRequest *CreateVSAClusterDeploymentRequest) (*CreateVSAClusterDeploymentResponse, error) {
	logger := util.GetLogger(ctx)

	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}

	accountId := createVSAClusterDeploymentRequest.VLMConfig.Deployment.Labels["account_id"]

	childWorkflowContxt := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:            createVSAClusterDeploymentRequest.VLMConfig.Deployment.DeploymentID, // This ensures that each child workflow has a unique identifier, even if the same Deployment ID is used across different zones
		TaskQueue:             getVLMWorkerQueue(logger, accountId),                                // As VLM workflows are executed in a VSALifecycleManagerQueue queue
		WaitForCancellation:   true,                                                                // The parent workflow waits until the child workflow is fully canceled (it finishes whatever it needs to do after being canceled).
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,          // Allows reuse only if the previous execution did not complete successfully (e.g., failed, timed out, terminated, or cancelled)
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	})

	createVSAClusterDeploymentResponse := &CreateVSAClusterDeploymentResponse{}

	err = workflow.ExecuteChildWorkflow(childWorkflowContxt, CreateVSAClusterDeploymentWorkflowName, createVSAClusterDeploymentRequest).Get(childWorkflowContxt, &createVSAClusterDeploymentResponse)

	if err != nil {
		logger.Error("Failed to create VSA cluster", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return createVSAClusterDeploymentResponse, nil
}

func (vlmManager *VSAClientWorkflowManager) CreateVSASVM(ctx workflow.Context, createSVMRequest *CreateSVMRequest) (*CreateSVMResponse, error) {
	logger := util.GetLogger(ctx)

	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}

	accountId := createSVMRequest.VLMConfig.Deployment.Labels["account_id"]

	childWorkflowContxt := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		TaskQueue:             getVLMWorkerQueue(logger, accountId),
		WaitForCancellation:   true,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	})

	createSVMResponse := &CreateSVMResponse{}

	err = workflow.ExecuteChildWorkflow(childWorkflowContxt, CreateVSASVMWorkflowName, createSVMRequest).Get(childWorkflowContxt, &createSVMResponse)
	if err != nil {
		logger.Error("Failed to create SVM", "error", err)
		if strings.Contains(err.Error(), "already exists and is in use by a different VM") {
			return nil, nil
		}
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return createSVMResponse, nil
}

func (vlmManager *VSAClientWorkflowManager) DeleteVSAClusterDeployment(ctx workflow.Context, deleteVSAClusterDeploymentRequest *DeleteVSAClusterDeploymentRequest, ontapVersion string) error {
	logger := util.GetLogger(ctx)

	if deleteVSAClusterDeploymentRequest.ProjectID == "" {
		logger.Warnf("Skipping VSA cluster deployment,cannot delete VSA cluster deployment without project ID")
		return nil
	}

	if deleteVSAClusterDeploymentRequest.DeploymentID == "" {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrMissingRequiredInputError, errors.New("deployment ID is required to delete pool")))
	}

	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return err
	}

	childWorkflowContxt := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		TaskQueue:             VSALifecycleManagerQueuePrefix + "-" + ontapVersion,
		WaitForCancellation:   true,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	})

	err = workflow.ExecuteChildWorkflow(childWorkflowContxt, DeleteVSAClusterDeploymentWorkflowName, deleteVSAClusterDeploymentRequest).Get(childWorkflowContxt, nil)
	if err != nil {
		logger.Error("Failed to delete VSA cluster", "error", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

func (vlmManager *VSAClientWorkflowManager) UpdateVSAClusterDeployment(ctx workflow.Context, updateVSAClusterDeploymentRequest *UpdateVSAClusterDeploymentRequest, ontapVersion string) (*UpdateVSAClusterDeploymentResponse, error) {
	logger := util.GetLogger(ctx)

	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}

	childWorkflowContxt := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		TaskQueue:             VSALifecycleManagerQueuePrefix + "-" + ontapVersion,
		WaitForCancellation:   true,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	})

	updateVSAClusterDeploymentResponse := &UpdateVSAClusterDeploymentResponse{}

	err = workflow.ExecuteChildWorkflow(childWorkflowContxt, UpdateVSAClusterDeploymentWorkflowName, updateVSAClusterDeploymentRequest).Get(childWorkflowContxt, &updateVSAClusterDeploymentResponse)

	if err != nil {
		logger.Error("Failed to update VSA cluster", "error", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return updateVSAClusterDeploymentResponse, nil
}

func PopulateRetryPolicyParams() (*WorkflowRetryPolicy, error) {
	activityStartToCloseTimeout, err := time.ParseDuration(VlmWorkflowStartToCloseTimeout)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}

	activityRetryInterval, err := time.ParseDuration(VlmWorkflowRetryInterval)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}

	activityRetryMaxAttempts := VlmWorkflowRetryMaxAttempts

	activityRetryMaxInterval, err := time.ParseDuration(VlmWorkflowRetryMaxInterval)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}

	activityRetryBackoff, err := strconv.ParseFloat(VlmWorkflowRetryBackoff, 64)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}

	return &WorkflowRetryPolicy{
		InitialInterval:     activityRetryInterval,
		StartToCloseTimeout: activityStartToCloseTimeout,
		BackoffCoefficient:  activityRetryBackoff,
		MaximumInterval:     activityRetryMaxInterval,
		MaximumAttempts:     activityRetryMaxAttempts,
	}, nil
}
