package background_kms_workflows

import (
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	errorcore "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type rotateKmsConfigWorkflow struct {
	workflows.BaseWorkflow
}

// Enforcing the WorkflowInterface on rotateKmsConfigWorkflow
var _ workflows.WorkflowInterface = &rotateKmsConfigWorkflow{}

// RotateKmsConfigWorkflow processes KMS config service account key rotation requests
func RotateKmsConfigWorkflow(ctx workflow.Context, params *common.RotateKmsConfigParams) (interface{}, error) {
	rotateKmsConfigWf := new(rotateKmsConfigWorkflow)
	err := rotateKmsConfigWf.Setup(ctx, params)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	rotateKmsConfigWf.Status = workflows.WorkflowStatusRunning
	err = rotateKmsConfigWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	_, customError := rotateKmsConfigWf.Run(ctx, params)
	if customError != nil {
		rotateKmsConfigWf.Status = workflows.WorkflowStatusFailed
		err = rotateKmsConfigWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), errorcore.WrapAsTemporalApplicationError(errorcore.NewVCPError(errorcore.ErrKMSRotate, customError)))
		return nil, workflows.ConvertToVSAError(err)
	}

	rotateKmsConfigWf.Status = workflows.WorkflowStatusCompleted
	err = rotateKmsConfigWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	return nil, nil
}

func (rotateKmsConfigWf *rotateKmsConfigWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	rotateKmsConfigParams := input.(*common.RotateKmsConfigParams)
	info := workflow.GetInfo(ctx)
	rotateKmsConfigWf.ID = info.WorkflowExecution.ID
	rotateKmsConfigWf.CustomerID = rotateKmsConfigParams.AccountName
	rotateKmsConfigWf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": rotateKmsConfigWf.ID, "customerID": rotateKmsConfigWf.CustomerID})
	logger := util.GetLogger(ctx)
	rotateKmsConfigWf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:         rotateKmsConfigWf.ID,
			Status:     rotateKmsConfigWf.Status,
			CustomerID: rotateKmsConfigWf.CustomerID,
		}, nil
	})
}

func (rotateKmsConfigWf *rotateKmsConfigWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *errorcore.CustomError) {
	params := args[0].(*common.RotateKmsConfigParams)
	logger := rotateKmsConfigWf.Logger

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		HeartbeatTimeout:    retryPolicy.HeartBeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	rotateKmsSAKeyActivity := &backgroundactivities.RotateKmsSAKeyActivity{}

	// Get the KMS config
	var kmsConfig *datamodel.KmsConfig
	err = workflow.ExecuteActivity(ctx, rotateKmsSAKeyActivity.GetKmsConfig, params.KmsConfigID).Get(ctx, &kmsConfig)
	if err != nil {
		logger.Error("GetKmsConfig activity failed.", "Error", err)
		return nil, workflows.ConvertToVSAError(err)
	}

	if kmsConfig.ServiceAccount == nil {
		logger.Error("Service account not found for KMS config", "KmsConfigID", params.KmsConfigID)
		return nil, errorcore.NewVCPError(errorcore.ErrServiceAccountNotFound, errors.New("service account not found for KMS config"))
	}

	// Execute child workflow for key rotation
	// The child workflow orchestrates all phases: validate, create key, store key, migrate pools, complete
	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:          workflow.GetInfo(ctx).WorkflowExecution.ID + "-child",
		WorkflowRunTimeout:  retryPolicy.StartToCloseTimeout * 10, // Give child workflow more time
		WorkflowTaskTimeout: retryPolicy.StartToCloseTimeout,
	}
	childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

	err = workflow.ExecuteChildWorkflow(childCtx, RotateKmsKeyChildWorkflow, kmsConfig.ServiceAccount, kmsConfig).Get(ctx, nil)
	if err != nil {
		logger.Error("RotateKmsKeyChildWorkflow failed",
			log.Fields{
				"error":               err,
				"serviceAccountEmail": kmsConfig.ServiceAccount.ServiceAccountEmail,
				"kmsConfigID":         params.KmsConfigID,
			})
		return nil, workflows.ConvertToVSAError(err)
	}

	logger.Info("Successfully rotated service account key for KMS config",
		log.Fields{
			"kmsConfigID":         params.KmsConfigID,
			"serviceAccountEmail": kmsConfig.ServiceAccount.ServiceAccountEmail,
		})

	return nil, nil
}
