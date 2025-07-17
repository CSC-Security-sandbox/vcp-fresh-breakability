package kms_workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errorcore "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	SdeKmsJobRetryMaxAttempts = env.GetInt("SDE_KMS_JOBS_RETRY_MAX_ATTEMPTS", 10)
)

type deleteKmsConfigWorkflow struct {
	workflows.BaseWorkflow
}

// Enforcing the WorkflowInterface on kmsDeleteWorkflow
var _ workflows.WorkflowInterface = &deleteKmsConfigWorkflow{}

// DeleteKmsConfigWorkflow process kms config delete request from a customer.
func DeleteKmsConfigWorkflow(ctx workflow.Context, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) (gcpgenserver.V1betaDeleteKmsConfigurationRes, error) {
	kmsConfigWf := new(deleteKmsConfigWorkflow)
	err := kmsConfigWf.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	kmsConfigWf.Status = workflows.WorkflowStatusRunning
	err = kmsConfigWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = kmsConfigWf.Run(ctx, kmsConfig, params)
	if err != nil {
		kmsConfigWf.Status = workflows.WorkflowStatusFailed
		err = kmsConfigWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), errorcore.WrapAsTemporalApplicationError(errorcore.NewVCPError(errorcore.ErrKMSDelete, err)))
		return nil, err
	}
	kmsConfigWf.Status = workflows.WorkflowStatusCompleted
	err = kmsConfigWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *deleteKmsConfigWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	deleteParams := input.(*common.DeleteKmsConfigParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = deleteParams.AccountName
	wf.Status = workflows.WorkflowStatusCreated
	logger := util.GetLogger(ctx)
	wf.Logger = logger.With(log.Fields{
		"workflowID": wf.ID,
		"customerID": wf.CustomerID,
	})

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *deleteKmsConfigWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	kmsConfig := args[0].(*datamodel.KmsConfig)
	params := args[1].(*common.DeleteKmsConfigParams)
	deleteActivity := &kms_activities.KmsConfigActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	defaultActivityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, defaultActivityOpts)
	jwtToken, err := getSignedJwtToken(params.AccountName)
	if err != nil {
		return nil, err
	}
	ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, jwtToken)
	sdeJobRetryOpts := defaultActivityOpts
	sdeJobRetryOpts.RetryPolicy.MaximumAttempts = int32(SdeKmsJobRetryMaxAttempts)
	ctx1 := workflow.WithActivityOptions(ctx, sdeJobRetryOpts)

	defer func() {
		if err != nil && kmsConfig.UUID != "" {
			err = workflow.ExecuteActivity(ctx, deleteActivity.UpdateKmsConfigState, kmsConfig, models.LifeCycleStateError, err.Error()).Get(ctx, nil)
			util.GetLogger(ctx).Errorf("Failed to update KMS config state to error : %w", err)
			return
		}
	}()

	var sdeJobId string
	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteSDEKmsConfig, kmsConfig, params).Get(ctx, &sdeJobId)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx1, deleteActivity.DescribeSDEDeleteJob, &sdeJobId, params).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	if kmsConfig.UUID != "" {
		err = workflow.ExecuteActivity(ctx, deleteActivity.DisableKmsServiceAccount, kmsConfig).Get(ctx, nil)
		if err != nil {
			return nil, err
		}

		err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteKmsConfig, kmsConfig, params).Get(ctx, nil)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
