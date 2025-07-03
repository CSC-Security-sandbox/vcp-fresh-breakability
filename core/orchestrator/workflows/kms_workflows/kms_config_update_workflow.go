package kms_workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type updateKmsConfigWorkflow struct {
	workflows.BaseWorkflow
}

// Enforcing the WorkflowInterface on kmsUpdateWorkflow
var _ workflows.WorkflowInterface = &updateKmsConfigWorkflow{}

// UpdateKmsConfigWorkflow process kms config update related requests from a customer.
func UpdateKmsConfigWorkflow(ctx workflow.Context, kmsConfig *datamodel.KmsConfig, params *common.UpdateKmsConfigParams) (gcpgenserver.V1betaUpdateKmsConfigurationRes, error) {
	kmsConfigWf := new(updateKmsConfigWorkflow)
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
		err = kmsConfigWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		if err != nil {
			return nil, err
		}
	}
	kmsConfigWf.Status = workflows.WorkflowStatusCompleted
	err = kmsConfigWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *updateKmsConfigWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	updateParams := input.(*common.UpdateKmsConfigParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = updateParams.AccountName
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

func (wf *updateKmsConfigWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	kmsConfig := args[0].(*datamodel.KmsConfig)
	params := args[1].(*common.UpdateKmsConfigParams)
	updateActivity := &kms_activities.KmsConfigActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, options)
	jwtToken, err := auth.GetSignedJwtToken(params.AccountName)
	if err != nil {
		return nil, err
	}
	ctx = workflow.WithValue(ctx, middleware.AuthToken, jwtToken)
	defer func() {
		if err != nil && kmsConfig.UUID != "" {
			err = workflow.ExecuteActivity(ctx, updateActivity.UpdateKmsConfigState, kmsConfig, params, models.LifeCycleStateError, err.Error()).Get(ctx, nil)
		}
	}()

	err = workflow.ExecuteActivity(ctx, updateActivity.UpdateSDEKmsConfig, kmsConfig, params).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	if kmsConfig.UUID != "" {
		err = workflow.ExecuteActivity(ctx, updateActivity.UpdateKmsConfig, kmsConfig, params).Get(ctx, nil)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
