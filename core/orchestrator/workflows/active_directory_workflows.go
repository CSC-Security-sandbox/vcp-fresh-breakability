package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type ActiveDirectoryCreateWorkflow struct {
	BaseWorkflow
}

func CreateActiveDirectoryWorkflow(
	ctx workflow.Context,
	params *common.CreateActiveDirectoryParams,
	adUUID string,
	accountId int64,
) (interface{}, error) {
	log := util.GetLogger(ctx)
	activeDirectoryWf := new(ActiveDirectoryCreateWorkflow)

	err := activeDirectoryWf.Setup(ctx, params)
	if err != nil {
		log.Errorf("Failed to setup ActiveDirectoryCreateWorkflow: %v", err)
		return nil, err
	}

	activeDirectoryWf.Status = WorkflowStatusRunning
	err = activeDirectoryWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for ActiveDirectoryCreateWorkflow: %v", err)
		return nil, err
	}

	_, customErr := activeDirectoryWf.Run(ctx, params, adUUID, accountId)
	if customErr != nil {
		log.Errorf("ActiveDirectoryCreateWorkflow completed with error: %v", customErr)
		activeDirectoryWf.Status = WorkflowStatusFailed
		err2 := activeDirectoryWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with error for ActiveDirectoryCreateWorkflow: %v", err2)
			return nil, err2
		}
		return nil, customErr
	}

	activeDirectoryWf.Status = WorkflowStatusCompleted
	err = activeDirectoryWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for ActiveDirectoryCreateWorkflow: %v", err)
	}
	return nil, err
}

func (wf *ActiveDirectoryCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createAdParams := input.(*common.CreateActiveDirectoryParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createAdParams.AccountId
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": wf.ID,
		"customerID": wf.CustomerID,
	})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *ActiveDirectoryCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	logger := util.GetLogger(ctx)
	activeDirectoryActivity := &active_directory_activities.ActiveDirectoryCreateActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	params := args[0].(*common.CreateActiveDirectoryParams)
	adUUID := args[1].(string)
	accountId := args[2].(int64)

	if cvp.CVP_HOST == "" {
		logger.Info("CVP_HOST environment variable is not set, creating AD in VCP")
		err = workflow.ExecuteActivity(
			ctx,
			activeDirectoryActivity.CreateVcpActiveDirectory,
			params,
			adUUID,
			accountId,
		).Get(ctx, nil)
	} else {
		err = workflow.ExecuteActivity(
			ctx,
			activeDirectoryActivity.CreateSdeActiveDirectory,
			params,
		).Get(ctx, nil)
	}

	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, nil
}
