package workflows

import (
	"fmt"

	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type updateBackupPolicyWorkflow struct {
	BaseWorkflow
}

var _ WorkflowInterface = &updateBackupPolicyWorkflow{}

func UpdateBackupPolicyWorkflow(ctx workflow.Context, params *common.UpdateBackupPolicyParams, dbBackupPolicy *datamodel.BackupPolicy) error {
	updateBackupPolicyWF := new(updateBackupPolicyWorkflow)
	err := updateBackupPolicyWF.Setup(ctx, params)
	if err != nil {
		return err
	}
	updateBackupPolicyWF.Status = WorkflowStatusRunning
	err = updateBackupPolicyWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return err
	}
	_, err = updateBackupPolicyWF.Run(ctx, params, dbBackupPolicy)
	if err != nil {
		updateBackupPolicyWF.Status = WorkflowStatusFailed
		err2 := updateBackupPolicyWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err2 != nil {
			updateBackupPolicyWF.Logger.Errorf("Failed to update job status for workflow %s: %v", updateBackupPolicyWF.ID, err2)
		}
		return err
	}
	updateBackupPolicyWF.Status = WorkflowStatusCompleted
	err2 := updateBackupPolicyWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	updateBackupPolicyWF.Logger.Errorf("Failed to update job status for workflow %s: %v", updateBackupPolicyWF.ID, err2)
	return nil
}

func (wf *updateBackupPolicyWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	params := input.(*common.UpdateBackupPolicyParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = params.AccountName
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger
	// Set the query handler in a non-blocking way
	err := workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (wf *updateBackupPolicyWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	params := args[0].(*common.UpdateBackupPolicyParams)
	dbBackupPolicy := args[1].(*datamodel.BackupPolicy)

	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	commonActivities := &activities.CommonActivities{}
	backupPolicyActivity := &activities.BackupPolicyActivity{}
	ctx = workflow.WithActivityOptions(ctx, ao)

	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()
	rollbackManager.AddActivity(backupPolicyActivity.RevertBackupPolicyUpdateInVCP, dbBackupPolicy)

	var authToken string
	err = workflow.ExecuteActivity(ctx, commonActivities.GetAuthJWTToken, params.AccountName).Get(ctx, &authToken)
	if err != nil {
		return nil, err
	}
	ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, authToken)

	var sdeBackupPolicy *cvpmodels.BackupPolicyV1beta
	err = workflow.ExecuteActivity(ctx, backupPolicyActivity.UpdateBackupPolicyInSDE, params).Get(ctx, &sdeBackupPolicy)
	if err != nil {
		wf.Logger.Errorf("Failed to update backup policy in SDE: backupPolicy: %v, err: %v", dbBackupPolicy, err.Error())
		return nil, fmt.Errorf("UpdateBackupPolicyInSDE failed: %v", err.Error())
	}

	rollbackManager.AddActivity(backupPolicyActivity.RevertBackupPolicyUpdateInSDE, params, dbBackupPolicy)
	if params.PolicyEnabled != nil {
		if *params.PolicyEnabled && !dbBackupPolicy.PolicyEnabled {
			err = workflow.ExecuteActivity(ctx, backupPolicyActivity.UnpauseBackupPolicySchedule, dbBackupPolicy).Get(ctx, nil)
			if err != nil {
				return nil, err
			}
			rollbackManager.AddActivity(backupPolicyActivity.PauseBackupPolicySchedule, dbBackupPolicy)
		} else if !*params.PolicyEnabled && dbBackupPolicy.PolicyEnabled {
			err = workflow.ExecuteActivity(ctx, backupPolicyActivity.PauseBackupPolicySchedule, dbBackupPolicy).Get(ctx, nil)
			if err != nil {
				return nil, err
			}
			rollbackManager.AddActivity(backupPolicyActivity.UnpauseBackupPolicySchedule, dbBackupPolicy)
		}
	}

	var updatedBackupPolicy *datamodel.BackupPolicy
	err = workflow.ExecuteActivity(ctx, backupPolicyActivity.UpdateBackupPolicyInVCP, params, dbBackupPolicy).Get(ctx, &updatedBackupPolicy)
	if err != nil {
		return nil, err
	}
	return updatedBackupPolicy, nil
}
