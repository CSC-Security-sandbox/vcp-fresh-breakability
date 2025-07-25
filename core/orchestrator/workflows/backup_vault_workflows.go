package workflows

import (
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type backupVaultUpdateWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

type backupVaultDeleteWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

var _ WorkflowInterface = &backupVaultUpdateWorkflow{}
var _ WorkflowInterface = &backupVaultDeleteWorkflow{}

func UpdateBackupVaultWorkflow(ctx workflow.Context, params *common.BackupVaultParams, backupVault *datamodel.BackupVault) (gcpgenserver.V1betaUpdateBackupVaultRes, error) {
	bvWF := new(backupVaultUpdateWorkflow)
	err := bvWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	bvWF.Status = WorkflowStatusRunning
	err = bvWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = bvWF.Run(ctx, backupVault, params)
	if err != nil {
		bvWF.Status = WorkflowStatusFailed
		err = bvWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
	}
	bvWF.Status = WorkflowStatusCompleted
	err = bvWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

// Setup UpdateBackupVaultWorkflow process pool related requests from a customer.
func (wf *backupVaultUpdateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	BackupVaultParams := input.(*common.BackupVaultParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = BackupVaultParams.AccountName
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

func (wf *backupVaultUpdateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	backupVault := args[0].(*datamodel.BackupVault)
	bvCommonParams := args[1].(*common.BackupVaultParams)
	backupVaultActivity := &activities.BackupVaultActivity{}

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
	ctx = workflow.WithActivityOptions(ctx, ao)

	defer func() {
		if err != nil {
			err = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultStateInCaseOfError, backupVault, models.LifeCycleStateError, err.Error()).Get(ctx, nil)
		}
	}()

	var jwtToken string
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetAuthJWTToken, bvCommonParams.AccountName).Get(ctx, &jwtToken)
	if err != nil {
		return nil, err
	}
	ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, jwtToken)

	sdeBackupVault := &datamodel.BackupVault{}
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultInSDE, bvCommonParams).Get(ctx, &sdeBackupVault)
	if err != nil {
		wf.Logger.Error("Failed to update backup vault in SDE", log.Fields{
			"error":  err,
			"params": backupVault,
		})
		return nil, fmt.Errorf("UpdateBackupVaultInSDE failed: %w", err)
	}

	dbBackupVault := &datamodel.BackupVault{}
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultInVCP, &sdeBackupVault, backupVault).Get(ctx, &dbBackupVault)
	if err != nil {
		return nil, err
	}

	return dbBackupVault, nil
}

func DeleteBackupVaultWorkflow(ctx workflow.Context, params *common.BackupVaultParams, backupVault *datamodel.BackupVault) (gcpgenserver.V1betaDeleteBackupVaultRes, error) {
	bvWF := new(backupVaultDeleteWorkflow)
	err := bvWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	bvWF.Status = WorkflowStatusRunning
	err = bvWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = bvWF.Run(ctx, backupVault, params)
	if err != nil {
		bvWF.Status = WorkflowStatusFailed
		err = bvWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
	}
	bvWF.Status = WorkflowStatusCompleted
	err = bvWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

// Setup UpdateBackupVaultWorkflow process pool related requests from a customer.
func (wf *backupVaultDeleteWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	BackupVaultParams := input.(*common.BackupVaultParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = BackupVaultParams.AccountName
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

func (wf *backupVaultDeleteWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	backupVault := args[0].(*datamodel.BackupVault)
	bvCommonParams := args[1].(*common.BackupVaultParams)
	backupVaultActivity := &activities.BackupVaultActivity{}

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
	ctx = workflow.WithActivityOptions(ctx, ao)

	defer func() {
		if err != nil {
			err = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultStateInCaseOfError, backupVault, models.LifeCycleStateError, err.Error()).Get(ctx, nil)
		}
	}()

	var jwtToken string
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetAuthJWTToken, bvCommonParams.AccountName).Get(ctx, &jwtToken)
	if err != nil {
		return nil, err
	}
	ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, jwtToken)

	sdeBackupVault := &datamodel.BackupVault{}
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.DeleteBackupVaultInSDE, bvCommonParams).Get(ctx, &sdeBackupVault)
	if err != nil {
		wf.Logger.Error("Failed to delete backup vault in SDE", log.Fields{
			"error":  err,
			"params": backupVault,
		})
		return nil, fmt.Errorf("DeleteBackupVaultInSDE failed: %w", err)
	}

	// Delete associated buckets
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.DeleteBackupVaultBuckets, backupVault).Get(ctx, nil)
	if err != nil {
		wf.Logger.Error("Failed to delete backup vault buckets", log.Fields{
			"error":  err,
			"params": backupVault,
		})
		return nil, fmt.Errorf("DeleteBackupVaultBuckets failed: %w", err)
	}

	// Delete backup vault in VCP database
	dbBackupVault := &datamodel.BackupVault{}
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.DeleteBackupVaultInVCP, bvCommonParams.BackupVaultID).Get(ctx, &dbBackupVault)
	if err != nil {
		return nil, err
	}

	return dbBackupVault, nil
}
