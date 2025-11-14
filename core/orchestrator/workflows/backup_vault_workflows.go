package workflows

import (
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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

var (
	_ WorkflowInterface = &backupVaultUpdateWorkflow{}
	_ WorkflowInterface = &backupVaultDeleteWorkflow{}
)

func UpdateBackupVaultWorkflow(ctx workflow.Context, params *common.BackupVaultParams, backupVault *datamodel.BackupVault) (gcpgenserver.V1betaUpdateBackupVaultRes, error) {
	bvWF := new(backupVaultUpdateWorkflow)
	err := bvWF.Setup(ctx, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	bvWF.Status = WorkflowStatusRunning
	err = bvWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	_, customErr := bvWF.Run(ctx, backupVault, params)

	if customErr != nil {
		bvWF.Status = WorkflowStatusFailed
		err2 := bvWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			bvWF.Logger.Errorf("Error when updating the job status: %v", err2)
		}
		return nil, customErr
	}
	bvWF.Status = WorkflowStatusCompleted
	err2 := bvWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		bvWF.Logger.Errorf("Error when updating the job status: %v", err2)
	}
	return nil, nil
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

func (wf *backupVaultUpdateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	backupVault := args[0].(*datamodel.BackupVault)
	bvCommonParams := args[1].(*common.BackupVaultParams)
	backupVaultActivity := &activities.BackupVaultActivity{}

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

	defer func() {
		if err != nil {
			_ = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultStateInCaseOfError, backupVault, models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).Get(ctx, nil)
		}
	}()

	var jwtToken string
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetAuthJWTToken, bvCommonParams.AccountName).Get(ctx, &jwtToken)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, jwtToken)

	sdeBackupVault := &datamodel.BackupVault{}
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultInSDE, bvCommonParams).Get(ctx, &sdeBackupVault)
	if err != nil {
		wf.Logger.Error("Failed to update backup vault in SDE", log.Fields{
			"error":  err,
			"params": backupVault,
		})
		return nil, ConvertToVSAError(fmt.Errorf("UpdateBackupVaultInSDE failed: %w", err))
	}

	dbBackupVault := &datamodel.BackupVault{}
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultInVCP, &sdeBackupVault, backupVault).Get(ctx, &dbBackupVault)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if dbBackupVault.BackupVaultType == activities.CrossRegionBackupType && *dbBackupVault.BackupRegionName != "" {
		remoteParams := *bvCommonParams
		remoteParams.BackupRegion = dbBackupVault.BackupRegionName

		dbRemoteBackupVault := &datamodel.BackupVault{}
		err = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateRemoteBackupVaultInVCP, &remoteParams, dbBackupVault).Get(ctx, &dbRemoteBackupVault)
		if err != nil {
			wf.Logger.Error("Failed to update remote backup vault in VCP", log.Fields{
				"error":  err,
				"params": dbBackupVault,
			})
			return nil, ConvertToVSAError(fmt.Errorf("UpdateRemoteBackupVaultInVCP failed: %w", err))
		}
	}

	return dbBackupVault, nil
}

func DeleteBackupVaultWorkflow(ctx workflow.Context, params *common.BackupVaultParams, backupVault *datamodel.BackupVault) (gcpgenserver.V1betaDeleteBackupVaultRes, error) {
	bvWF := new(backupVaultDeleteWorkflow)
	err := bvWF.Setup(ctx, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	bvWF.Status = WorkflowStatusRunning
	err = bvWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	_, customErr := bvWF.Run(ctx, backupVault, params)

	if customErr != nil {
		bvWF.Status = WorkflowStatusFailed
		err2 := bvWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			bvWF.Logger.Errorf("Error when updating the job status: %v", err2)
		}
		return nil, customErr
	}
	bvWF.Status = WorkflowStatusCompleted
	err2 := bvWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err2 != nil {
		bvWF.Logger.Errorf("Error when updating the job status: %v", err2)
	}
	return nil, nil
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

func (wf *backupVaultDeleteWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	backupVault := args[0].(*datamodel.BackupVault)
	bvCommonParams := args[1].(*common.BackupVaultParams)
	backupVaultActivity := &activities.BackupVaultActivity{}

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

	defer func() {
		if err != nil {
			_ = workflow.ExecuteActivity(ctx, backupVaultActivity.UpdateBackupVaultStateInCaseOfError, backupVault, models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).Get(ctx, nil)
		}
	}()

	var jwtToken string
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetAuthJWTToken, bvCommonParams.AccountName).Get(ctx, &jwtToken)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, jwtToken)

	// Delete associated buckets
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.DeleteBackupVaultBuckets, backupVault).Get(ctx, nil)
	if err != nil {
		// We may fail to delete the buckets as there might be some backups which will be later deleted by Ontap GC.
		wf.Logger.Error("Failed to delete backup vault buckets", log.Fields{
			"error":  err,
			"params": backupVault,
		})
	}

	// Delete backup vault in VCP database
	dbBackupVault := &datamodel.BackupVault{}
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.DeleteBackupVaultInVCP, bvCommonParams.BackupVaultID).Get(ctx, &dbBackupVault)
	if err != nil {
		wf.Logger.Error("Failed to delete backup vault in VCP", log.Fields{
			"error":  err,
			"params": backupVault,
		})
		return nil, ConvertToVSAError(fmt.Errorf("DeleteBackupVaultInVCP failed: %w", err))
	}

	sdeBackupVault := &datamodel.BackupVault{}
	err = workflow.ExecuteActivity(ctx, backupVaultActivity.DeleteBackupVaultInSDE, bvCommonParams).Get(ctx, &sdeBackupVault)
	if err != nil {
		wf.Logger.Error("Failed to delete backup vault in SDE", log.Fields{
			"error":  err,
			"params": backupVault,
		})
		return nil, ConvertToVSAError(fmt.Errorf("DeleteBackupVaultInSDE failed: %w", err))
	}

	if dbBackupVault.BackupVaultType == activities.CrossRegionBackupType && *dbBackupVault.BackupRegionName != "" {
		remoteParams := *bvCommonParams
		remoteParams.BackupRegion = dbBackupVault.BackupRegionName

		dbRemoteBackupVault := &datamodel.BackupVault{}
		err = workflow.ExecuteActivity(ctx, backupVaultActivity.DeleteRemoteBackupVaultInVCP, &remoteParams).Get(ctx, &dbRemoteBackupVault)
		if err != nil {
			wf.Logger.Error("Failed to delete remote backup vault in VCP", log.Fields{
				"error":  err,
				"params": backupVault,
			})
			return nil, ConvertToVSAError(fmt.Errorf("DeleteRemoteBackupVaultInVCP failed: %w", err))
		}
	}

	return dbBackupVault, nil
}
