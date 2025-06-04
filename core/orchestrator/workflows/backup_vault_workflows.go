package workflows

import (
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type backupVaultWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

var _ WorkflowInterface = &backupVaultWorkflow{}

func CreateBackupVault(ctx workflow.Context, params *common.BackupVaultParams, bvParams *datamodel.BackupVault, gcpParams gcpgenserver.V1betaCreateBackupVaultParams) (gcpgenserver.V1betaCreateBackupVaultRes, error) {
	bvWF := new(backupVaultWorkflow)
	err := bvWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	bvWF.Status = WorkflowStatusRunning
	err = bvWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = bvWF.Run(ctx, bvParams, gcpParams)
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

// Setup CreateBackupVaultWorkflow process pool related requests from a customer.
func (wf *backupVaultWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	BackupVaultParams := input.(*common.BackupVaultParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = BackupVaultParams.AccountName
	wf.Status = "created"
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

func (wf *backupVaultWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	bvParams := args[0].(*datamodel.BackupVault)
	gcpParams := args[1].(gcpgenserver.V1betaCreateBackupVaultParams)
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

	dbBvSDE := &datamodel.BackupVault{}
	bv := workflow.ExecuteActivity(ctx, backupVaultActivity.CreateBackupVaultInSDE, bvParams, gcpParams)
	err = bv.Get(ctx, &dbBvSDE)
	if err != nil {
		wf.Logger.Error("Failed to create backup vault in SDE", log.Fields{
			"error":  err,
			"params": bvParams,
		})
		return nil, fmt.Errorf("CreateBackupVaultInSDE failed: %w", err)
	}

	dbBackupVault := &datamodel.BackupVault{}
	future := workflow.ExecuteActivity(ctx, backupVaultActivity.CreateBackupVaultInVCP, &dbBvSDE, bvParams, gcpParams)
	err = future.Get(ctx, &dbBackupVault)
	if err != nil {
		return nil, err
	}

	return dbBackupVault, nil
}
