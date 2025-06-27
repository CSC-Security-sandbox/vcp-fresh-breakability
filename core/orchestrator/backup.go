package orchestrator

import (
	"context"
	"database/sql"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

var (
	createBackup               = _createBackup
	validateCreateBackupParams = _validateCreateBackupParams
	getBackups                 = _getBackups
)

// CreateBackup creates the specified backup and adds it to the list of backup belonging to the specified BackupVault
func (o *Orchestrator) CreateBackup(ctx context.Context, params *common.CreateBackupParams) (*models.Backup, string, error) {
	return createBackup(ctx, o.storage, o.temporal, params)
}

func (o *Orchestrator) ListBackups(ctx context.Context, params *common.GetBackupsParams, filters [][]interface{}) ([]*datamodel.Backup, error) {
	return getBackups(ctx, o.storage, params, filters)
}

// GetBackupsUnderBackupVault retrieves all backups associated with the specified BackupVault
func (o *Orchestrator) GetBackupsUnderBackupVault(ctx context.Context, backupVaultID, ownerID string, backupUUIDs []string) ([]*datamodel.Backup, error) {
	se := o.storage
	account, err := getOrCreateAccount(ctx, se, ownerID)
	if err != nil {
		return nil, err
	}
	params := &common.GetBackupsParams{
		BackupVaultID: backupVaultID,
		AccountID:     account.ID,
	}
	conditions := [][]interface{}{{"uuid in ?", backupUUIDs}}
	return o.GetBackups(ctx, params, conditions)
}

func _createBackup(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateBackupParams) (*models.Backup, string, error) {
	logger := util.GetLogger(ctx)
	// Get the account
	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}
	err = validateCreateBackupParams(ctx, se, params)
	if err != nil {
		return nil, "", err
	}

	volume, err := se.GetVolumeWithAccountID(ctx, params.VolumeUUID, account.ID)
	if err != nil {
		return nil, "", err
	}

	backupVault, err := se.GetBackupVault(ctx, params.BackupVaultID)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			return nil, "", customerrors.NewUserInputValidationErr("Backup vault not found")
		}
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeCreateBackup),
		State:        string(models.JobsStateNEW),
		ResourceName: params.BackupName,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create backup create job in database", "error", err)
		return nil, "", err
	}

	backupAttributes := datamodel.BackupAttributes{
		VolumeName:        volume.Name,
		AccountIdentifier: account.Name,
		Protocols:         volume.VolumeAttributes.Protocols,
	}
	dbBackup := &datamodel.Backup{
		Name:          params.BackupName,
		VolumeUUID:    params.VolumeUUID,
		BackupVaultID: backupVault.ID,
		Attributes:    &backupAttributes,
		Description:   params.Description,
		Type:          params.BackupType,
	}
	dbBackup.State = models.LifeCycleStateCreating
	dbBackup.StateDetails = models.LifeCycleStateCreatingDetails

	dbBackup, err = se.CreateBackup(ctx, dbBackup)
	if err != nil {
		return nil, "", err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.CreateBackupWorkflow,
		params,
		dbBackup,
		backupVault,
		volume,
	)

	if err != nil {
		logger.Error("Failed to start create backup workflow: ", "error", err)
		return nil, "", err
	}

	return convertDatastoreBackupToModel(dbBackup), createdJob.UUID, nil
}

func _validateCreateBackupParams(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
	backupAlreadyCreating, err := se.IsBackupInCreatingorDeletingStateByVolume(ctx, params.VolumeUUID)
	if err != nil {
		return err
	}
	if backupAlreadyCreating {
		return customerrors.NewUserInputValidationErr("Already a backup is in creating state for selected volume")
	}
	vol, err := se.GetVolume(ctx, params.VolumeUUID)
	if err != nil {
		return err
	}
	if vol.State != models.LifeCycleStateREADY {
		return customerrors.NewUserInputValidationErr("Volume is not in available state")
	}
	if vol.DataProtection == nil {
		return customerrors.NewUserInputValidationErr("Volume does not have any backup vault associated with it")
	}
	if vol.DataProtection != nil && vol.DataProtection.BackupVaultID != params.BackupVaultID {
		return customerrors.NewUserInputValidationErr("Volume does not have the specified backup vault associated with it")
	}
	return nil
}

// GetBackups retrieves all backups associated with the specified BackupVault
func (o *Orchestrator) GetBackups(ctx context.Context, params *common.GetBackupsParams, filters [][]interface{}) ([]*datamodel.Backup, error) {
	return _getBackups(ctx, o.storage, params, filters)
}

func _getBackups(ctx context.Context, se database.Storage, params *common.GetBackupsParams, filters [][]interface{}) ([]*datamodel.Backup, error) {
	return se.GetBackupsByBackupVaultOwnerIDAndFilter(ctx, params.BackupVaultID, params.AccountID, filters)
}

func convertDatastoreBackupToModel(backup *datamodel.Backup) *models.Backup {
	return &models.Backup{
		BackupID:              backup.UUID,
		Name:                  backup.Name,
		VolumeID:              backup.VolumeUUID,
		Region:                *backup.BackupVault.SourceRegionName,
		VolumeName:            backup.Attributes.VolumeName,
		BackupVaultID:         backup.BackupVault.UUID,
		LifeCycleState:        backup.State,
		LifeCycleStateDetails: backup.StateDetails,
		Description:           &backup.Description,
		Type:                  backup.Type,
	}
}
