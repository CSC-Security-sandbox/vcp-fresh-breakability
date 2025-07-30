package orchestrator

import (
	"context"
	"database/sql"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
	deleteBackup               = _deleteBackup
	updateBackup               = _updateBackup
	validateBackupDeleteParams = _validateBackupDeleteParams
)

// CreateBackup creates the specified backup and adds it to the list of backup belonging to the specified BackupVault
func (o *Orchestrator) CreateBackup(ctx context.Context, params *common.CreateBackupParams) (*models.Backup, string, error) {
	return createBackup(ctx, o.storage, o.temporal, params)
}

func (o *Orchestrator) UpdateBackup(ctx context.Context, params *common.UpdateBackupParams) (*models.Backup, string, error) {
	return updateBackup(ctx, o.storage, o.temporal, params)
}

func (o *Orchestrator) ListBackups(ctx context.Context, backupVaultID, ownerID string, filters [][]interface{}) ([]*datamodel.Backup, error) {
	se := o.storage
	account, err := getOrCreateAccount(ctx, se, ownerID)
	if err != nil {
		return nil, err
	}
	params := &common.GetBackupsParams{
		BackupVaultID: backupVaultID,
		AccountID:     account.ID,
	}
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
	workflowStarted := false
	stateUpdated := false
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

	defer func() {
		if err != nil && !workflowStarted {
			// Only rollback if the state was successfully updated but workflow failed to start
			// The workflow will handle its own error states
			if stateUpdated {
				dbBackup.State = models.LifeCycleStateError
				dbBackup.StateDetails = err.Error()
				if _, rollbackErr := se.UpdateBackupState(ctx, dbBackup); rollbackErr != nil {
					logger.Error("Failed to make backup  state", "error", rollbackErr, "originalState", dbBackup.State, "backupUUID", dbBackup.UUID)
				}
			}

			// Mark job as error if it was created
			if createdJob != nil {
				if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
					logger.Error("Failed to update job state to ERROR", "error", jobErr, "jobUUID", createdJob.UUID)
				}
			}
		}
	}()

	dbBackup, err = se.CreateBackup(ctx, dbBackup)
	if err != nil {
		return nil, "", err
	}

	stateUpdated = true

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
	workflowStarted = true
	return convertDatastoreBackupToModel(dbBackup), createdJob.UUID, nil
}

func _updateBackup(ctx context.Context, se database.Storage, temporal client.Client, params *common.UpdateBackupParams) (*models.Backup, string, error) {
	logger := util.GetLogger(ctx)

	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}
	// Fetch the backup
	backup, err := se.GetBackup(ctx, params.BackupVaultUUID, params.BackupUUID, account.Name)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			return nil, "", customerrors.NewUserInputValidationErr("Backup not found")
		}
		return nil, "", err
	}
	// Check if the backup is in a state that allows updates
	if backup.State != models.LifeCycleStateAvailable {
		logger.Errorf("Backup %s cannot be updated, current state: %s. Only backups in AVAILABLE state can be updated", params.BackupUUID, backup.State)
		return nil, "", customerrors.NewUserInputValidationErr("Backup can only be updated when in AVAILABLE state, current state: " + backup.State)
	}

	stateUpdated := false
	workflowStarted := false
	originalState := backup.State
	originalStateDetails := backup.StateDetails

	// Update backup state
	backup.State = models.LifeCycleStateUpdating
	backup.StateDetails = models.LifeCycleStateUpdatingDetails

	// Create a job for the update operation
	job := &datamodel.Job{
		Type:         string(models.JobTypeUpdateBackup),
		State:        string(models.JobsStateNEW),
		ResourceName: params.BackupUUID,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	defer func() {
		if err != nil && !workflowStarted {
			// Only rollback if the state was successfully updated but workflow failed to start
			// The workflow will handle its own error states
			if stateUpdated {
				backup.State = originalState
				backup.StateDetails = originalStateDetails
				if _, rollbackErr := se.UpdateBackupState(ctx, backup); rollbackErr != nil {
					logger.Error("Failed to rollback backup  state", "error", rollbackErr, "originalState", originalState)
				}
			}

			// Mark job as error if it was created
			if createdJob != nil {
				if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
					logger.Error("Failed to update job state to ERROR", "error", jobErr, "jobUUID", createdJob.UUID)
				}
			}
		}
	}()

	backup, err = se.UpdateBackupState(ctx, backup)
	if err != nil {
		logger.Error("Failed to update backup state in database", "error", err)
		return nil, "", err
	}
	stateUpdated = true
	backup.Description = params.Description

	// Execute the workflow for updating the backup
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.UpdateBackupWorkflow,
		backup,
	)

	if err != nil {
		logger.Error("Failed to start update backup workflow: ", "error", err)
		return nil, "", err
	}
	workflowStarted = true
	return convertDatastoreBackupToModel(backup), createdJob.UUID, nil
}

func _validateCreateBackupParams(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
	backupInTransition, err := se.IsBackupInCreatingorDeletingStateByVolume(ctx, params.VolumeUUID)
	if err != nil {
		return err
	}
	if backupInTransition {
		return customerrors.NewUserInputValidationErr("A backup operation from the same volume is currently in progress. Please wait for it to complete before starting a new backup")
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

// GetBackup retrieves the backup associated with the specified BackupVault uuid and backup uuid and account name
func (o *Orchestrator) GetBackup(ctx context.Context, params *common.GetBackupParams) (*datamodel.Backup, error) {
	return _getBackup(ctx, o.storage, params)
}

func _getBackup(ctx context.Context, se database.Storage, params *common.GetBackupParams) (*datamodel.Backup, error) {
	return se.GetBackup(ctx, params.BackupVaultID, params.BackupUUID, params.AccountName)
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

func (o *Orchestrator) DeleteBackup(ctx context.Context, params *common.DeleteBackupParams) (*models.BaseModel, string, error) {
	return deleteBackup(ctx, o.storage, o.temporal, params)
}

func _deleteBackup(ctx context.Context, se database.Storage, temporal client.Client, params *common.DeleteBackupParams) (*models.BaseModel, string, error) {
	logger := util.GetLogger(ctx)

	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	err = validateBackupDeleteParams(ctx, se, params)
	if err != nil {
		return nil, "", err
	}

	backup, err := se.GetBackup(ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	// Check whether any volume restore is in progress for this backup
	conditions := [][]interface{}{{"volume_attributes->>'restored_backup_id' = ?", backup.UUID}, {"state = ?", models.LifeCycleStateRestoring}}
	volumes, err := se.ListVolumes(ctx, conditions)
	if err != nil {
		return nil, "", err
	}
	if len(volumes) > 0 {
		return nil, "", customerrors.NewUserInputValidationErr("Cannot delete backup as restore is in progress for this backup")
	}

	originalState := backup.State
	originalStateDetails := backup.StateDetails
	workflowStarted := false
	stateUpdated := false

	backup.State = models.LifeCycleStateDeleting
	backup.StateDetails = models.LifeCycleStateDeletingDetails

	job := &datamodel.Job{
		Type:         string(models.JobTypeDeleteBackup),
		State:        string(models.JobsStateNEW),
		ResourceName: params.BackupUUID,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	defer func() {
		if err != nil && !workflowStarted {
			// Only rollback if the state was successfully updated but workflow failed to start
			// The workflow will handle its own error states
			if stateUpdated {
				backup.State = originalState
				backup.StateDetails = originalStateDetails
				if _, rollbackErr := se.UpdateBackupState(ctx, backup); rollbackErr != nil {
					logger.Error("Failed to rollback backup  state", "error", rollbackErr, "originalState", originalState)
				}
			}

			// Mark job as error if it was created
			if createdJob != nil {
				if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
					logger.Error("Failed to update job state to ERROR", "error", jobErr, "jobUUID", createdJob.UUID)
				}
			}
		}
	}()

	_, err = se.UpdateBackupState(ctx, backup)
	if err != nil {
		logger.Error("Failed to change backup state in database", "error", err)
		return nil, "", err
	}
	stateUpdated = true
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		workflows.DeleteBackupWorkflow,
		params,
	)

	if err != nil {
		logger.Error("Failed to start delete backup workflow: ", "error", err)
		return nil, "", err
	}
	workflowStarted = true
	return nil, createdJob.UUID, nil
}

func _validateBackupDeleteParams(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
	backup, err := se.GetBackup(ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			return customerrors.NewUserInputValidationErr("Backup not found")
		}
		return err
	}
	// Check if any backup for the same volume is in transition state (CREATING or DELETING)
	backupInTransition, err := se.IsBackupInCreatingorDeletingStateByVolume(ctx, backup.VolumeUUID)
	if err != nil {
		return err
	}
	if backupInTransition {
		return customerrors.NewUserInputValidationErr("A backup operation from the same volume is currently in progress. Please wait for it to complete before starting a new backup")
	}

	// check if backup is latest
	isLatest, err := se.IsLatestBackup(ctx, backup.UUID, backup.VolumeUUID)
	if err != nil {
		return err
	}

	// get count of backups under the volume
	count, err := se.BackupCountByVolumeID(ctx, backup.VolumeUUID)
	if err != nil {
		return err
	}

	if isLatest && count != 1 {
		return customerrors.NewUserInputValidationErr("Cannot delete latest backup")
	}
	return nil
}
