package gcp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	expertModeWorkflows "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/expertMode"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

const (
	ExpertModeVolumeStyleFlexgroup = "flexgroup"
	APIAccessModeONTAP             = "ONTAP"
)

// expertModeVolumeToVolumeForSFR builds a partial *datamodel.Volume from expert mode volume for use with RestoreFilesFromBackupWorkflow.
// Same field mapping as convertExpertModeVolumeToVolume in workflows/expertMode (VolumeAttributes has no FileProperties/BlockProperties).
func expertModeVolumeToVolumeForSFR(emv *datamodel.ExpertModeVolumes) *datamodel.Volume {
	volumeAttributes := &datamodel.VolumeAttributes{ExternalUUID: emv.ExternalUUID}
	return &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: emv.UUID},
		Name:             emv.Name,
		Description:      emv.Description,
		State:            emv.State,
		SizeInBytes:      emv.SizeInBytes,
		AccountID:        emv.AccountID,
		PoolID:           emv.PoolID,
		SvmID:            emv.SvmID,
		Account:          emv.Account,
		Pool:             emv.Pool,
		Svm:              emv.Svm,
		VolumeAttributes: volumeAttributes,
		DataProtection:   emv.BackupConfig,
	}
}

// RestoreOntapModeBackup restores an expert mode volume from backup by starting RestoreForOntapModeVolumeWorkflow.
func (o *GCPOrchestrator) RestoreOntapModeBackup(ctx context.Context, params *commonparams.RestoreOntapModeBackupParams) (string, error) {
	return restoreOntapModeBackup(ctx, o.storage, o.temporal, params)
}

// SFROntapModeBackup performs selective file restore for an expert mode volume from backup (when SourceFileList is set).
func (o *GCPOrchestrator) SFROntapModeBackup(ctx context.Context, params *commonparams.RestoreOntapModeBackupParams) (string, error) {
	return sfrOntapModeBackup(ctx, o.storage, o.temporal, params)
}

// sfrOntapModeBackup runs the selective file restore path for ontap mode: expert-mode validation + backup resolution + RestoreFilesFromBackupWorkflow.
func sfrOntapModeBackup(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.RestoreOntapModeBackupParams) (string, error) {
	logger := util.GetLogger(ctx)

	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return "", err
	}

	if params.PoolID == "" {
		return "", customerrors.NewUserInputValidationErr("PoolID is required for expert mode restore")
	}

	poolView, err := se.DescribePool(ctx, params.PoolID, account.ID)
	if err != nil {
		return "", err
	}
	if poolView.APIAccessMode != commonparams.ONTAPMode {
		return "", customerrors.NewUserInputValidationErr("Pool is not an expert mode (ONTAP) pool")
	}

	expertModeVolume, err := se.GetExpertModeVolumeByExternalUUID(ctx, params.VolumeUUID)
	if err != nil {
		return "", err
	}

	// Check runs at restore start only. If a previous restore failed and left state in ERROR/RESTORING,
	// this check fails and blocks starting a new restore until state is corrected (e.g. by defer in SFR workflow).
	if expertModeVolume.State != models.LifeCycleStateREADY {
		return "", customerrors.NewUserInputValidationErr("Volume is not available")
	}

	if params.BackupPath == "" {
		return "", customerrors.NewUserInputValidationErr("BackupPath must be provided")
	}

	components := strings.Split(params.BackupPath, "/")
	if len(components) < MaxBackupPathComponents {
		return "", customerrors.NewUserInputValidationErr("Backup path is not in correct format")
	}

	backupRegion := components[LocationIdIndex]
	location, err := utils.GetLocationFromVendorID(expertModeVolume.Pool.VendorID)
	if err != nil {
		logger.Error("Failed to get location from vendor ID: ", "error", err)
		return "", err
	}
	volumeRegion, _, err := utils.ParseRegionAndZone(location)
	if err != nil {
		return "", err
	}

	var backupVault *datamodel.BackupVault
	backupVaultName := components[BackupVaultNameIndex]
	if backupRegion != volumeRegion {
		backupVaultPath := strings.Join(components[:BackupVaultNameIndex+1], "/")
		backupVault, err = se.GetBackupVaultByCrossRegionBackupVaultName(ctx, backupVaultPath, account.ID)
		if err != nil {
			return "", err
		}
	} else {
		backupVault, err = se.GetBackupVaultByNameAndOwnerID(ctx, backupVaultName, strconv.FormatInt(account.ID, 10))
		if err != nil {
			return "", err
		}
	}

	backupName := components[BackupNameIndex]
	backup, err := se.GetBackupByNameAndBackupVaultID(ctx, backupName, backupVault.ID)
	if err != nil {
		return "", err
	}

	if backup.State != models.LifeCycleStateAvailable {
		return "", customerrors.NewUserInputValidationErr("Cannot restore files from backup which is not available")
	}

	originalState := expertModeVolume.State
	stateUpdated := false

	job := &datamodel.Job{
		Type:          string(models.JobTypeRestoreFilesBackup),
		State:         string(models.JobsStateNEW),
		ResourceName:  expertModeVolume.UUID,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create restore files from backup job in database", "error", err)
		return "", err
	}

	defer func() {
		if err != nil {
			if stateUpdated {
				if rollbackErr := se.UpdateExpertModeVolumeFields(ctx, expertModeVolume.ExternalUUID, map[string]interface{}{
					"state": originalState,
				}); rollbackErr != nil {
					logger.Error("Failed to rollback expert mode volume state", "error", rollbackErr, "originalState", originalState)
				}
			}
			if createdJob != nil {
				if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
					logger.Error("Failed to update job state to ERROR", "error", jobErr, "jobUUID", createdJob.UUID)
				}
			}
		}
	}()

	err = se.UpdateExpertModeVolumeFields(ctx, expertModeVolume.ExternalUUID, map[string]interface{}{
		"state": models.LifeCycleStateRestoring,
	})
	if err != nil {
		logger.Error("Failed to update expert mode volume state to restoring", "error", err)
		return "", err
	}
	stateUpdated = true

	sfrParams := &commonparams.RestoreFilesFromBackupParams{
		AccountName:         params.AccountName,
		BackupPath:          params.BackupPath,
		SourceFileList:      params.SourceFileList,
		RestoreFilePath:     params.RestoreFilePath,
		VolumeUUID:          params.VolumeUUID,
		Region:              params.Region,
		PoolID:              params.PoolID,
		IsExpertModeRestore: true,
	}
	volumeForWorkflow := expertModeVolumeToVolumeForSFR(expertModeVolume)

	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.RestoreFilesFromBackupWorkflow,
		workflowengine.GetSFRWorkflowTimeout(),
		sfrParams,
		backup,
		volumeForWorkflow,
	)
	if err != nil {
		logger.Error("Failed to start restore files from backup workflow after retries: ", "error", err)
		return "", err
	}
	return createdJob.UUID, nil
}

func restoreOntapModeBackup(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.RestoreOntapModeBackupParams) (string, error) {
	logger := util.GetLogger(ctx)

	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return "", err
	}

	if params.PoolID == "" {
		return "", customerrors.NewUserInputValidationErr("PoolID is required for expert mode restore")
	}

	poolView, err := se.DescribePool(ctx, params.PoolID, account.ID)
	if err != nil {
		return "", err
	}
	if poolView.APIAccessMode != commonparams.ONTAPMode {
		return "", customerrors.NewUserInputValidationErr("Pool is not an expert mode (ONTAP) pool")
	}

	expertModeVolume, err := se.GetExpertModeVolumeByExternalUUID(ctx, params.VolumeUUID)
	if err != nil {
		return "", err
	}

	if expertModeVolume.State != models.LifeCycleStateREADY {
		return "", customerrors.NewUserInputValidationErr("Volume is not available")
	}

	volumeRegion := params.Region
	originalState := expertModeVolume.State
	stateUpdated := false

	job := &datamodel.Job{
		Type:          string(models.JobTypeRestoreOntapModeBackup),
		State:         string(models.JobsStateNEW),
		ResourceName:  expertModeVolume.UUID,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create restore from backup job in database", "error", err)
		return "", err
	}

	defer func() {
		if err != nil {
			if stateUpdated {
				if rollbackErr := se.UpdateExpertModeVolumeFields(ctx, expertModeVolume.ExternalUUID, map[string]interface{}{
					"state": originalState,
				}); rollbackErr != nil {
					logger.Error("Failed to rollback expert mode volume state", "error", rollbackErr, "originalState", originalState)
				}
			}
			if createdJob != nil {
				if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
					logger.Error("Failed to update job state to ERROR", "error", jobErr, "jobUUID", createdJob.UUID)
				}
			}
		}
	}()

	err = se.UpdateExpertModeVolumeFields(ctx, expertModeVolume.ExternalUUID, map[string]interface{}{
		"state": models.LifeCycleStateRestoring,
	})
	if err != nil {
		logger.Error("Failed to update expert mode volume state to restoring", "error", err)
		return "", err
	}
	stateUpdated = true

	restoreParams := &commonparams.RestoreForOntapModeParams{
		AccountName:      params.AccountName,
		BackupPath:       params.BackupPath,
		Region:           volumeRegion,
		ExpertModeVolume: expertModeVolume,
	}

	// Start Temporal workflow for restore
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    *workflowengine.GetCreateBackupWorkflowTimeout(),
		},
		expertModeWorkflows.RestoreForOntapModeVolumeWorkflow,
		restoreParams,
	)
	if err != nil {
		logger.Error("Failed to start restore workflow", "error", err)
		return "", err
	}
	return createdJob.UUID, nil
}

// CreateExpertModeVolume creates a new expert mode volume
func (o *GCPOrchestrator) CreateExpertModeVolume(ctx context.Context, params *commonparams.ExpertModeVolumeParams) error {
	return _createExpertModeVolume(ctx, o.storage, o.temporal, params)
}

// createExpertModeVolume creates a new expert mode volume and triggers reconciliation workflow
func _createExpertModeVolume(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.ExpertModeVolumeParams) error {
	logger := util.GetLogger(ctx)

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return err
	}

	// Get the pool by ID
	dbPoolView, err := se.GetPool(ctx, params.PoolUUID, account.ID)
	if err != nil {
		logger.Error("Failed to get pool by UUID", "poolUUID", params.PoolUUID, "error", err)
		return err
	}

	volumeName := params.VolumeName
	existingVolume, err := se.GetExpertModeVolumeByNameAndPoolID(ctx, volumeName, dbPoolView.ID)
	if err != nil {
		// If the error is NOT "record not found", it's a real database error
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			logger.Error("Failed to check for existing volume", "volumeName", volumeName, "poolID", dbPoolView.ID, "error", err)
			return err
		}
	} else if existingVolume != nil {
		logger.Error("Volume with same name already exists in pool",
			"volumeName", volumeName,
			"poolID", dbPoolView.ID)
		return customerrors.NewBadRequestErr(fmt.Sprintf("a volume named '%s' already exists in this pool; if the previous volume was deleted or creation failed, wait at least 180 seconds before reusing the name", volumeName))
	}

	err = canFitInPool(ctx, se, dbPoolView.ID, dbPoolView.SizeInBytes, params.SizeInBytes)
	if err != nil {
		return err
	}

	// Create expert mode volume record
	expertModeVolume := &datamodel.ExpertModeVolumes{
		Name:        params.VolumeName,
		SizeInBytes: params.SizeInBytes,
		PoolID:      dbPoolView.ID,
		AccountID:   dbPoolView.AccountID,
		Style:       params.Style,
		State:       models.LifeCycleStateCreating,
	}

	// Look up SVM based on provided parameters
	var svm *datamodel.Svm
	if params.SvmUuid != "" {
		// If svmUUID is provided, fetch SVM by external UUID and validate it belongs to the pool
		svm, err = se.GetSvmByExternalUUID(ctx, params.SvmUuid, dbPoolView.ID)
		if err != nil {
			logger.Error("Failed to find SVM by external UUID", "svmUuid", params.SvmUuid, "error", err)
			if customerrors.IsNotFoundErr(err) {
				return customerrors.NewBadRequestErr(fmt.Sprintf("SVM with UUID '%s' not found in pool", params.SvmUuid))
			}
			return err
		}
		expertModeVolume.SvmID = svm.ID
	} else if params.SvmName != "" {
		// If svmName is provided, fetch SVM by name and poolID
		svm, err = se.GetSvmByNameAndPoolID(ctx, params.SvmName, dbPoolView.ID)
		if err != nil {
			logger.Error("Failed to find SVM by name and poolID", "svmName", params.SvmName, "poolID", dbPoolView.ID, "error", err)
			if customerrors.IsNotFoundErr(err) {
				return customerrors.NewBadRequestErr(fmt.Sprintf("SVM with name '%s' not found in pool", params.SvmName))
			}
			return err
		}
		expertModeVolume.SvmID = svm.ID
	} else {
		// Neither svmUUID nor svmName is provided
		logger.Error("Neither svmName nor svmUUID has been passed")
		return customerrors.NewBadRequestErr("neither svmName nor svmUUID has been passed")
	}

	createdVolume, err := se.CreateExpertModeVolume(ctx, expertModeVolume)
	if err != nil {
		logger.Error("Failed to create expert mode volume", "error", err)
		return err
	}

	volume, err := se.GetExpertModeVolumeByUUID(ctx, createdVolume.UUID)
	if err != nil {
		logger.Error("Failed to get expert mode volume with preloads", "volumeUUID", createdVolume.UUID, "error", err)
		return err
	}

	// Create a job for the volume creation workflow
	correlationID := utils.GetCoRelationIDFromContext(ctx)
	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateExpertModeVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  createdVolume.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: createdVolume.UUID, PoolUUID: dbPoolView.UUID},
		CorrelationID: correlationID,
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job for expert mode volume", "error", err)
		return err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), createdJob.TrackingID, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.BackgroundTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetExpertModeSyncWorkflowTimeout(),
		},
		expertModeWorkflows.VolumeCreateReconciliationWorkflow,
		volume,
	)

	if err != nil {
		logger.Error("Failed to start volume reconciliation workflow", "workflowID", createdJob.WorkflowID, "error", err)
		return err
	}
	if enableAutoPoolScaling {
		dbPool := database.ConvertPoolViewToPool(dbPoolView)
		logger.Infof("Triggering pool scaling for ONTAP mode pool %s after volume creation", dbPool.Name)
		checkAndTriggerPoolScalingIfNeeded(ctx, se, temporal, dbPool, false)
	}
	return nil
}

// returns error if the new volume cannot fit in the pool
func canFitInPool(ctx context.Context, se database.Storage, poolID, poolSizeInBytes, newVolumeSizeToAdd int64) error {
	logger := util.GetLogger(ctx)
	if newVolumeSizeToAdd <= 0 {
		logger.Error("Volume size must be greater than 0")
		return customerrors.NewBadRequestErr("volume size must be greater than 0")
	}

	// Calculate the total existing size to validate pool capacity
	capacity, err := se.GetExpertModePoolUsedCapacityAndVolumeCount(ctx, poolID)
	if err != nil {
		logger.Error("Failed to calculate total existing size", "poolID", poolID, "error", err)
		return err
	}

	consumedSizeOfPool := capacity.TotalSize
	// Check if the new volume can fit in the pool
	if consumedSizeOfPool+newVolumeSizeToAdd > int64(poolSizeInBytes) {
		logger.Error("Insufficient pool capacity", "poolID", poolID, "requestedSize", newVolumeSizeToAdd, "availableSize", int64(poolSizeInBytes)-consumedSizeOfPool)
		return customerrors.NewBadRequestErr(fmt.Sprintf("insufficient pool capacity: requested %d bytes, available %d bytes",
			newVolumeSizeToAdd, int64(poolSizeInBytes)-consumedSizeOfPool))
	}
	return nil
}

// DeleteExpertModeVolume deletes an expert mode volume
func (o *GCPOrchestrator) DeleteExpertModeVolume(ctx context.Context, params *commonparams.ExpertModeVolumeParams) error {
	return _deleteExpertModeVolume(ctx, o.storage, o.temporal, params)
}

// _deleteExpertModeVolume deletes an expert mode volume and triggers reconciliation workflow
func _deleteExpertModeVolume(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.ExpertModeVolumeParams) error {
	logger := util.GetLogger(ctx)

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return err
	}

	dbPoolView, err := se.GetPool(ctx, params.PoolUUID, account.ID)
	if err != nil {
		logger.Error("Failed to get pool", "poolUUID", params.PoolUUID, "error", err)
		return err
	}

	volume, err := getExpertModeVolume(ctx, se, params, dbPoolView)
	if err != nil {
		return err
	}

	if params.PoolUUID != volume.Pool.UUID {
		logger.Error("Volume is not associated to the pool for delete operation", "volumeUUID", volume.ExternalUUID, "poolUUID", params.PoolUUID)
		return customerrors.NewBadRequestErr("volume is not associated to the specified pool for delete operation")
	}

	// Check if volume is already deleted
	if volume.State == models.LifeCycleStateDeleted {
		return nil
	}

	previousState := volume.State
	volume.State = models.LifeCycleStateDeleting
	_, err = se.UpdateExpertModeVolume(ctx, volume)
	if err != nil {
		logger.Error("Failed to update volume state to DELETING", "volumeUUID", volume.UUID, "error", err)
		return err
	}

	var volumeMarkedAsDeleting bool = true

	// Create a job for the volume deletion workflow
	correlationID := utils.GetCoRelationIDFromContext(ctx)
	job := &datamodel.Job{
		Type:          string(models.JobTypeDeleteExpertModeVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  volume.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: volume.UUID, PoolUUID: volume.Pool.UUID},
		CorrelationID: correlationID,
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job for expert mode volume deletion", "error", err)
		return err
	}

	// Defer statement to mark job as errored and revert volume state if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), createdJob.TrackingID, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
			// Revert volume state only if it was successfully marked as deleting
			if volumeMarkedAsDeleting {
				volume.State = previousState
				if _, revertErr := se.UpdateExpertModeVolume(ctx, volume); revertErr != nil {
					logger.Error("Failed to revert volume state", "volumeUUID", volume.UUID, "previousState", previousState, "error", revertErr)
				}
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.BackgroundTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetExpertModeSyncWorkflowTimeout(),
		},
		expertModeWorkflows.VolumeDeleteReconciliationWorkflow,
		volume,
	)

	if err != nil {
		logger.Error("Failed to start volume deletion reconciliation workflow", "workflowID", createdJob.WorkflowID, "error", err)
		return err
	}
	if enableAutoPoolScaling {
		dbPool := database.ConvertPoolViewToPool(dbPoolView)
		logger.Infof("Triggering pool scaling for ONTAP mode pool %s after volume deletion", dbPool.Name)
		checkAndTriggerPoolScalingIfNeeded(ctx, se, temporal, dbPool, true)
	}
	return nil
}

// GetExpertModeVolumeByExternalUUID retrieves an expert mode volume by its UUID
func (o *GCPOrchestrator) GetExpertModeVolumeByExternalUUID(ctx context.Context, volumeUUID string) (*datamodel.ExpertModeVolumes, error) {
	return o.storage.GetExpertModeVolumeByExternalUUID(ctx, volumeUUID)
}

// UpdateExpertModeVolume updates an existing expert mode volume
func (o *GCPOrchestrator) UpdateExpertModeVolume(ctx context.Context, params *commonparams.ExpertModeVolumeParams) error {
	return _updateExpertModeVolume(ctx, o.storage, o.temporal, params)
}

func validateUpdateParams(ctx context.Context, se database.Storage, params *commonparams.ExpertModeVolumeParams, volume *datamodel.ExpertModeVolumes) error {
	logger := util.GetLogger(ctx)

	if params.PoolUUID != volume.Pool.UUID {
		logger.Error("Volume is not associated to the pool for update operation", "volumeUUID", volume.ExternalUUID, "params.PoolUUID", params.PoolUUID, "volume.Pool.UUID", volume.Pool.UUID)
		return customerrors.NewBadRequestErr("volume is not associated to the pool for update operation")
	}
	if params.SizeInBytes < 0 {
		logger.Error("Volume size must be greater than or equal to 0", "volumeSize", params.SizeInBytes)
		return customerrors.NewBadRequestErr("Volume size must be greater than or equal to 0")
	}

	if volume.State == models.LifeCycleStateDeleted || volume.State == models.LifeCycleStateError {
		logger.Error("Volume is deleted, cannot update", "volumeUUID", volume.ExternalUUID)
		return customerrors.NewBadRequestErr(fmt.Sprintf("volume with UUID '%s' is deleted", volume.ExternalUUID))
	}

	if volume.State == models.LifeCycleStateCreating || volume.State == models.LifeCycleStateDeleting || volume.State == models.LifeCycleStateUpdating {
		logger.Error("Volume is in a transitional state and cannot be updated", "volumeUUID", volume.ExternalUUID, "state", volume.State)
		return customerrors.NewBadRequestErr(fmt.Sprintf("volume with UUID '%s' is in a transitional state and cannot be updated", volume.ExternalUUID))
	}

	if params.VolumeName != "" {
		poolID := volume.PoolID
		// Check if another volume with the same name exists in the same pool
		existingVolume, err := se.GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, poolID)
		if err != nil {
			// If the error is NOT "record not found", it's a real database error
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				logger.Error("Failed to check for existing volume", "volumeName", params.VolumeName, "poolID", poolID, "error", err)
				return err
			}
		} else if existingVolume != nil {
			// If a volume with the same name exists and it's not the same volume being updated, return an error
			if existingVolume.ExternalUUID != volume.ExternalUUID {
				logger.Error("Volume with same name already exists in pool",
					"volumeName", params.VolumeName,
					"poolID", poolID,
					"existingVolumeExternalUUID", existingVolume.ExternalUUID)
				return customerrors.NewBadRequestErr(fmt.Sprintf("volume with name '%s' already exists in pool", params.VolumeName))
			}
		}
	}
	return nil
}

// _updateExpertModeVolume updates an expert mode volume and updates it in the DB
func _updateExpertModeVolume(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.ExpertModeVolumeParams) error {
	logger := util.GetLogger(ctx)

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return err
	}

	dbPoolView, err := se.GetPool(ctx, params.PoolUUID, account.ID)
	if err != nil {
		logger.Error("Failed to get pool", "poolUUID", params.PoolUUID, "error", err)
		return err
	}

	volume, err := getExpertModeVolume(ctx, se, params, dbPoolView)
	if err != nil {
		return err
	}

	err = validateUpdateParams(ctx, se, params, volume)
	if err != nil {
		return err
	}

	// Validate account matches - use AccountID directly as it's always set
	if volume.AccountID != account.ID {
		logger.Error("Volume does not belong to the specified account", "volumeUUID", volume.ExternalUUID, "volumeAccountID", volume.AccountID, "accountID", account.ID)
		return customerrors.NewBadRequestErr("volume does not belong to the specified account")
	}

	// Create a deep copy of the volume before modifying it
	// This preserves the original values for the workflow's error handling
	oldVolumeCopy := *volume
	oldVolume := &oldVolumeCopy
	var previousSize int64 = volume.SizeInBytes
	// Validate size if provided
	if params.SizeInBytes > 0 {
		// Calculate size increase
		sizeIncrease := params.SizeInBytes - volume.SizeInBytes
		if sizeIncrease > 0 {
			// Check pool capacity if size is being increased
			err = canFitInPool(ctx, se, volume.Pool.ID, volume.Pool.SizeInBytes, sizeIncrease)
			if err != nil {
				return err
			}
		}
		// Update size only if provided and > 0
		volume.SizeInBytes = params.SizeInBytes
	}

	if params.VolumeName != "" {
		volume.Name = params.VolumeName
	}

	previousState := volume.State
	volume.State = models.LifeCycleStateUpdating

	volumeMarkedAsUpdating := true

	// Update volume in DB
	_, err = se.UpdateExpertModeVolume(ctx, volume)
	if err != nil {
		logger.Error("Failed to update volume state to UPDATING and size", "volumeUUID", volume.UUID, "error", err)
		return err
	}

	// Defer statement to mark job as errored and revert volume state if any operation fails
	// Registered early to ensure it executes even if CreateJob or ExecuteWorkflow fails
	var createdJob *datamodel.Job
	defer func() {
		if err != nil {
			if createdJob != nil {
				if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), createdJob.TrackingID, err.Error()); jobErr != nil {
					logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
				}
			}
			// Revert volume state only if it was successfully marked as updating
			if volumeMarkedAsUpdating {
				volume.State = previousState
				volume.SizeInBytes = previousSize
				if _, revertErr := se.UpdateExpertModeVolume(ctx, volume); revertErr != nil {
					logger.Error("Failed to revert volume state", "volumeUUID", volume.UUID, "previousState", previousState, "error", revertErr)
				}
			}
		}
	}()

	// Create a job for the volume update workflow
	correlationID := utils.GetCoRelationIDFromContext(ctx)
	job := &datamodel.Job{
		Type:          string(models.JobTypeUpdateExpertModeVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  volume.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: volume.UUID, PoolUUID: volume.Pool.UUID},
		CorrelationID: correlationID,
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err = se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job for expert mode volume update", "error", err)
		// Defer will handle the volume state revert
		return err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.BackgroundTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetExpertModeSyncWorkflowTimeout(),
		},
		expertModeWorkflows.VolumeUpdateReconciliationWorkflow,
		volume, oldVolume,
	)

	if err != nil {
		logger.Error("Failed to start volume update reconciliation workflow", "workflowID", createdJob.WorkflowID, "error", err)
		return err
	}

	return nil
}

// GetExpertModeVolumeByUUID retrieves an expert mode volume by its UUID
func (o *GCPOrchestrator) GetExpertModeVolumeByUUID(ctx context.Context, volumeUUID string) (*datamodel.ExpertModeVolumes, error) {
	return o.storage.GetExpertModeVolumeByUUID(ctx, volumeUUID)
}

// GetBackupConfigsForPool retrieves backup configurations for all expert mode volumes in a pool
func (o *GCPOrchestrator) GetBackupConfigsForPool(ctx context.Context, poolID string, accountName string, locationId string) ([]*models.ExpertModeVolumeBackupConfig, error) {
	logger := util.GetLogger(ctx)

	// Get account
	account, err := getAccountWithName(ctx, o.storage, accountName)
	if err != nil {
		return nil, err
	}

	// Get pool to validate it exists and get the internal ID
	dbPoolView, err := o.storage.GetPool(ctx, poolID, account.ID)
	if err != nil {
		logger.Error("Failed to get pool", "poolID", poolID, "error", err)
		return nil, err
	}

	if dbPoolView.APIAccessMode != APIAccessModeONTAP {
		logger.Error("Backup configurations are only available for ONTAP-mode pools", "poolID", poolID, "apiAccessMode", dbPoolView.APIAccessMode)
		return nil, customerrors.NewBadRequestErr("backup configurations are only available for ONTAP pools")
	}

	// Get all expert mode volumes for this pool
	expertModeVolumes, err := o.storage.ListExpertModeVolumesByPoolID(ctx, dbPoolView.ID)
	if err != nil {
		logger.Error("Failed to list expert mode volumes", "poolID", dbPoolView.ID, "error", err)
		return nil, err
	}

	backupVaults, err := o.storage.ListBackupVaults(ctx, account.ID)
	if err != nil {
		logger.Error("Failed to list backup vaults", "accountID", account.ID, "error", err)
		return nil, err
	}
	vaultsByUUID := make(map[string]string, len(backupVaults))
	for _, bv := range backupVaults {
		vaultsByUUID[bv.UUID] = bv.Name
	}

	conditions := [][]interface{}{{"account_id = ?", account.ID}}
	backupPolicies, err := o.storage.ListBackupPolicies(ctx, conditions)
	if err != nil {
		logger.Error("Failed to list backup policies", "accountID", account.ID, "error", err)
		return nil, err
	}
	policiesByUUID := make(map[string]string, len(backupPolicies))
	for _, bp := range backupPolicies {
		policiesByUUID[bp.UUID] = bp.Name
	}

	backupConfigs := make([]*models.ExpertModeVolumeBackupConfig, 0, len(expertModeVolumes))
	for _, vol := range expertModeVolumes {
		config := &models.ExpertModeVolumeBackupConfig{
			VolumeResourceID: vol.Name,
		}

		if vol.BackupConfig != nil && vol.BackupConfig.BackupVaultID != "" {
			if vaultName, ok := vaultsByUUID[vol.BackupConfig.BackupVaultID]; ok {
				path := fmt.Sprintf("projects/%s/locations/%s/backupVaults/%s", accountName, locationId, vaultName)
				config.BackupVaultPath = &path
			} else {
				logger.Error("Backup vault not found for volume", "volumeName", vol.Name, "backupVaultID", vol.BackupConfig.BackupVaultID)
			}
		}

		if vol.BackupConfig != nil && vol.BackupConfig.BackupPolicyID != "" {
			if policyName, ok := policiesByUUID[vol.BackupConfig.BackupPolicyID]; ok {
				path := fmt.Sprintf("projects/%s/locations/%s/backupPolicies/%s", accountName, locationId, policyName)
				config.BackupPolicyPath = &path
			} else {
				logger.Error("Backup policy not found for volume", "volumeName", vol.Name, "backupPolicyID", vol.BackupConfig.BackupPolicyID)
			}
		}

		backupConfigs = append(backupConfigs, config)
	}

	return backupConfigs, nil
}

// RenameExpertModeVolume renames an expert mode volume after validating pool, SVM name, and new name uniqueness; then triggers the update workflow.
func (o *GCPOrchestrator) RenameExpertModeVolume(ctx context.Context, params *commonparams.ExpertModeVolumeRenameParams) error {
	return _renameExpertModeVolume(ctx, o.storage, o.temporal, params)
}

func _renameExpertModeVolume(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.ExpertModeVolumeRenameParams) error {
	logger := util.GetLogger(ctx)

	if params.VolumeName == "" || params.NewName == "" {
		return customerrors.NewBadRequestErr("volumeName and new name are required for rename")
	}

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return err
	}

	dbPoolView, err := se.GetPool(ctx, params.PoolUUID, account.ID)
	if err != nil {
		logger.Error("Failed to get pool by UUID", "poolUUID", params.PoolUUID, "error", err)
		return err
	}

	volume, err := se.GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, dbPoolView.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || customerrors.IsNotFoundErr(err) {
			return customerrors.NewBadRequestErr(fmt.Sprintf("volume with name '%s' not found in pool", params.VolumeName))
		}
		logger.Error("Failed to find volume by name and pool", "volumeName", params.VolumeName, "poolID", dbPoolView.ID, "error", err)
		return err
	}

	if volume.Svm == nil {
		return customerrors.NewBadRequestErr("volume has no SVM and cannot be renamed")
	}
	if volume.Svm.Name != params.SvmName {
		return customerrors.NewBadRequestErr(fmt.Sprintf("SVM name does not match: expected %s", params.SvmName))
	}

	if volume.AccountID != account.ID {
		logger.Error("Volume does not belong to the specified account", "volumeName", params.VolumeName, "volumeAccountID", volume.AccountID, "accountID", account.ID)
		return customerrors.NewBadRequestErr("volume does not belong to the specified account")
	}

	if volume.State == models.LifeCycleStateCreating || volume.State == models.LifeCycleStateDeleting || volume.State == models.LifeCycleStateUpdating {
		return customerrors.NewBadRequestErr(fmt.Sprintf("volume '%s' is in a transitional state and cannot be renamed", params.VolumeName))
	}

	existingWithNewName, err := se.GetExpertModeVolumeByNameAndPoolID(ctx, params.NewName, dbPoolView.ID)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		logger.Error("Failed to check for existing volume with new name", "newName", params.NewName, "poolID", dbPoolView.ID, "error", err)
		return err
	}
	if existingWithNewName != nil && existingWithNewName.UUID != volume.UUID {
		return customerrors.NewBadRequestErr(fmt.Sprintf("volume with name '%s' already exists in pool", params.NewName))
	}

	oldName := volume.Name
	previousState := volume.State
	volume.Name = params.NewName
	volume.State = models.LifeCycleStateUpdating
	volumeMarkedAsUpdating := true

	oldVolume := &datamodel.ExpertModeVolumes{
		BaseModel:    datamodel.BaseModel{UUID: volume.UUID},
		Name:         oldName,
		SizeInBytes:  volume.SizeInBytes,
		Style:        volume.Style,
		State:        previousState,
		ExternalUUID: volume.ExternalUUID,
	}

	_, err = se.UpdateExpertModeVolume(ctx, volume)
	if err != nil {
		logger.Error("Failed to update volume name and state for rename", "volumeUUID", volume.UUID, "error", err)
		return err
	}

	var createdJob *datamodel.Job
	defer func() {
		if err != nil {
			if createdJob != nil {
				if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), createdJob.TrackingID, err.Error()); jobErr != nil {
					logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
				}
			}
			if volumeMarkedAsUpdating {
				volume.Name = oldName
				volume.State = previousState
				if _, revertErr := se.UpdateExpertModeVolume(ctx, volume); revertErr != nil {
					logger.Error("Failed to revert volume state after rename failure", "volumeUUID", volume.UUID, "error", revertErr)
				}
			}
		}
	}()

	correlationID := utils.GetCoRelationIDFromContext(ctx)
	job := &datamodel.Job{
		Type:          string(models.JobTypeUpdateExpertModeVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  volume.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: volume.UUID, PoolUUID: volume.Pool.UUID},
		CorrelationID: correlationID,
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err = se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job for expert mode volume rename", "error", err)
		return err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.BackgroundTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetExpertModeSyncWorkflowTimeout(),
		},
		expertModeWorkflows.VolumeUpdateReconciliationWorkflow,
		volume, oldVolume,
	)
	if err != nil {
		logger.Error("Failed to start volume rename reconciliation workflow", "workflowID", createdJob.WorkflowID, "error", err)
		return err
	}

	return nil
}

// getExpertModeVolume resolves a volume by UUID first; if UUID not provided, tries by volumeName.
func getExpertModeVolume(ctx context.Context, se database.Storage, params *commonparams.ExpertModeVolumeParams, dbPoolView *datamodel.PoolView) (*datamodel.ExpertModeVolumes, error) {
	logger := util.GetLogger(ctx)

	if params.VolumeUUID == "" && params.VolumeName == "" {
		return nil, customerrors.NewBadRequestErr("either volumeUUID or (volumeName and poolUUID) is required")
	}

	// 1. Look up by UUID when provided
	if params.VolumeUUID != "" {
		volume, err := se.GetExpertModeVolumeByExternalUUID(ctx, params.VolumeUUID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) || customerrors.IsNotFoundErr(err) {
				return nil, customerrors.NewBadRequestErr(fmt.Sprintf("volume with UUID '%s' not found", params.VolumeUUID))
			}
			logger.Error("Failed to find volume by UUID", "volumeUUID", params.VolumeUUID, "error", err)
			return nil, err
		}
		return volume, nil
	}

	// 2. Try by name
	if dbPoolView != nil {
		volume, err := se.GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, dbPoolView.ID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) || customerrors.IsNotFoundErr(err) {
				return nil, customerrors.NewBadRequestErr(fmt.Sprintf("volume with name '%s' not found in pool", params.VolumeName))
			}
			logger.Error("Failed to find volume by name and pool", "volumeName", params.VolumeName, "poolID", dbPoolView.ID, "error", err)
			return nil, err
		}
		return volume, nil
	}

	return nil, customerrors.NewBadRequestErr("volume not found")
}
