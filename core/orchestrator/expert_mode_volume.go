package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
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
)

// CreateExpertModeVolume creates a new expert mode volume
func (o *Orchestrator) CreateExpertModeVolume(ctx context.Context, params *commonparams.ExpertModeVolumeParams) error {
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
		return customerrors.NewBadRequestErr(fmt.Sprintf("volume with name '%s' already exists in pool", volumeName))
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
func (o *Orchestrator) DeleteExpertModeVolume(ctx context.Context, params *commonparams.ExpertModeVolumeParams) error {
	return _deleteExpertModeVolume(ctx, o.storage, o.temporal, params)
}

// _deleteExpertModeVolume deletes an expert mode volume and triggers reconciliation workflow
func _deleteExpertModeVolume(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.ExpertModeVolumeParams) error {
	logger := util.GetLogger(ctx)

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return err
	}

	// VolumeUUID is required for delete
	if params.VolumeUUID == "" {
		logger.Error("VolumeUUID is required for delete operation")
		return customerrors.NewBadRequestErr("VolumeUUID is required for delete operation")
	}

	// Fetch volume by external UUID
	volume, err := se.GetExpertModeVolumeByExternalUUID(ctx, params.VolumeUUID)
	if err != nil {
		logger.Error("Failed to find volume by external UUID", "volumeUUID", params.VolumeUUID, "error", err)
		if customerrors.IsNotFoundErr(err) {
			return customerrors.NewBadRequestErr(fmt.Sprintf("volume with UUID '%s' not found", params.VolumeUUID))
		}
		return err
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

	return nil
}

// GetExpertModeVolumeByExternalUUID retrieves an expert mode volume by its UUID
func (o *Orchestrator) GetExpertModeVolumeByExternalUUID(ctx context.Context, volumeUUID string) (*datamodel.ExpertModeVolumes, error) {
	return o.storage.GetExpertModeVolumeByExternalUUID(ctx, volumeUUID)
}

// UpdateExpertModeVolume updates an existing expert mode volume
func (o *Orchestrator) UpdateExpertModeVolume(ctx context.Context, params *commonparams.ExpertModeVolumeParams) error {
	return _updateExpertModeVolume(ctx, o.storage, params)
}

func validateUpdateParams(ctx context.Context, se database.Storage, params *commonparams.ExpertModeVolumeParams, volume *datamodel.ExpertModeVolumes) error {
	logger := util.GetLogger(ctx)

	if params.VolumeUUID == "" {
		logger.Error("VolumeUUID is required for update operation", "volumeUUID", params.VolumeUUID)
		return customerrors.NewBadRequestErr("VolumeUUID is required for update operation")
	}
	if params.SizeInBytes < 0 {
		logger.Error("Volume size must be greater than or equal to 0", "volumeSize", params.SizeInBytes)
		return customerrors.NewBadRequestErr("Volume size must be greater than or equal to 0")
	}

	if volume.State == models.LifeCycleStateDeleted || volume.State == models.LifeCycleStateError {
		logger.Error("Volume is deleted, cannot update", "volumeUUID", params.VolumeUUID)
		return customerrors.NewBadRequestErr(fmt.Sprintf("volume with UUID '%s' is deleted", params.VolumeUUID))
	}

	if volume.State == models.LifeCycleStateCreating || volume.State == models.LifeCycleStateDeleting || volume.State == models.LifeCycleStateUpdating {
		logger.Error("Volume is in a transitional state and cannot be updated", "volumeUUID", params.VolumeUUID, "state", volume.State)
		return customerrors.NewBadRequestErr(fmt.Sprintf("volume with UUID '%s' is in a transitional state and cannot be updated", params.VolumeUUID))
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
			// params.VolumeUUID is the ExternalUUID, so compare with existingVolume.ExternalUUID
			if existingVolume.ExternalUUID != params.VolumeUUID {
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
func _updateExpertModeVolume(ctx context.Context, se database.Storage, params *commonparams.ExpertModeVolumeParams) error {
	logger := util.GetLogger(ctx)

	if params.VolumeUUID == "" {
		logger.Error("VolumeUUID is required for update operation", "volumeUUID", params.VolumeUUID, "poolUUID", params.PoolUUID)
		return customerrors.NewBadRequestErr("VolumeUUID is required for update operation")
	}

	// Fetch volume by the ONTAP's external UUID
	volume, err := se.GetExpertModeVolumeByExternalUUID(ctx, params.VolumeUUID)
	if err != nil {
		logger.Error("Failed to find volume by UUID", "volumeUUID", params.VolumeUUID, "error", err)
		if customerrors.IsNotFoundErr(err) || errors.Is(err, gorm.ErrRecordNotFound) {
			return customerrors.NewBadRequestErr(fmt.Sprintf("volume with UUID '%s' not found", params.VolumeUUID))
		}
		return err
	}

	err = validateUpdateParams(ctx, se, params, volume)
	if err != nil {
		return err
	}

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return err
	}

	// Validate account matches - use AccountID directly as it's always set
	if volume.AccountID != account.ID {
		logger.Error("Volume does not belong to the specified account", "volumeUUID", params.VolumeUUID, "volumeAccountID", volume.AccountID, "accountID", account.ID)
		return customerrors.NewBadRequestErr("volume does not belong to the specified account")
	}

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

	// to revert the volume state if the update fails while reconciliation
	defer func() {
		if err != nil {
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

	// Update volume in DB
	_, err = se.UpdateExpertModeVolume(ctx, volume)
	if err != nil {
		logger.Error("Failed to update volume state to UPDATING and size", "volumeUUID", volume.UUID, "error", err)
		return err
	}

	return nil
}
