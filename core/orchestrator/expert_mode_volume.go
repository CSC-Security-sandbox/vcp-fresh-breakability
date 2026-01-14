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

	existingVolume, err := se.GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, dbPoolView.ID)
	if err != nil {
		// If error is NOT "record not found", it's a real database error
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			logger.Error("Failed to check for existing volume", "volumeName", params.VolumeName, "poolID", dbPoolView.ID, "error", err)
			return err
		}
	} else if existingVolume != nil {
		logger.Error("Volume with same name already exists in pool",
			"volumeName", params.VolumeName,
			"poolID", dbPoolView.ID)
		return customerrors.NewBadRequestErr(fmt.Sprintf("volume with name '%s' already exists in pool", params.VolumeName))
	}

	// Validate that flexgroup volumes require a large capacity pool
	if params.Style == ExpertModeVolumeStyleFlexgroup && !dbPoolView.LargeCapacity {
		logger.Error("Flexgroup volume requires large capacity pool", "poolUUID", params.PoolUUID, "largeCapacity", dbPoolView.LargeCapacity)
		return customerrors.NewBadRequestErr("Pool is not type of largeCapacity")
	}

	// Calculate total existing size to validate pool capacity
	capacity, err := se.GetExpertModePoolUsedCapacityAndVolumeCount(ctx, dbPoolView.ID)
	if err != nil {
		logger.Error("Failed to calculate total existing size", "poolID", dbPoolView.ID, "error", err)
		return err
	}
	totalUsedSize := capacity.TotalSize

	// Check if new volume can fit in the pool
	if totalUsedSize+params.SizeInBytes > int64(dbPoolView.SizeInBytes) {
		logger.Error("Insufficient pool capacity", "poolID", dbPoolView.ID, "requestedSize", params.SizeInBytes, "availableSize", int64(dbPoolView.SizeInBytes)-totalUsedSize)
		return customerrors.NewBadRequestErr(fmt.Sprintf("insufficient pool capacity: requested %d bytes, available %d bytes",
			params.SizeInBytes, int64(dbPoolView.SizeInBytes)-totalUsedSize))
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

// GetExpertModeVolumeByUUID retrieves an expert mode volume by its UUID
func (o *Orchestrator) GetExpertModeVolumeByUUID(ctx context.Context, volumeUUID string) (*datamodel.ExpertModeVolumes, error) {
	return o.storage.GetExpertModeVolumeByUUID(ctx, volumeUUID)
}
