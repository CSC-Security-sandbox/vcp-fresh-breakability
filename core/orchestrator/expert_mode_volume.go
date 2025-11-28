package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"gorm.io/gorm"
)

const (
	ExpertModeVolumeStyleFlexgroup = "flexgroup"
)

// CreateExpertModeVolume creates a new expert mode volume
func (o *Orchestrator) CreateExpertModeVolume(ctx context.Context, params *commonparams.CreateExpertModeVolumeParams) error {
	return _createExpertModeVolume(ctx, o.storage, o.temporal, params)
}

// createExpertModeVolume creates a new expert mode volume and triggers reconciliation workflow
func _createExpertModeVolume(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.CreateExpertModeVolumeParams) error {
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
	totalUsedSize, err := se.GetExpertModePoolUsedCapacity(ctx, dbPoolView.ID)
	if err != nil {
		logger.Error("Failed to calculate total existing size", "poolID", dbPoolView.ID, "error", err)
		return err
	}

	// Check if new volume can fit in the pool
	if totalUsedSize+params.SizeInBytes > int64(dbPoolView.SizeInBytes) {
		logger.Error("Insufficient pool capacity", "poolID", dbPoolView.ID, "requestedSize", params.SizeInBytes, "availableSize", int64(dbPoolView.SizeInBytes)-totalUsedSize)
		return customerrors.NewBadRequestErr(fmt.Sprintf("insufficient pool capacity: requested %d bytes, available %d bytes",
			params.SizeInBytes, int64(dbPoolView.SizeInBytes)-totalUsedSize))
	}

	// Create expert mode volume record
	expertModeVolume := &datamodel.ExpertModeVolumes{
		Name:         params.VolumeName,
		SizeInBytes:  params.SizeInBytes,
		PoolID:       dbPoolView.ID,
		AccountID:    dbPoolView.AccountID,
		Style:        params.Style,
		ExternalUUID: utils.RandomUUID(),
		State:        models.LifeCycleStateCreating,
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

	// Start reconciliation workflow
	workflowID := fmt.Sprintf("volume_reconciliation_%s", createdVolume.UUID)
	logger.Info("Starting volume reconciliation workflow", "workflowID", workflowID, "volumeUUID", createdVolume.UUID)

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.BackgroundTaskQueue,
			ID:                    workflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetExpertModeSyncWorkflowTimeout(),
		},
		VolumeReconciliationWorkflow,
		createdVolume,
	)

	if err != nil {
		logger.Error("Failed to start volume reconciliation workflow", "workflowID", workflowID, "error", err)
		// Note: We don't return error here as the volume was created successfully
		// The workflow failure can be handled separately
	}

	// Return success response
	return nil
}

// VolumeReconciliationWorkflow is a skeleton workflow for volume reconciliation
func VolumeReconciliationWorkflow(ctx workflow.Context, expertModeVolume *datamodel.ExpertModeVolumes, correlationID string) error {
	// This is a skeleton workflow - no actual logic added
	// TODO: Implement actual reconciliation logic

	// For now, just log that the workflow was triggered
	workflow.GetLogger(ctx).Info("Volume reconciliation workflow triggered",
		"volumeUUID", expertModeVolume.UUID,
		"volumeName", expertModeVolume.Name,
		"correlationID", correlationID)

	return nil
}
