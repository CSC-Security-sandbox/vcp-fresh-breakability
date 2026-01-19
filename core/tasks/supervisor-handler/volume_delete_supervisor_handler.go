package supervisorhandler

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// VolumeDeleteHandler implements Handler for Volume delete operations.
type VolumeDeleteHandler struct{}

// NewVolumeDeleteHandler returns the handler that reverts volume state for stale delete jobs.
func NewVolumeDeleteHandler() *VolumeDeleteHandler {
	return &VolumeDeleteHandler{}
}

// JobTypes enumerates the job types supported by the volume delete handler.
func (h *VolumeDeleteHandler) JobTypes() []models.JobType {
	return []models.JobType{
		models.JobTypeDeleteVolume,
		models.JobTypeDeleteLargeVolume,
		models.JobTypeFlexCacheDeleteVolume,
	}
}

// Handle reverts volume state from DELETING to previous state for stale delete jobs.
func (h *VolumeDeleteHandler) Handle(ctx context.Context, job *datamodel.Job, event Event, storage database.Storage) error {
	if event != EventTimeout {
		return nil
	}

	logger := util.GetLogger(ctx).With(log.Fields{
		"jobUUID":                               job.UUID,
		"jobType":                               job.Type,
		string(middleware.RequestCorrelationID): utils.GetCoRelationIDFromContext(ctx),
	})

	if job.JobAttributes == nil || job.JobAttributes.ResourceUUID == "" {
		logger.Warnf("workflow-supervisor-task: job lacks volume resource UUID; skipping cleanup")
		return nil
	}

	volume, err := storage.GetVolume(ctx, job.JobAttributes.ResourceUUID)
	if err != nil {
		if vsaerrors.IsNotFoundErr(err) {
			logger.Warnf("workflow-supervisor-task: volume already deleted in VCP")
			return nil
		}
		return fmt.Errorf("load volume: %w", err)
	}

	// Only revert if volume is in DELETING state
	if volume.State != models.LifeCycleStateDeleting {
		logger.Warnf("workflow-supervisor-task: volume not in DELETING state (%s); skipping revert", volume.State)
		return nil
	}

	// Get previous state from job attributes, with fallback
	previousState := job.JobAttributes.PreviousState
	previousStateDetails := job.JobAttributes.PreviousStateDetails

	// Fallback if previous state not stored (for backward compatibility with old jobs)
	if previousState == "" {
		logger.Warnf("workflow-supervisor-task: previous state not stored in job attributes, using default READY")
		previousState = models.LifeCycleStateREADY
		previousStateDetails = models.LifeCycleStateAvailableDetails
	}

	err = storage.UpdateVolumeFields(ctx, volume.UUID, map[string]interface{}{
		"state":         previousState,
		"state_details": previousStateDetails,
	})
	if err != nil {
		return fmt.Errorf("revert volume state to %s: %w", previousState, err)
	}

	logger.Infof("workflow-supervisor-task: volume %s reverted from DELETING to %s", volume.UUID, previousState)
	return nil
}

