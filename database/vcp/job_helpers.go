package database

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// GetExistingDeleteJobForDeletingState checks for an existing delete job when resource is in DELETING state.
func GetExistingDeleteJobForDeletingState(ctx context.Context, se Storage, resourceUUID string, deleteJobType datamodel.JobType, logger log.Logger) (existingJobUUID string) {
	existingJob, err := se.GetJobByResourceUUID(ctx, resourceUUID, string(deleteJobType))
	if err == nil && existingJob != nil {
		if existingJob.State != string(datamodel.JobsStateDONE) && existingJob.State != string(datamodel.JobsStateERROR) {
			logger.Infof("Delete job already in progress for %s, returning existing job UUID: %s", resourceUUID, existingJob.UUID)
			return existingJob.UUID
		}
	}
	return ""
}

// ValidateCorrelationIDForCreatingResource handles CREATING state logic for delete operations.
func ValidateCorrelationIDForCreatingResource(ctx context.Context, se Storage, resourceUUID string, resourceType string, createJobType datamodel.JobType, deleteJobType datamodel.JobType, logger log.Logger) (existingDeleteJobUUID string, isCleanupDelete bool, err error) {
	correlationID := utils.GetCoRelationIDFromContext(ctx)
	if correlationID == "" {
		logger.Warnf("Correlation ID is empty for delete request on %s %s in CREATING state", resourceType, resourceUUID)
		return "", false, customerrors.NewConflictErr(fmt.Sprintf("Error deleting %s - %s is already transitioning between states", resourceType, resourceType))
	}

	existingDeleteJob, err := se.GetJobByResourceUUID(ctx, resourceUUID, string(deleteJobType))
	if err == nil && existingDeleteJob != nil {
		if existingDeleteJob.CorrelationID != correlationID {
			logger.Warnf("Correlation ID mismatch: create job correlation ID %s does not match delete request correlation ID %s", existingDeleteJob.CorrelationID, correlationID)
			return "", false, customerrors.NewConflictErr(fmt.Sprintf("Error deleting %s - %s is already transitioning between states", resourceType, resourceType))
		}
		if existingDeleteJob.State != string(datamodel.JobsStateDONE) && existingDeleteJob.State != string(datamodel.JobsStateERROR) {
			logger.Infof("Delete job already in progress for %s %s, returning existing job UUID: %s", resourceType, resourceUUID, existingDeleteJob.UUID)
			return existingDeleteJob.UUID, false, nil
		}
	}

	createJob, err := se.GetJobByResourceUUID(ctx, resourceUUID, string(createJobType))
	if err != nil || createJob == nil {
		logger.Warnf("Create job not found for %s %s in CREATING state", resourceType, resourceUUID)
		return "", false, customerrors.NewConflictErr(fmt.Sprintf("Error deleting %s - %s is already transitioning between states", resourceType, resourceType))
	}

	if createJob.CorrelationID != correlationID {
		logger.Warnf("Correlation ID mismatch: create job correlation ID %s does not match delete request correlation ID %s", createJob.CorrelationID, correlationID)
		return "", false, customerrors.NewConflictErr(fmt.Sprintf("Error deleting %s - %s is already transitioning between states", resourceType, resourceType))
	}

	return "", true, nil
}
