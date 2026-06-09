package backgroundactivities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type FlexCachePrepopulateActivity struct {
	SE database.Storage
}

// GetActivePrepopulateJobs - Only log when jobs are found
func (a *FlexCachePrepopulateActivity) GetActivePrepopulateJobs(ctx context.Context) ([]*datamodel.Job, error) {
	logger := util.GetLogger(ctx)

	jobs, err := a.SE.GetActivePrepopulateJobs(ctx)
	if err != nil {
		logger.Errorf("Failed to get active prepopulate jobs: %v", err)
		return nil, fmt.Errorf("failed to get active prepopulate jobs: %w", err)
	}

	if len(jobs) > 0 {
		logger.Infof("Found %d active prepopulate jobs", len(jobs))
	}
	return jobs, nil
}

// GetVolumeByResourceName retrieves a volume by its UUID stored in job.ResourceName.
func (a *FlexCachePrepopulateActivity) GetVolumeByResourceName(
	ctx context.Context,
	volumeUUID string,
) (*datamodel.Volume, error) {
	logger := util.GetLogger(ctx)

	volume, err := a.SE.GetVolume(ctx, volumeUUID)
	if err != nil {
		// Volume not found is expected when the volume was deleted.
		// Return (nil, nil) so the workflow can handle it with a simple nil check
		// instead of trying to detect error types across the Temporal boundary.
		if errors.IsNotFoundErr(err) {
			logger.Warnf("Volume %s not found (likely deleted)", volumeUUID)
			return nil, nil
		}
		logger.Errorf("Failed to get volume %s: %v", volumeUUID, err)
		return nil, fmt.Errorf("failed to get volume: %w", err)
	}

	logger.Debugf("Retrieved volume %s (name: %s)", volume.UUID, volume.Name)
	return volume, nil
}

// PollPrepopulateJobStatus polls ONTAP for the status of a prepopulate job
func (a *FlexCachePrepopulateActivity) PollPrepopulateJobStatus(
	ctx context.Context,
	volume *datamodel.Volume,
	job *datamodel.Job,
) (*common.PrepopulateJobStatus, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("Polling prepopulate job status for job %s, volume %s", job.UUID, volume.UUID)

	if job.JobAttributes == nil || job.JobAttributes.ResourceUUID == "" {
		return nil, fmt.Errorf("job %s has no ONTAP job UUID", job.UUID)
	}

	ontapJobUUID := job.JobAttributes.ResourceUUID

	nodes, err := a.SE.GetNodesByPoolID(ctx, volume.PoolID)
	if err != nil {
		logger.Errorf("Failed to get nodes for pool %d: %v", volume.PoolID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if len(nodes) == 0 {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrUnexpectedNodeCountForPool,
				errors.New("no nodes found for the pool")))
	}

	node := vsa.CreateNodeForProvider(vsa.NodeProviderInput{
		Nodes:            nodes,
		DeploymentName:   volume.Pool.DeploymentName,
		OntapCredentials: volume.Pool.PoolCredentials,
	})

	provider, err := vsa.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to get provider for node: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Query ONTAP for job status
	ontapJob, err := provider.JobGet(ontapJobUUID)
	if err != nil {
		logger.Errorf("Failed to get job status from ONTAP for UUID %s: %v", ontapJobUUID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	status := &common.PrepopulateJobStatus{
		JobUUID: ontapJobUUID,
	}

	if ontapJob.State != "" {
		status.State = ontapJob.State
	}

	if ontapJob.Error != nil {
		status.ErrorMessage = ontapJob.Error.Message
	}

	switch ontapJob.State {
	case common.OntapJobStateSuccess, common.OntapJobStateFailure:
		logger.Infof("Prepopulate ONTAP job %s reached terminal state: %s", ontapJobUUID, status.State)
	default:
		logger.Debugf("Prepopulate ONTAP job %s status: %s", ontapJobUUID, status.State)
	}

	return status, nil
}

// UpdateJobAndVolumeStatus updates both the job record and volume record when prepopulate completes
func (a *FlexCachePrepopulateActivity) UpdateJobAndVolumeStatus(
	ctx context.Context,
	jobUUID string,
	volumeUUID string,
	jobStatus *common.PrepopulateJobStatus,
) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Updating job %s and volume %s with prepopulate status: %s",
		jobUUID, volumeUUID, jobStatus.State)

	var jobState string
	var errorDetails string

	switch jobStatus.State {
	case common.OntapJobStateSuccess:
		jobState = string(datamodel.JobsStateDONE)
	case common.OntapJobStateFailure:
		jobState = string(datamodel.JobsStateERROR)
		errorDetails = jobStatus.ErrorMessage
	default:
		// Still processing - don't update
		logger.Infof("Job %s still in progress with state %s, no update needed", jobUUID, jobStatus.State)
		return nil
	}

	volume, err := a.SE.GetVolume(ctx, volumeUUID)
	if err != nil {
		logger.Errorf("Failed to get volume %s: %v", volumeUUID, err)
		return fmt.Errorf("failed to get volume: %w", err)
	}

	if volume.CacheParameters == nil {
		volume.CacheParameters = &datamodel.CacheParameters{}
	}
	if volume.CacheParameters.CacheConfig == nil {
		volume.CacheParameters.CacheConfig = &datamodel.CacheConfig{}
	}

	volume.CacheParameters.CacheConfig.CachePrePopulateState = common.MapOntapStateToAPIState(jobStatus.State)

	updates := map[string]interface{}{
		"cache_parameters": volume.CacheParameters,
	}

	err = a.SE.UpdateVolumeFields(ctx, volumeUUID, updates)
	if err != nil {
		logger.Errorf("Failed to update volume %s: %v", volumeUUID, err)
		return fmt.Errorf("failed to update volume: %w", err)
	}

	// Update job last, this marks the operation as complete
	err = a.SE.UpdateJob(ctx, jobUUID, jobState, 0, errorDetails)
	if err != nil {
		logger.Errorf("Failed to update job %s: %v", jobUUID, err)
		logger.Warnf("Volume %s updated successfully but job %s update failed - job may need manual reconciliation",
			volumeUUID, jobUUID)
		return fmt.Errorf("failed to update job: %w", err)
	}

	return nil
}

// MarkOrphanedPrepopulateJob marks a prepopulate job as ERROR when the associated volume is not found
// This handles the case where a volume was deleted but the prepopulate job remains in NEW state
func (a *FlexCachePrepopulateActivity) MarkOrphanedPrepopulateJob(
	ctx context.Context,
	jobUUID string,
	volumeUUID string,
) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Marking prepopulate job %s as orphaned (volume %s not found)", jobUUID, volumeUUID)

	errorDetails := fmt.Sprintf("Volume %s was deleted, prepopulate job cannot complete", volumeUUID)
	err := a.SE.UpdateJob(ctx, jobUUID, string(datamodel.JobsStateERROR), 0, errorDetails)
	if err != nil {
		logger.Errorf("Failed to mark orphaned job %s: %v", jobUUID, err)
		return fmt.Errorf("failed to mark orphaned job: %w", err)
	}

	logger.Infof("Successfully marked prepopulate job %s as orphaned", jobUUID)
	return nil
}
