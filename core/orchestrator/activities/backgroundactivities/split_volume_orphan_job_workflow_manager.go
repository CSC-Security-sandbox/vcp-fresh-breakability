package backgroundactivities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// SplitVolumeArgs implements OrphanJobWorkflowManager for the split-clone-volume job type.
// It is used by OrphanJobsActivity to re-submit VolumePollSplitWorkflow when
// Temporal was unavailable at the time of the original split-start API call.
type SplitVolumeArgs struct{}

// FailedWorkflowJob is called when the orphan job processor has exhausted all retry
// attempts without successfully starting the split workflow.
//
// At this point ONTAP has already accepted the split request and data movement may be
// in progress (splitInitiated was true before the workflow was first attempted). We
// cannot revert the ONTAP operation, so we mark the volume clone state as
// ERROR_IN_SPLITTING so that operators and the API caller can see the failure.
func (s *SplitVolumeArgs) FailedWorkflowJob(ctx context.Context, se database.Storage, job *datamodel.Job, reason string) error {
	logger := util.GetLogger(ctx)
	volumeUUID := job.JobAttributes.ResourceUUID

	volume, err := se.GetVolume(ctx, volumeUUID)
	if err != nil {
		return fmt.Errorf("failed to get volume %s for split failure cleanup: %w", volumeUUID, err)
	}

	if volume.VolumeAttributes == nil || volume.VolumeAttributes.CloneParentInfo == nil {
		logger.Warnf("Volume %s has no clone parent info, skipping split failure state update", volumeUUID)
		return nil
	}

	updatedAttrs := *volume.VolumeAttributes
	cloneInfo := *volume.VolumeAttributes.CloneParentInfo
	cloneInfo.State = datamodel.CloneStateErrorInSplitting
	cloneInfo.StateDetails = reason
	updatedAttrs.CloneParentInfo = &cloneInfo

	if updateErr := se.UpdateVolumeFields(ctx, volumeUUID, map[string]interface{}{
		"volume_attributes": &updatedAttrs,
	}); updateErr != nil {
		return fmt.Errorf("failed to update clone state to %s for volume %s: %w", datamodel.CloneStateErrorInSplitting, volumeUUID, updateErr)
	}

	logger.Warnf("Marked volume %s clone state as %s after split workflow failed to start: %s", volumeUUID, datamodel.CloneStateErrorInSplitting, reason)
	return nil
}

// PrepareWorkflowArgs reconstructs the arguments needed to start VolumePollSplitWorkflow
// from the durable state stored in the database when the job was created.
//
// The volume row carries the ONTAP job UUID in VolumeAttributes.SplitJobUUID (written
// by _splitStartVolume before the workflow was first attempted) and the node details via
// the pool association, so no in-memory state is required.
func (s *SplitVolumeArgs) PrepareWorkflowArgs(ctx context.Context, se database.Storage, job *datamodel.Job) ([]interface{}, error) {
	volumeUUID := job.JobAttributes.ResourceUUID

	volume, err := se.GetVolume(ctx, volumeUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get volume %s for split workflow args: %w", volumeUUID, err)
	}

	if volume.VolumeAttributes == nil || volume.VolumeAttributes.SplitJobUUID == "" {
		return nil, fmt.Errorf("volume %s has no SplitJobUUID, cannot re-submit split workflow", volumeUUID)
	}

	if volume.Pool == nil {
		return nil, fmt.Errorf("volume %s has no associated pool, cannot re-submit split workflow", volumeUUID)
	}

	dbNodes, err := se.GetNodesByPoolID(ctx, volume.Pool.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes for pool %d (volume %s): %w", volume.Pool.ID, volumeUUID, err)
	}
	if len(dbNodes) == 0 {
		return nil, fmt.Errorf("no nodes found for pool %s, cannot re-submit split workflow for volume %s", volume.Pool.UUID, volumeUUID)
	}

	node := vsa.CreateNodeForProvider(vsa.NodeProviderInput{
		Nodes:            dbNodes,
		DeploymentName:   volume.Pool.DeploymentName,
		OntapCredentials: volume.Pool.PoolCredentials,
	})

	ontapJobUUID := volume.VolumeAttributes.SplitJobUUID

	return []interface{}{volume, node, ontapJobUUID}, nil
}
