package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
)

type VolumeSplitActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

// UpdateCloneSharedBytesInDB updates the clones_shared_bytes field to 0 for a volume in the database
func (a VolumeSplitActivity) UpdateCloneSharedBytesInDB(ctx context.Context, volumeUUID string, clonesSharedBytes uint64) error {
	activity.RecordHeartbeat(ctx, "Initializing clone shared bytes update")
	logger := util.GetLogger(ctx)
	se := a.SE

	activity.RecordHeartbeat(ctx, "Updating clones_shared_bytes in database")
	// Update the clones_shared_bytes field in the database
	err := se.UpdateVolumeFields(ctx, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	})
	if err != nil {
		logger.Errorf("Failed to update clones_shared_bytes for volume %s in the database: %v", volumeUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Clone shared bytes updated successfully")
	logger.Debugf("Successfully updated clones_shared_bytes to %d for volume %s in the database", clonesSharedBytes, volumeUUID)
	return nil
}

// CleanupSplitSnapshot deletes the clone's snapshot that mirrors the parent snapshot
// and hydrates the deletion to CCFE. This runs after the ONTAP split job has completed
// successfully. All failures are treated as warnings — the split itself already succeeded.
func (a VolumeSplitActivity) CleanupSplitSnapshot(ctx context.Context, volume *datamodel.Volume) error {
	activity.RecordHeartbeat(ctx, "Starting split snapshot cleanup")
	logger := util.GetLogger(ctx)

	if volume.VolumeAttributes == nil ||
		volume.VolumeAttributes.CloneParentInfo == nil ||
		volume.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID == "" ||
		volume.VolumeAttributes.CloneParentInfo.ParentVolumeUUID == "" {
		logger.Debugf("Volume %s has no clone parent info with snapshot, skipping snapshot cleanup", volume.Name)
		return nil
	}

	activity.RecordHeartbeat(ctx, "Retrieving parent volume information")
	parentVolume, err := a.SE.GetVolume(ctx, volume.VolumeAttributes.CloneParentInfo.ParentVolumeUUID)
	if err != nil {
		logger.Warnf("Failed to get parent volume %s during split snapshot cleanup: %v", volume.VolumeAttributes.CloneParentInfo.ParentVolumeUUID, err)
		return nil
	}

	activity.RecordHeartbeat(ctx, "Retrieving parent snapshot information")
	parentSnapshot, err := a.SE.GetSnapshotByUUID(ctx, volume.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID, parentVolume.AccountID, parentVolume.ID)
	if err != nil {
		logger.Warnf("Failed to get parent snapshot %s during split snapshot cleanup: %v", volume.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID, err)
		return nil
	}

	activity.RecordHeartbeat(ctx, "Retrieving clone volume snapshot")
	cloneSnapshot, err := a.SE.GetSnapshotByNameAndVolumeId(ctx, parentSnapshot.Name, volume.AccountID, volume.ID)
	if err != nil {
		logger.Warnf("Failed to get clone volume snapshot with name %s for volume %s: %v", parentSnapshot.Name, volume.Name, err)
		return nil
	}
	if cloneSnapshot == nil {
		logger.Debugf("No clone snapshot found for volume %s, nothing to clean up", volume.Name)
		return nil
	}
	logger.Debugf("Found clone volume snapshot %s (UUID: %s) with same name as parent snapshot %s", cloneSnapshot.Name, cloneSnapshot.UUID, parentSnapshot.Name)

	activity.RecordHeartbeat(ctx, "Deleting clone snapshot")
	_, err = a.SE.DeleteSnapshot(ctx, cloneSnapshot.UUID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Warnf("Snapshot %s not found during cleanup, assuming it is already deleted", cloneSnapshot.Name)
			return nil
		}
		logger.Errorf("Failed to delete snapshot %s after split operation: %v", cloneSnapshot.Name, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Debugf("Snapshot %s (UUID: %s) marked as deleted successfully in the db", cloneSnapshot.Name, cloneSnapshot.UUID)

	activity.RecordHeartbeat(ctx, "Hydrating snapshot deletion to CCFE")
	hydrateErr := hydrationActivities.HydrateBatchSnapshotstoCCFE(ctx, nil, []*datamodel.Snapshot{cloneSnapshot})
	if hydrateErr != nil {
		logger.Warnf("Failed to hydrate snapshot deletion to CCFE after split: %v, snapshot: %+v", hydrateErr, cloneSnapshot)
	}

	activity.RecordHeartbeat(ctx, "Split snapshot cleanup completed")
	return nil
}
