package activities

import (
	"context"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type VolumeSplitActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

// UpdateCloneSharedBytesInDB updates the clones_shared_bytes field to 0 for a volume in the database
func (a VolumeSplitActivity) UpdateCloneSharedBytesInDB(ctx context.Context, volumeUUID string, clonesSharedBytes uint64) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	// Update the clones_shared_bytes field in the database
	err := se.UpdateVolumeFields(ctx, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	})
	if err != nil {
		logger.Errorf("Failed to update clones_shared_bytes for volume %s in the database: %v", volumeUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Debugf("Successfully updated clones_shared_bytes to %d for volume %s in the database", clonesSharedBytes, volumeUUID)
	return nil
}
