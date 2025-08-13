package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type VolumeRevertActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

func (a VolumeRevertActivity) RevertVolume(ctx context.Context, volume *datamodel.Volume, snapshot *datamodel.Snapshot, node *models.Node, params vsa.RevertVolumeParams) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	revertVolumeParams := vsa.RevertVolumeParams{
		VolumeID:        volume.VolumeAttributes.ExternalUUID,
		SnapshotID:      snapshot.SnapshotAttributes.ExternalUUID,
		SnapshotName:    snapshot.Name,
		SvmName:         volume.Svm.Name,
		PreRevertVolume: volume,
	}

	err = provider.RevertVolume(revertVolumeParams)
	if err != nil {
		logger.Errorf("Failed to revert volume %s in ontap: %v", params.VolumeID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Debugf("Volume %s Reverted successfully in ontap", params.VolumeID)

	se := a.SE
	snapshots, err := se.RevertedVolume(ctx, volume, snapshot)
	if err != nil {
		logger.Errorf("Failed to update the reverted volume %s in DB: %v", params.VolumeID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Hydrate snapshots to CCFE after volume revert
	if len(snapshots) > 0 {
		hydrateErr := hydrationActivities.HydrateBatchSnapshotstoCCFE(ctx, nil, snapshots)
		if hydrateErr != nil {
			logger.Errorf("Failed to hydrate snapshots to CCFE after volume revert: %v, snapshots: %+v", hydrateErr, snapshots)
		}
	}

	return nil
}
