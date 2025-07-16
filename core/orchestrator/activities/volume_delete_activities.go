package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type VolumeDeleteActivity struct {
	SE database.Storage
}

func (va VolumeDeleteActivity) DeleteVolumeInONTAP(ctx context.Context, volumeExternalUUID, volumeName string, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	err = provider.DeleteVolume(volumeExternalUUID, volumeName)
	if err != nil {
		return err
	}
	logger.Debugf("Volume %s deleted successfully from the vsa cluster", volumeName)

	return nil
}

func (va VolumeDeleteActivity) DeleteVolume(ctx context.Context, volume *datamodel.Volume) error {
	logger := util.GetLogger(ctx)
	se := va.SE

	_, err := se.DeleteVolume(ctx, volume.UUID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return nil
		}
		return err
	}
	logger.Debugf("Volume:%s marked deleted successfully in the db", volume.Name)

	return nil
}

// DeleteSnapshotPolicyInONTAP deletes the snapshot policy associated with a volume in ONTAP.
func (va VolumeDeleteActivity) DeleteSnapshotPolicyInONTAP(ctx context.Context, SnapshotPolicyName string, node *models.Node) error {
	if node != nil && SnapshotPolicyName != "" {
		logger := util.GetLogger(ctx)
		provider, err := GetProviderByNode(ctx, node)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		op := func() error {
			return provider.DeleteSnapshotPolicy(SnapshotPolicyName)
		}
		err = vsa.RetryOnErrors(op, []string{"Policy is in use by at least one volume"})
		if err != nil {
			logger.Errorf("failed to delete snapshot policy: %v", err)
			return err
		}
	}
	return nil
}

func (va VolumeDeleteActivity) DeleteSnapmirrorInONTAP(ctx context.Context, volumeUUID string, node *models.Node) (*vsa.OntapAsyncResponse, error) {
	logger := util.GetLogger(ctx)
	if node != nil && volumeUUID != "" {
		provider, err := GetProviderByNode(ctx, node)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}

		se := va.SE
		backupsCount, err := se.BackupCountByVolumeID(ctx, volumeUUID)
		if err != nil {
			logger.Errorf("failed to get backups count for volume %s: %v", volumeUUID, err)
			return nil, err
		}
		if backupsCount != 0 {
			return provider.SnapmirrorRelationshipDelete(volumeUUID)
		} else {
			logger.Debugf("no snapmirror relationship found for volume %s, skipping deletion", volumeUUID)
		}
	}
	return nil, nil
}

func (va VolumeDeleteActivity) DeleteVolumeAssociatedSnapshots(ctx context.Context, volumeID int64) error {
	logger := util.GetLogger(ctx)
	se := va.SE
	snapshots, err := se.GetSnapshotsByVolumeID(ctx, volumeID)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Debugf("no snapshots found for volumeID: %d", volumeID)
			return nil // No snapshots to delete
		}
		logger.Errorf("failed to get snapshot by volumeID: %v", err)
		return err
	}

	for _, snapshot := range snapshots {
		_, err = se.DeleteSnapshot(ctx, snapshot.UUID)
		if err != nil {
			logger.Warnf("failed to mark snapshot %s as deleted because of error: %v", snapshot.Name, err)
		}
	}
	return nil
}
