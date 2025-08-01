package activities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
		return vsaerrors.WrapAsTemporalApplicationError(err)
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
		return vsaerrors.WrapAsTemporalApplicationError(err)
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
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}
	return nil
}

func (va VolumeDeleteActivity) DeleteSnapmirrorInONTAP(ctx context.Context, volume *datamodel.Volume, node *models.Node) (*vsa.OntapAsyncResponse, error) {
	logger := util.GetLogger(ctx)
	if node != nil && volume.UUID != "" {
		provider, err := GetProviderByNode(ctx, node)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}

		se := va.SE
		backupsCount, err := se.BackupCountByVolumeID(ctx, volume.UUID)
		if err != nil {
			logger.Errorf("Failed to get backups count for volume %s: %v", volume.UUID, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get backups count for volume %s: %w", volume.UUID, err))
		}

		if backupsCount == 0 {
			logger.Debugf("No backups found for volume %s, skipping snapmirror deletion", volume.UUID)
			return nil, nil
		}

		if volume.DataProtection == nil || volume.DataProtection.BackupVaultID == "" {
			logger.Warnf("Volume %s has backups but no data protection configuration, skipping snapmirror deletion", volume.UUID)
			return nil, nil
		}

		dbBackupVault, err := se.GetBackupVault(ctx, volume.DataProtection.BackupVaultID)
		if err != nil {
			logger.Errorf("Failed to get backup vault for volume %s: %v", volume.UUID, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get backup vault for volume %s: %w", volume.UUID, err))
		}

		smDestinationPath, err := GetSmDestinationPath(dbBackupVault, volume)
		if err != nil {
			logger.Errorf("Failed to get snapmirror destination path for volume %s: %v", volume.UUID, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get snapmirror destination path for volume %s: %w", volume.UUID, err))
		}

		smSourcePath := fmt.Sprintf("%s:%s", volume.Svm.Name, volume.Name)

		snapmirror, err := provider.SnapmirrorRelationshipGet(smDestinationPath, smSourcePath)
		if err != nil {
			if errors.IsNotFoundErr(err) {
				logger.Debugf("No snapmirror relationship found for volume %s (paths: %s -> %s), skipping deletion", volume.UUID, smSourcePath, smDestinationPath)
				return nil, nil
			}
			logger.Errorf("Failed to get snapmirror relationship for volume %s: %v", volume.UUID, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get snapmirror relationship for volume %s: %w", volume.UUID, err))
		}

		logger.Debugf("Deleting snapmirror relationship %s for volume %s", snapmirror.UUID.String(), volume.UUID)

		response, err := provider.SnapmirrorRelationshipDelete(snapmirror.UUID.String())
		if err != nil {
			logger.Errorf("Failed to delete snapmirror relationship %s for volume %s: %v", snapmirror.UUID.String(), volume.UUID, err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to delete snapmirror relationship %s for volume %s: %w", snapmirror.UUID.String(), volume.UUID, err))
		}

		return response, nil
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
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	for _, snapshot := range snapshots {
		_, err = se.DeleteSnapshot(ctx, snapshot.UUID)
		if err != nil {
			logger.Warnf("failed to mark snapshot %s as deleted because of error: %v", snapshot.Name, err)
		}
	}
	return nil
}
