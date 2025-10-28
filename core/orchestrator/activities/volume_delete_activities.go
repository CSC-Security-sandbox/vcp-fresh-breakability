package activities

import (
	"context"
	"fmt"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
)

type VolumeDeleteActivity struct {
	SE database.Storage
}

func (va VolumeDeleteActivity) DeleteVolumeInONTAP(ctx context.Context, volumeExternalUUID, volumeName string, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	err = provider.DeleteVolume(volumeExternalUUID, volumeName)
	if err != nil {
		if strings.Contains(err.Error(), "volume is in use") {
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(err)
		}
		if strings.Contains(err.Error(), "Retries exhausted when attempting to reach the storage server") {
			logger.Errorf("DeleteVolumeInONTAP - Unable to reach node %s Error: %v", node.Name, err)
			return temporal.NewNonRetryableApplicationError("Unable to delete volume: Node not reachable", "DeleteVolumeInONTAPError", errors.New("unable to reach node"))
		}

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
		provider, err := hyperscaler.GetProviderByNode(ctx, node)
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
		provider, err := hyperscaler.GetProviderByNode(ctx, node)
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

func (vda VolumeDeleteActivity) DeleteIgroupsFromBlockProperties(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	logger := util.GetLogger(ctx)
	se := vda.SE
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	for _, hostgroup := range volume.VolumeAttributes.BlockProperties.HostGroupDetails {
		volumesWithHG, err := se.GetAllVolumesForHG(ctx, hostgroup.HostGroupUUID, volume.AccountID)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}

		// We have to check if there is any other volume using this HG
		deleteHG := true
		for _, volumeWithHG := range volumesWithHG {
			if volume.PoolID == volumeWithHG.PoolID && volume.UUID != volumeWithHG.UUID {
				deleteHG = false
				break
			}
		}

		if !deleteHG {
			logger.Debugf("Hostgroup %s has attached volume, not deleting", hostgroup.HostGroupUUID)
			continue
		}

		hostgroupDB, err := se.GetHostGroup(ctx, hostgroup.HostGroupUUID, volume.AccountID)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}

		igroup, err := provider.IgroupGet(&hostgroupDB.Name, nil)
		if err != nil {
			if errors.IsNotFoundErr(err) {
				logger.Debugf("IGroups %s is already deleted, skipping", hostgroup.HostGroupUUID)
				continue
			}
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		if igroup != nil && igroup.UUID != nil {
			err = provider.IgroupDelete(*igroup.UUID)
			if err != nil {
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}
		} else {
			logger.Debugf("Igroup %s not found for volume %s", hostgroup.HostGroupUUID, volume.UUID)
		}

		logger.Debug("Igroup deleted successfully", "name", hostgroup.HostGroupUUID)
	}
	return nil
}

func (vda VolumeDeleteActivity) DeleteIgroups(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	logger := util.GetLogger(ctx)
	se := vda.SE
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	for _, blockDevice := range *volume.VolumeAttributes.BlockDevices {
		for _, hostgroup := range blockDevice.HostGroupDetails {
			volumesWithHG, err := se.GetAllVolumesForHG(ctx, hostgroup.HostGroupUUID, volume.AccountID)
			if err != nil {
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}

			// We have to check if there is any other volume using this HG
			deleteHG := true
			for _, volumeWithHG := range volumesWithHG {
				if volume.PoolID == volumeWithHG.PoolID && volume.UUID != volumeWithHG.UUID {
					deleteHG = false
					break
				}
			}

			if !deleteHG {
				logger.Debugf("Hostgroup %s has attached volume, not deleting", hostgroup.HostGroupUUID)
				continue
			}

			hostgroupDB, err := se.GetHostGroup(ctx, hostgroup.HostGroupUUID, volume.AccountID)
			if err != nil {
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}

			igroup, err := provider.IgroupGet(&hostgroupDB.Name, nil)
			if err != nil {
				if errors.IsNotFoundErr(err) {
					logger.Debugf("IGroups %s is already deleted, skipping", hostgroup.HostGroupUUID)
					continue
				}
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}
			if igroup != nil && igroup.UUID != nil {
				err = provider.IgroupDelete(*igroup.UUID)
				if err != nil {
					return vsaerrors.WrapAsTemporalApplicationError(err)
				}
			} else {
				logger.Debugf("Igroup %s not found for volume %s", hostgroup.HostGroupUUID, volume.UUID)
			}

			logger.Debug("Igroup deleted successfully", "name", hostgroup.HostGroupUUID)
		}
	}
	return nil
}

func (vda VolumeDeleteActivity) DeleteExportPolicy(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if volume.VolumeAttributes.FileProperties.ExportPolicy.ExportPolicyName == "" {
		logger.Warnf("Volume %s has no export policy, skipping deletion", volume.Name)
		return nil
	}
	exportPolicyName := volume.VolumeAttributes.FileProperties.ExportPolicy.ExportPolicyName
	vsaExportPolicy := &vsa.ExportPolicy{
		ExportPolicyName: exportPolicyName,
		SvmName:          volume.Svm.Name,
	}
	err = provider.DeleteExportPolicy(vsaExportPolicy)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			logger.Warnf("Export policy %s not found for volume %s, skipping deletion", exportPolicyName, volume.Name)
			return nil
		}
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Export policy %s deleted successfully for volume %s", exportPolicyName, volume.Name)
	return nil
}
