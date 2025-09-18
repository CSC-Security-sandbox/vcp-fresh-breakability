package backgroundactivities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// VolumeBackupSyncActivity handles activities for syncing volume and backup logical sizes
type VolumeBackupSyncActivity struct {
	SE database.Storage
}

// GetVolumeLatestBackupMapActivity retrieves volumes with their associated latest backups
func (a *VolumeBackupSyncActivity) GetVolumeLatestBackupMapActivity(ctx context.Context) (map[int64]*datamodel.VolumeLatestBackup, error) {
	logger := util.GetLogger(ctx)

	volumeBackupMap, err := a.SE.GetVolumeLatestBackupMap(ctx)
	if err != nil {
		logger.Errorf("Failed to get volume latest backup map: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Retrieved %d volume-backup mappings", len(volumeBackupMap))
	return volumeBackupMap, nil
}

// getObjectStoreEndpointInfo gets object store endpoint info using the provided node and UUIDs
func (a *VolumeBackupSyncActivity) getObjectStoreEndpointInfo(ctx context.Context, objectStoreUUID, endpointUUID string, volumeBackup *datamodel.VolumeLatestBackup) (*vsa.SmObjectStoreEndpointt, error) {
	dbNodes, err := a.SE.GetNodesByPoolID(ctx, volumeBackup.Volume.PoolID)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if len(dbNodes) == 0 {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrUnexpectedNodeCountForPool, errors.New("Node not found for the pool")))
	}

	// Prepare node provider input
	nodeProviderInput := hyperscaler.NodeProviderInput{
		Nodes:          dbNodes,
		Password:       volumeBackup.Volume.Pool.PoolCredentials.Password,
		SecretID:       volumeBackup.Volume.Pool.PoolCredentials.SecretID,
		DeploymentName: volumeBackup.Volume.Pool.DeploymentName,
		CertificateID:  volumeBackup.Volume.Pool.PoolCredentials.CertificateID,
		AuthType:       volumeBackup.Volume.Pool.PoolCredentials.AuthType,
	}

	node := hyperscaler.CreateNodeForProvider(nodeProviderInput)

	// Get the provider and fetch object store endpoint info
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, err
	}

	objStoreEndpointInfo, err := provider.ObjectStoreEndpointInfoGet(objectStoreUUID, endpointUUID)
	if err != nil {
		return nil, err
	}

	return objStoreEndpointInfo, nil
}

// GetObjectStoreEndpointInfoActivity gets object store endpoint info for a volume
func (a *VolumeBackupSyncActivity) GetObjectStoreEndpointInfoActivity(ctx context.Context, volumeBackup *datamodel.VolumeLatestBackup) (*vsa.SmObjectStoreEndpointt, error) {
	logger := util.GetLogger(ctx)

	if volumeBackup.LatestBackup == nil {
		logger.Infof("No latest backup found for volume %s, skipping", volumeBackup.Volume.Name)
		return nil, nil
	}

	// Check if backup has required attributes
	if volumeBackup.LatestBackup.Attributes == nil ||
		volumeBackup.LatestBackup.Attributes.ObjectStoreUUID == "" ||
		volumeBackup.LatestBackup.Attributes.EndpointUUID == "" {
		logger.Infof("Backup %s missing required attributes (ObjectStoreUUID or EndpointUUID), skipping", volumeBackup.LatestBackup.Name)
		return nil, nil
	}

	// Get object store endpoint info to fetch logical size
	objStoreEndpointInfo, err := a.getObjectStoreEndpointInfo(ctx,
		volumeBackup.LatestBackup.Attributes.ObjectStoreUUID,
		volumeBackup.LatestBackup.Attributes.EndpointUUID, volumeBackup)
	if err != nil {
		logger.Errorf("Failed to get object store endpoint info for backup %s: %v", volumeBackup.LatestBackup.Name, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return objStoreEndpointInfo, nil
}

// UpdateBackupAndVolumeActivity updates backup and volume with logical size information
func (a *VolumeBackupSyncActivity) UpdateBackupAndVolumeActivity(ctx context.Context, volumeBackup *datamodel.VolumeLatestBackup, logicalSize int64) error {
	logger := util.GetLogger(ctx)

	if volumeBackup.LatestBackup == nil {
		logger.Infof("No latest backup found for volume %s, skipping update", volumeBackup.Volume.Name)
		return nil
	}

	// Update backup with latest logical size using UpdateBackupFields
	backupUpdates := map[string]interface{}{
		"latest_logical_backup_size": logicalSize,
	}
	err := a.SE.UpdateBackupFields(ctx, volumeBackup.LatestBackup.UUID, backupUpdates)
	if err != nil {
		logger.Errorf("Failed to update backup %s with logical size: %v", volumeBackup.LatestBackup.Name, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Update volume's latest logical backup size
	volumeUpdates := make(map[string]interface{})
	// LatestLogicalBackupSize(backup datamodel) is equivalent to BackupChainBytes(volume datamodel) and chainStorageBytes
	volumeBackup.Volume.DataProtection.BackupChainBytes = &logicalSize
	volumeUpdates["data_protection"] = volumeBackup.Volume.DataProtection
	err = a.SE.UpdateVolumeFields(ctx, volumeBackup.Volume.UUID, volumeUpdates)
	if err != nil {
		logger.Errorf("Failed to update volume %s with latest logical backup size: %v", volumeBackup.Volume.Name, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully updated logical size %d for volume %s and backup %s",
		logicalSize, volumeBackup.Volume.Name, volumeBackup.LatestBackup.Name)

	return nil
}
