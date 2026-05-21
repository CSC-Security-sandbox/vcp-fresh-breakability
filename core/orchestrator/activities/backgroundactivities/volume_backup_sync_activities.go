package backgroundactivities

import (
	"context"
	"fmt"

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

// volumeBackupName returns a display name for logging, regardless of whether the entry is
// a regular volume or an expert mode volume.
func volumeBackupName(volumeBackup *datamodel.VolumeLatestBackup) string {
	if volumeBackup.Volume != nil {
		return volumeBackup.Volume.Name
	}
	if volumeBackup.ExpertModeVolume != nil {
		return volumeBackup.ExpertModeVolume.Name
	}
	return "unknown"
}

// getObjectStoreEndpointInfo gets object store endpoint info using the provided node and UUIDs.
// It supports both regular volumes (volumeBackup.Volume) and expert mode volumes
// (volumeBackup.ExpertModeVolume).
func (a *VolumeBackupSyncActivity) getObjectStoreEndpointInfo(ctx context.Context, objectStoreUUID, endpointUUID string, volumeBackup *datamodel.VolumeLatestBackup) (*vsa.SmObjectStoreEndpointt, error) {
	var poolID int64
	var pool *datamodel.Pool
	if volumeBackup.ExpertModeVolume != nil {
		poolID = volumeBackup.ExpertModeVolume.PoolID
		pool = volumeBackup.ExpertModeVolume.Pool
	} else {
		poolID = volumeBackup.Volume.PoolID
		pool = volumeBackup.Volume.Pool
	}

	dbNodes, err := a.SE.GetNodesByPoolID(ctx, poolID)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if len(dbNodes) == 0 {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrUnexpectedNodeCountForPool, errors.New("Node not found for the pool")))
	}

	// Prepare node provider input
	if pool == nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, fmt.Errorf("pool not found for pool %d", poolID)))
	}
	if pool.PoolCredentials == nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, fmt.Errorf("pool credentials not found for pool %d", poolID)))
	}
	nodeProviderInput := hyperscaler.NodeProviderInput{
		Nodes:            dbNodes,
		DeploymentName:   pool.DeploymentName,
		OntapCredentials: pool.PoolCredentials,
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
		logger.Infof("No latest backup found for volume %s, skipping", volumeBackupName(volumeBackup))
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
		logger.Infof("No latest backup found for volume %s, skipping update", volumeBackupName(volumeBackup))
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

	// Update volume's latest logical backup size.
	// LatestLogicalBackupSize (backup model) is equivalent to BackupChainBytes (volume model).
	// Branch on which volume pointer is populated rather than on the backup attribute flag so
	// that a missing or inconsistent pointer produces a clear error instead of a nil panic.
	volumeUpdates := make(map[string]interface{})
	switch {
	case volumeBackup.ExpertModeVolume != nil:
		if volumeBackup.ExpertModeVolume.BackupConfig == nil {
			volumeBackup.ExpertModeVolume.BackupConfig = &datamodel.DataProtection{}
		}
		volumeBackup.ExpertModeVolume.BackupConfig.BackupChainBytes = &logicalSize
		volumeUpdates["data_protection"] = volumeBackup.ExpertModeVolume.BackupConfig
		// UpdateExpertModeVolumeFields looks up expert mode volumes by ExternalUUID.
		err = a.SE.UpdateExpertModeVolumeFields(ctx, volumeBackup.ExpertModeVolume.ExternalUUID, volumeUpdates)
		if err != nil {
			logger.Errorf("Failed to update expert mode volume %s with latest logical backup size: %v", volumeBackupName(volumeBackup), err)
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	case volumeBackup.Volume != nil:
		if volumeBackup.Volume.DataProtection == nil {
			volumeBackup.Volume.DataProtection = &datamodel.DataProtection{}
		}
		volumeBackup.Volume.DataProtection.BackupChainBytes = &logicalSize
		volumeUpdates["data_protection"] = volumeBackup.Volume.DataProtection
		err = a.SE.UpdateVolumeFields(ctx, volumeBackup.Volume.UUID, volumeUpdates)
		if err != nil {
			logger.Errorf("Failed to update volume %s with latest logical backup size: %v", volumeBackup.Volume.Name, err)
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	default:
		return vsaerrors.WrapAsTemporalApplicationError(
			fmt.Errorf("volume backup entry (backup %s) has neither Volume nor ExpertModeVolume populated", volumeBackup.LatestBackup.Name),
		)
	}

	// Update backup chain history (method checks if size actually changed).
	// Derive the UUID from the volume pointer directly rather than from backup.VolumeUUID:
	// backup.VolumeUUID carries the same value by design, but it has no JSON tag and may
	// not survive Temporal's JSON serialization round-trip when the activity input is decoded.
	// - Regular volumes:    volume.UUID           == backup.VolumeUUID
	// - Expert mode volumes: expertModeVolume.ExternalUUID == backup.VolumeUUID
	//                        (= resource_uuid stored in backup_chain_histories)
	var chainHistoryUUID string
	if volumeBackup.ExpertModeVolume != nil {
		chainHistoryUUID = volumeBackup.ExpertModeVolume.ExternalUUID
	} else {
		chainHistoryUUID = volumeBackup.Volume.UUID
	}
	err = a.SE.UpdateBackupChainHistory(ctx, chainHistoryUUID, logicalSize)
	if err != nil {
		logger.Warnf("Failed to update backup chain history for volume %s: %v", volumeBackupName(volumeBackup), err)
		// Don't fail the entire operation if history update fails
	}

	logger.Infof("Successfully updated logical size %d for volume %s and backup %s",
		logicalSize, volumeBackupName(volumeBackup), volumeBackup.LatestBackup.Name)

	return nil
}
