package replicationActivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type InternalVolumeReplicationReverseActivity struct {
	SE database.Storage
}

func (a *InternalVolumeReplicationReverseActivity) ReverseVolumeReplication(ctx context.Context, params *common.CreateVolumeReplicationInternalParams, node *models.Node, volumeExternalUUID string) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("CreateVolumeReplicationInternal in reverse direction")

	provider, err := vsa.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	volumeReplication := params.VolumeReplication
	vrf := &vsa.VolumeReplication{
		EndpointType:          "dst",
		SourceHostName:        volumeReplication.ReplicationAttributes.DestinationHostName,
		SourceSVMName:         volumeReplication.ReplicationAttributes.DestinationSvmName,
		SourceVolumeName:      volumeReplication.ReplicationAttributes.DestinationVolumeName,
		DestinationHostName:   volumeReplication.ReplicationAttributes.SourceHostName,
		DestinationSVMName:    volumeReplication.ReplicationAttributes.SourceSvmName,
		ReplicationSchedule:   volumeReplication.ReplicationAttributes.ReplicationSchedule,
		ReplicationPolicy:     volumeReplication.ReplicationAttributes.ReplicationPolicy,
		DestinationVolumeName: volumeReplication.ReplicationAttributes.SourceVolumeName,
		Volume: &vsa.Volume{
			ExternalUUID: volumeExternalUUID,
		},
	}
	createReplicationParams := &vsa.CreateVolumeReplicationParams{
		VolumeReplication: vrf,
		ReverseResync:     params.ReverseResync,
	}
	res, err := provider.CreateVolumeReplication(createReplicationParams)
	if err != nil {
		logger.Error("Failed to create volume replication in reverse direction", "error", err)
		return nil, err
	}
	return res, nil
}

func (a *InternalVolumeReplicationReverseActivity) UpdateVolumeTypeForNewDestination(ctx context.Context, replication *datamodel.VolumeReplication) error {
	se := a.SE
	logger := util.GetLogger(ctx)
	logger.Infof("UpdateVolumeTypeForReverse")

	destUpdates := make(map[string]interface{})
	if replication.Volume.VolumeAttributes != nil {
		replication.Volume.VolumeAttributes.IsDataProtection = true
	}
	destUpdates["volume_attributes"] = replication.Volume.VolumeAttributes

	err := se.UpdateVolumeFields(ctx, replication.ReplicationAttributes.SourceVolumeUUID, destUpdates)
	if err != nil {
		return err
	}
	return nil
}

// ConvertReplicationDataModelToModel converts a datamodel.VolumeReplication to models.VolumeReplication
func ConvertReplicationDataModelToModel(replication *datamodel.VolumeReplication) *models.VolumeReplication {
	if replication == nil {
		return nil
	}

	result := &models.VolumeReplication{
		BaseModel: models.BaseModel{
			UUID:      replication.UUID,
			CreatedAt: replication.CreatedAt,
			UpdatedAt: replication.UpdatedAt,
		},
		Name:        replication.Name,
		Description: replication.Description,
		State:       replication.State,
		Uri:         replication.Uri,
		RemoteUri:   replication.RemoteUri,
		AccountID:   replication.AccountID,
		VolumeID:    replication.VolumeID,
	}

	// Convert ReplicationAttributes if present
	if replication.ReplicationAttributes != nil {
		result.ReplicationAttributes = &models.ReplicationDetails{
			EndpointType:               replication.ReplicationAttributes.EndpointType,
			ReplicationType:            replication.ReplicationAttributes.ReplicationType,
			ReplicationSchedule:        replication.ReplicationAttributes.ReplicationSchedule,
			SourcePoolUUID:             replication.ReplicationAttributes.SourcePoolUUID,
			SourceVolumeUUID:           replication.ReplicationAttributes.SourceVolumeUUID,
			SourceRegion:               replication.ReplicationAttributes.SourceLocation,
			SourceHostName:             replication.ReplicationAttributes.SourceHostName,
			SourceReplicationUUID:      replication.ReplicationAttributes.SourceReplicationUUID,
			SourceSvmName:              replication.ReplicationAttributes.SourceSvmName,
			SourceVolumeName:           replication.ReplicationAttributes.SourceVolumeName,
			DestinationPoolUUID:        replication.ReplicationAttributes.DestinationPoolUUID,
			DestinationVolumeUUID:      replication.ReplicationAttributes.DestinationVolumeUUID,
			DestinationRegion:          replication.ReplicationAttributes.DestinationLocation,
			DestinationHostName:        replication.ReplicationAttributes.DestinationHostName,
			DestinationReplicationUUID: replication.ReplicationAttributes.DestinationReplicationUUID,
			DestinationSvmName:         replication.ReplicationAttributes.DestinationSvmName,
			DestinationVolumeName:      replication.ReplicationAttributes.DestinationVolumeName,
		}
	}

	return result
}
