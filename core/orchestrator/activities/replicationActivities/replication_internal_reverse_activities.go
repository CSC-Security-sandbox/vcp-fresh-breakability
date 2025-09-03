package replicationActivities

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type InternalVolumeReplicationReverseActivity struct {
	SE database.Storage
}

func (a *InternalVolumeReplicationReverseActivity) ReverseVolumeReplication(ctx context.Context, replication *datamodel.VolumeReplication, node *models.Node) (*vsa.SnapmirrorDestination, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Use our newly created SnapmirrorRelationshipReverse function
	vsaReverseParams := &vsa.VolumeReplication{
		RelationshipID:        replication.ReplicationAttributes.ExternalUUID,
		SourceVolumeName:      replication.ReplicationAttributes.SourceVolumeName,
		SourceSVMName:         replication.ReplicationAttributes.SourceSvmName,
		DestinationVolumeName: replication.ReplicationAttributes.DestinationVolumeName,
		DestinationSVMName:    replication.ReplicationAttributes.DestinationSvmName,
		Volume: &vsa.Volume{
			ExternalUUID: replication.Volume.VolumeAttributes.ExternalUUID,
		},
	}

	resp, err := provider.ReverseVolumeReplication(vsaReverseParams)
	if err != nil {
		if errors.IsConflictErr(err) {
			return nil, errors.NewNonRetryableErr(err.Error())
		}
		logger.Error("Failed to reverse volume replication", "error", err)
		return nil, err
	}

	return resp, nil
}

func (a *InternalVolumeReplicationReverseActivity) UpdateVolumeReplicationReverseDetails(ctx context.Context, replication *datamodel.VolumeReplication) error {
	logger := util.GetLogger(ctx)
	replicationAttributes := replication.ReplicationAttributes
	// Swap all source and destination details after reversal
	if replicationAttributes != nil {
		oldReplicationAttributes := *replicationAttributes
		
		// Swap source and destination fields
		replicationAttributes.SourcePoolUUID = oldReplicationAttributes.DestinationPoolUUID
		replicationAttributes.SourceVolumeUUID = oldReplicationAttributes.DestinationVolumeUUID
		replicationAttributes.SourceLocation = oldReplicationAttributes.DestinationLocation
		replicationAttributes.SourceHostName = oldReplicationAttributes.DestinationHostName
		replicationAttributes.SourceReplicationUUID = oldReplicationAttributes.DestinationReplicationUUID
		replicationAttributes.SourceSvmName = oldReplicationAttributes.DestinationSvmName
		replicationAttributes.SourceVolumeName = oldReplicationAttributes.DestinationVolumeName

		replicationAttributes.DestinationPoolUUID = oldReplicationAttributes.SourcePoolUUID
		replicationAttributes.DestinationVolumeUUID = oldReplicationAttributes.SourceVolumeUUID
		replicationAttributes.DestinationLocation = oldReplicationAttributes.SourceLocation
		replicationAttributes.DestinationHostName = oldReplicationAttributes.SourceHostName
		replicationAttributes.DestinationReplicationUUID = oldReplicationAttributes.SourceReplicationUUID
		replicationAttributes.DestinationSvmName = oldReplicationAttributes.SourceSvmName
		replicationAttributes.DestinationVolumeName = oldReplicationAttributes.SourceVolumeName

		replicationAttributes.EndpointType = "src"
		replicationAttributes.ExternalUUID = ""
	}

	updates := make(map[string]interface{})
	updates["mirror_state"] = nillable.GetStringPtr("")
	updates["total_transfer_bytes"] = 0
	updates["total_transfer_time_secs"] = 0
	updates["last_transfer_size"] = int64(0)
	updates["last_transfer_error"] = ""
	updates["last_transfer_duration"] = 0
	updates["last_transfer_end_time"] = nil
	updates["lag_time"] = 0
	updates["last_updated_from_ontap"] = time.Now()
	updates["progress_last_updated"] = time.Now()

	updates["replication_attributes"] = replication.ReplicationAttributes
	updates["state"] = models.LifeCycleStateREADY
	updates["state_details"] = models.LifeCycleStateAvailableDetails

	// Update the volume replication in the database
	err := a.SE.UpdateVolumeReplicationFields(ctx, replication.UUID, updates)
	if err != nil {
		logger.Errorf("Failed to update volume replication %s in the database: %v", replication.Name, err)
		return err
	}

	logger.Debugf("Volume Replication %s updated successfully in the database", replication.Name)
	return nil
}

func (a *InternalVolumeReplicationReverseActivity) UpdateVolumeTypeForReverse(ctx context.Context, replication *datamodel.VolumeReplication) error {
	se := a.SE

	destUpdates := make(map[string]interface{})
	if replication.Volume.VolumeAttributes != nil {
		replication.Volume.VolumeAttributes.IsDataProtection = false
	}
	destUpdates["volume_attributes"] = replication.Volume.VolumeAttributes

	err := se.UpdateVolumeFields(ctx, replication.ReplicationAttributes.DestinationVolumeUUID, destUpdates)
	if err != nil {
		return err
	}
	return nil
}
