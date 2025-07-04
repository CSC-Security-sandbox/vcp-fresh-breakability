package replicationActivities

import (
	"context"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type InternalVolumeReplicationActivity struct {
	SE database.Storage
}

func (a *InternalVolumeReplicationActivity) CreateVolumeReplicationInternal(ctx context.Context, params *common.CreateVolumeReplicationInternalParams, node *models.Node, volumeExternalUUID string) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	provider, err := activities.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	vsaCreateVolumeReplicationParams := prepareCreateVolumeReplicationParamsVSA(params, volumeExternalUUID)
	res, err := provider.CreateVolumeReplication(vsaCreateVolumeReplicationParams)
	if err != nil {
		logger.Error("Failed to create volume replication", "error", err)
		return nil, err
	}
	return res, nil
}

func (a *InternalVolumeReplicationActivity) UpdateVolumeReplicationDetails(ctx context.Context, replication *datamodel.VolumeReplication, replicationCreateResponseONTAP *vsa.VolumeReplication) error {
	se := a.SE

	replication.State = models.LifeCycleStateAvailable
	replication.StateDetails = models.LifeCycleStateAvailableDetails
	replication.ReplicationAttributes.ExternalUUID = replicationCreateResponseONTAP.RelationshipID
	replication.MirrorState = &replicationCreateResponseONTAP.MirrorState
	replication.RelationshipStatus = &replicationCreateResponseONTAP.RelationshipStatus
	replication.TotalTransferBytes = replicationCreateResponseONTAP.TotalTransferBytes
	replication.TotalTransferTimeSecs = replicationCreateResponseONTAP.TotalTransferTimeSecs
	replication.LastTransferSize = int64(replicationCreateResponseONTAP.LastTransferSize)
	replication.LastTransferError = replicationCreateResponseONTAP.LastTransferError
	replication.LastTransferDuration = replicationCreateResponseONTAP.LastTransferDuration
	replication.LastTransferEndTime = replicationCreateResponseONTAP.LastTransferEndTime
	replication.LagTime = replicationCreateResponseONTAP.LagTime
	replication.LastUpdatedFromOntap = time.Now()
	replication.ProgressLastUpdated = &replication.LastUpdatedFromOntap

	if err := se.UpdateVolumeReplication(ctx, replication); err != nil {
		return err
	}

	return nil
}

func prepareCreateVolumeReplicationParamsVSA(params *common.CreateVolumeReplicationInternalParams, volumeExternalUUID string) *vsa.CreateVolumeReplicationParams {
	volumeReplication := params.VolumeReplication
	vrf := &vsa.VolumeReplication{
		EndpointType:          volumeReplication.ReplicationAttributes.EndpointType,
		SourceHostName:        volumeReplication.ReplicationAttributes.SourceHostName,
		SourceSVMName:         volumeReplication.ReplicationAttributes.SourceSvmName,
		SourceVolumeName:      volumeReplication.ReplicationAttributes.SourceVolumeName,
		DestinationHostName:   volumeReplication.ReplicationAttributes.DestinationHostName,
		DestinationSVMName:    volumeReplication.ReplicationAttributes.DestinationSvmName,
		ReplicationSchedule:   volumeReplication.ReplicationAttributes.ReplicationSchedule,
		ReplicationPolicy:     volumeReplication.ReplicationAttributes.ReplicationPolicy,
		DestinationVolumeName: volumeReplication.ReplicationAttributes.DestinationVolumeName,
		Volume: &vsa.Volume{
			ExternalUUID: volumeExternalUUID,
		},
	}
	return &vsa.CreateVolumeReplicationParams{
		VolumeReplication: vrf,
		ReverseResync:     params.ReverseResync,
	}
}

func (a *InternalVolumeReplicationActivity) HydrateReplicationCreate(ctx context.Context, replicationDb *datamodel.VolumeReplication, accountName string) error {
	if hydrationEnabled {
		err := HydrateVolumeReplication(ctx, convertReplicationDbModelToDataModel(replicationDb), accountName)
		if err != nil {
			util.GetLogger(ctx).Error("Error hydrating replication create", "error", err)
			return err
		}
	}
	return nil
}

func convertReplicationDbModelToDataModel(replicationDb *datamodel.VolumeReplication) models.VolumeReplication {
	return models.VolumeReplication{
		Name:  replicationDb.Name,
		State: strings.ToLower(models.LifeCycleStateAvailable),
		ReplicationAttributes: &models.ReplicationDetails{
			DestinationRegion:     replicationDb.ReplicationAttributes.DestinationLocation,
			DestinationVolumeName: replicationDb.ReplicationAttributes.DestinationVolumeName,
		},
	}
}
