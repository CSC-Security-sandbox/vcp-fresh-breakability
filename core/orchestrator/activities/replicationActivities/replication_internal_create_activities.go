package replicationActivities

import (
	"context"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
)

// safeRecordHeartbeat safely records a heartbeat only if the context is an activity context.
// This prevents panics when the function is called from non-activity contexts (e.g., unit tests).
func safeRecordHeartbeat(ctx context.Context, details ...interface{}) {
	defer func() {
		if r := recover(); r != nil {
			// Ignore panic - we're not in an activity context
		}
	}()
	activity.RecordHeartbeat(ctx, details...)
}

type InternalVolumeReplicationActivity struct {
	SE database.Storage
}

func (a *InternalVolumeReplicationActivity) CreateVolumeReplicationInternal(ctx context.Context, params *common.CreateVolumeReplicationInternalParams, node *models.Node, volumeExternalUUID string) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("CreateVolumeReplicationInternal")
	safeRecordHeartbeat(ctx, "CreateVolumeReplicationInternal started")

	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	safeRecordHeartbeat(ctx, "Got provider, creating volume replication on ONTAP")

	vsaCreateVolumeReplicationParams := prepareCreateVolumeReplicationParamsVSA(params, volumeExternalUUID)
	res, err := provider.CreateVolumeReplication(vsaCreateVolumeReplicationParams)
	if err != nil {
		logger.Error("Failed to create volume replication", "error", err)
		return nil, err
	}
	safeRecordHeartbeat(ctx, "CreateVolumeReplicationInternal completed")
	return res, nil
}

func (a *InternalVolumeReplicationActivity) UpdateVolumeReplicationDetails(ctx context.Context, replication *datamodel.VolumeReplication, replicationCreateResponseONTAP *vsa.VolumeReplication, params *common.UpdateVolumeReplicationInternalParams) error {
	safeRecordHeartbeat(ctx, "UpdateVolumeReplicationDetails started")
	se := a.SE
	logger := util.GetLogger(ctx)

	replication.State = models.LifeCycleStateAvailable
	replication.StateDetails = models.LifeCycleStateAvailableDetails
	if replicationCreateResponseONTAP != nil {
		replication.ReplicationAttributes.ExternalUUID = replicationCreateResponseONTAP.RelationshipID
		replication.ReplicationAttributes.ReplicationSchedule = replicationCreateResponseONTAP.ReplicationSchedule
		replication.MirrorState = &replicationCreateResponseONTAP.MirrorState
		replication.RelationshipStatus = &replicationCreateResponseONTAP.RelationshipStatus
		replication.TotalTransferBytes = replicationCreateResponseONTAP.TotalTransferBytes
		replication.TotalTransferTimeSecs = replicationCreateResponseONTAP.TotalTransferTimeSecs
		replication.LastTransferSize = int64(replicationCreateResponseONTAP.LastTransferSize)
		replication.LastTransferError = replicationCreateResponseONTAP.LastTransferError
		replication.LastTransferDuration = replicationCreateResponseONTAP.LastTransferDuration
		replication.LastTransferEndTime = replicationCreateResponseONTAP.LastTransferEndTime
		replication.LagTime = replicationCreateResponseONTAP.LagTime
	}
	replication.LastUpdatedFromOntap = time.Now()
	replication.ProgressLastUpdated = &replication.LastUpdatedFromOntap

	if params != nil {
		if params.Description != nil {
			replication.Description = *params.Description
		}
		if params.Labels != nil {
			replication.ReplicationAttributes.Labels = params.Labels
		}
	}

	if err := se.UpdateVolumeReplication(ctx, replication); err != nil {
		return err
	}
	logger.Debug("Successfully updated VolumeReplicationDetails after creation on ONTAP")
	safeRecordHeartbeat(ctx, "UpdateVolumeReplicationDetails completed")
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
	safeRecordHeartbeat(ctx, "HydrateReplicationCreate started")
	logger := util.GetLogger(ctx)
	if hydrationEnabled {
		logger.Debug("HydrateReplicationCreate")
		err := HydrateVolumeReplication(ctx, convertReplicationDbModelToDataModel(replicationDb), accountName)
		if err != nil {
			util.GetLogger(ctx).Error("Error hydrating replication create", "error", err)
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrHydrateVolumeReplicationCreate, err))
		}
	}
	safeRecordHeartbeat(ctx, "HydrateReplicationCreate completed")
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
