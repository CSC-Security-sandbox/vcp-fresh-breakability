package replicationActivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type InternalVolumeReplicationDeleteActivity struct {
	SE database.Storage
}

func (a *InternalVolumeReplicationDeleteActivity) DeleteVolumeReplication(ctx context.Context, replication *datamodel.VolumeReplication, node *models.Node) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	vsaDeleteVolumeReplicationParams := prepareDeleteVolumeReplicationParamsVSA(replication)
	res, err := provider.DeleteVolumeReplication(vsaDeleteVolumeReplicationParams)
	if err != nil {
		if errors.IsConflictErr(err) {
			logger.Error("Failed to delete volume replication", "error", err)
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrProviderDeleteVolumeReplication, err))
		}
		logger.Error("Failed to delete volume replication", "error", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrProviderDeleteVolumeReplication, err)
	}
	return res, nil
}

func (a *InternalVolumeReplicationDeleteActivity) CleanupReplicationAfterReverse(ctx context.Context, replication *datamodel.VolumeReplication, node *models.Node) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	volumeRep := &vsa.VolumeReplication{
		EndpointType:          replication.ReplicationAttributes.EndpointType,
		SourceHostName:        replication.ReplicationAttributes.DestinationHostName,
		SourceSVMName:         replication.ReplicationAttributes.DestinationSvmName,
		SourceVolumeName:      replication.ReplicationAttributes.DestinationVolumeName,
		DestinationHostName:   replication.ReplicationAttributes.SourceHostName,
		DestinationSVMName:    replication.ReplicationAttributes.SourceSvmName,
		ReplicationSchedule:   replication.ReplicationAttributes.ReplicationSchedule,
		DestinationVolumeName: replication.ReplicationAttributes.SourceVolumeName,
		RelationshipID:        replication.ReplicationAttributes.ExternalUUID,
		IsCleanup:             true,
	}
	vsaDeleteVolumeReplicationParams := &vsa.DeleteVolumeReplicationParams{
		VolumeReplication: volumeRep,
	}
	res, err := provider.DeleteVolumeReplication(vsaDeleteVolumeReplicationParams)
	if err != nil {
		if errors.IsConflictErr(err) {
			logger.Error("Failed to cleanup volume replication", "error", err)
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrProviderDeleteVolumeReplication, err))
		}
		logger.Error("Failed to cleanup volume replication", "error", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrProviderDeleteVolumeReplication, err)
	}
	return res, nil
}

func prepareDeleteVolumeReplicationParamsVSA(volumeReplication *datamodel.VolumeReplication) *vsa.DeleteVolumeReplicationParams {
	params := false
	replication := &vsa.VolumeReplication{
		EndpointType:          volumeReplication.ReplicationAttributes.EndpointType,
		SourceHostName:        volumeReplication.ReplicationAttributes.SourceHostName,
		SourceSVMName:         volumeReplication.ReplicationAttributes.SourceSvmName,
		SourceVolumeName:      volumeReplication.ReplicationAttributes.SourceVolumeName,
		DestinationHostName:   volumeReplication.ReplicationAttributes.DestinationHostName,
		DestinationSVMName:    volumeReplication.ReplicationAttributes.DestinationSvmName,
		ReplicationSchedule:   volumeReplication.ReplicationAttributes.ReplicationSchedule,
		DestinationVolumeName: volumeReplication.ReplicationAttributes.DestinationVolumeName,
		RelationshipID:        volumeReplication.ReplicationAttributes.ExternalUUID,
	}
	return &vsa.DeleteVolumeReplicationParams{
		VolumeReplication: replication,
		DestinationOnly:   &params,
		SourceOnly:        &params,
	}
}

func (a *InternalVolumeReplicationDeleteActivity) UpdateReplicationStateInDBForDelete(ctx context.Context, volumeRep *datamodel.VolumeReplication) error {
	se := a.SE
	volumeRep.State = models.LifeCycleStateError
	volumeRep.StateDetails = models.LifeCycleStateDeletionErrorDetails
	if err := se.UpdateVolumeReplicationStates(ctx, volumeRep); err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}
