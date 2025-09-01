package replicationActivities

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerror "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type InternalStopVolumeReplicationActivity struct {
	SE database.Storage
}

func (j *InternalStopVolumeReplicationActivity) GetReplicationFromDB(ctx context.Context, uuid string) (*datamodel.VolumeReplication, error) {
	se := j.SE

	replication, err := se.GetVolumeReplication(ctx, uuid)
	if err != nil {
		return nil, vsaerror.NewVCPError(vsaerror.ErrDatabaseDataReadError, err)
	}
	return replication, nil
}

func (j *InternalStopVolumeReplicationActivity) BreakVolumeReplication(ctx context.Context, replication *datamodel.VolumeReplication, node *models.Node) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	provider, err := activitiesGetProviderByNode(ctx, node)
	if err != nil {
		logger.Error("Failed to get provider by node", "error", err)
		return nil, vsaerror.WrapAsTemporalApplicationError(err)
	}
	vsaReplication := &vsa.VolumeReplication{
		ExternalUUID: replication.ReplicationAttributes.ExternalUUID,
	}
	snapmirror, err := provider.GetVolumeReplication(vsaReplication)
	if err != nil {
		logger.Error("Failed to get volume replication details", "error", err)
		return nil, vsaerror.WrapAsNonRetryableTemporalApplicationError(vsaerror.NewVCPError(vsaerror.ErrProviderGetVolumeReplication, err))
	}
	if snapmirror.RelationshipStatus == models.SnapmirrorRelationshipTransferring {
		return nil, vsaerror.WrapAsNonRetryableTemporalApplicationError(vsaerror.NewVCPError(vsaerror.ErrProviderBreakVolumeReplication, errors.New("Replication is in transferring state, cannot stop replication")))
	}
	snapmirror.MirrorState = models.OntapBrokenOff

	_, err = provider.BreakVolumeReplication(snapmirror)
	if err != nil {
		logger.Error("Failed to break volume replication", "error", err)
		return nil, vsaerror.WrapAsNonRetryableTemporalApplicationError(vsaerror.NewVCPError(vsaerror.ErrProviderBreakVolumeReplication, err))
	}
	return snapmirror, nil
}

func (j *InternalStopVolumeReplicationActivity) AbortVolumeReplication(ctx context.Context, replication *datamodel.VolumeReplication, node *models.Node, forcestop bool) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	if !forcestop {
		logger.Info("Force is not set to true, skipping abort volume replication")
		return nil, nil
	}
	provider, err := activitiesGetProviderByNode(ctx, node)
	if err != nil {
		logger.Error("Failed to get provider by node", "error", err)
		return nil, vsaerror.WrapAsNonRetryableTemporalApplicationError(vsaerror.NewVCPError(vsaerror.ErrGCPClientInitializationError, err))
	}
	vsaReplication := &vsa.VolumeReplication{
		ExternalUUID: replication.ReplicationAttributes.ExternalUUID,
	}
	snapmirror, err := provider.GetVolumeReplication(vsaReplication)
	if err != nil {
		logger.Error("Failed to get volume replication details", "error", err)
		return nil, vsaerror.WrapAsNonRetryableTemporalApplicationError(vsaerror.NewVCPError(vsaerror.ErrProviderGetVolumeReplication, err))
	}

	if snapmirror.RelationshipStatus != models.SnapmirrorRelationshipTransferring {
		logger.Info("Replication is not in transferring state, skipping abort volume replication")
		return vsaReplication, nil
	}

	if snapmirror.TransferUUID == "" {
		return vsaReplication, nil
	}

	vsaReplication.TransferUUID = snapmirror.TransferUUID
	vsaReplication.RelationshipStatus = models.SnapmirrorRelationshipAborted
	_, err = provider.AbortVolumeReplication(vsaReplication)
	if err != nil {
		logger.Error("Failed to abort volume replication", "error", err)
		return nil, vsaerror.WrapAsNonRetryableTemporalApplicationError(vsaerror.NewVCPError(vsaerror.ErrProviderAbortVolumeReplication, err))
	}
	return vsaReplication, nil
}

func (j *InternalStopVolumeReplicationActivity) GetSnapMirrorFromOntap(ctx context.Context, dbReplication *datamodel.VolumeReplication, node *models.Node) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	provider, err := activitiesGetProviderByNode(ctx, node)
	if err != nil {
		logger.Error("Failed to get provider by node", "error", err)
		return nil, vsaerror.WrapAsNonRetryableTemporalApplicationError(vsaerror.NewVCPError(vsaerror.ErrGCPClientInitializationError, err))
	}
	replicationParams := convertToSnapmirrorGetParams(dbReplication, dbReplication.Account.Name)
	ontapRep, err := provider.GetReplicationDetails(ctx, replicationParams)
	if err != nil {
		logger.Errorf("Failed to get replication details from Ontap for replication %s: %v", dbReplication.UUID, err)
		return nil, vsaerror.WrapAsNonRetryableTemporalApplicationError(vsaerror.NewVCPError(vsaerror.ErrProviderGetVolumeReplication, err))
	}
	return ontapRep, nil
}

func (j *InternalStopVolumeReplicationActivity) UpdateVolumeReplicationStopDetails(ctx context.Context, replication *datamodel.VolumeReplication, vsaReplication *vsa.VolumeReplication) error {
	se := j.SE

	replication.State = models.LifeCycleStateAvailable
	replication.StateDetails = models.LifeCycleStateAvailableDetails
	replication.MirrorState = &vsaReplication.MirrorState
	replication.RelationshipStatus = &vsaReplication.RelationshipStatus
	replication.TotalTransferBytes = vsaReplication.TotalTransferBytes
	replication.TotalTransferTimeSecs = vsaReplication.TotalTransferTimeSecs
	replication.LastTransferSize = vsaReplication.LastTransferSize
	replication.LastTransferError = vsaReplication.LastTransferError
	replication.LastTransferDuration = vsaReplication.LastTransferDuration
	replication.LastTransferEndTime = vsaReplication.LastTransferEndTime
	replication.LagTime = vsaReplication.LagTime
	replication.LastUpdatedFromOntap = time.Now()
	replication.ProgressLastUpdated = &replication.LastUpdatedFromOntap

	if err := se.UpdateVolumeReplication(ctx, replication); err != nil {
		return vsaerror.NewVCPError(vsaerror.ErrDatabaseDataUpdateError, err)
	}

	return nil
}

func (j *InternalStopVolumeReplicationActivity) UpdateVolumeToNonDPVolume(ctx context.Context, replication *datamodel.VolumeReplication) error {
	se := j.SE
	updates := make(map[string]interface{})
	if replication.Volume.VolumeAttributes != nil {
		replication.Volume.VolumeAttributes.IsDataProtection = false
	}
	updates["volume_attributes"] = replication.Volume.VolumeAttributes
	err := se.UpdateVolumeFields(ctx, replication.ReplicationAttributes.DestinationVolumeUUID, updates)
	if err != nil {
		return vsaerror.NewVCPError(vsaerror.ErrDatabaseDataUpdateError, err)
	}
	return nil
}
