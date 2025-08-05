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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type InternalVolumeReplicationResumeActivity struct {
	SE database.Storage
}

func (a *InternalVolumeReplicationResumeActivity) ResumeVolumeReplication(ctx context.Context, replication *datamodel.VolumeReplication, node *models.Node, forceResume bool) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	vsaResumeParams := &vsa.VolumeReplication{
		MirrorState:    *replication.MirrorState,
		RelationshipID: replication.ReplicationAttributes.ExternalUUID,
		Force:          &forceResume,
		Volume: &vsa.Volume{
			ExternalUUID: replication.Volume.VolumeAttributes.ExternalUUID,
		},
	}
	resp, err := provider.ResyncVolumeReplication(vsaResumeParams)
	if err != nil {
		if errors.IsConflictErr(err) {
			return nil, errors.NewNonRetryableErr(err.Error())
		}
		logger.Error("Failed to resume volume replication", "error", err)
		return nil, err
	}

	return resp, nil
}

func (a *InternalVolumeReplicationResumeActivity) GetSnapmirrorDetails(ctx context.Context, replication *datamodel.VolumeReplication, node *models.Node) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	vsaResumeParams := &vsa.VolumeReplication{
		DestinationVolumeName: replication.ReplicationAttributes.DestinationVolumeName,
		DestinationSVMName:    replication.ReplicationAttributes.DestinationSvmName,
		ExternalUUID:          replication.ReplicationAttributes.ExternalUUID,
	}
	resp, err := provider.GetReplicationDetails(ctx, vsaResumeParams)
	if err != nil {
		logger.Error("Failed to get snapmirror details", "error", err)
		return nil, err
	}
	return resp, nil
}

func (a *InternalVolumeReplicationResumeActivity) UpdateVolumeReplicationResumeDetails(ctx context.Context, replication *datamodel.VolumeReplication, replicationResumeResponse *vsa.VolumeReplication) error {
	se := a.SE

	replication.State = models.LifeCycleStateAvailable
	replication.StateDetails = models.LifeCycleStateAvailableDetails
	replication.MirrorState = &replicationResumeResponse.MirrorState
	replication.RelationshipStatus = &replicationResumeResponse.RelationshipStatus
	replication.TotalTransferBytes = replicationResumeResponse.TotalTransferBytes
	replication.TotalTransferTimeSecs = replicationResumeResponse.TotalTransferTimeSecs
	replication.LastTransferSize = replicationResumeResponse.LastTransferSize
	replication.LastTransferError = replicationResumeResponse.LastTransferError
	replication.LastTransferDuration = replicationResumeResponse.LastTransferDuration
	replication.LastTransferEndTime = replicationResumeResponse.LastTransferEndTime
	replication.LagTime = replicationResumeResponse.LagTime
	replication.LastUpdatedFromOntap = time.Now()
	replication.ProgressLastUpdated = &replication.LastUpdatedFromOntap

	if err := se.UpdateVolumeReplication(ctx, replication); err != nil {
		return err
	}

	return nil
}

func (a *InternalVolumeReplicationResumeActivity) UpdateVolumeType(ctx context.Context, replication *datamodel.VolumeReplication) error {
	se := a.SE
	updates := make(map[string]interface{})
	if replication.Volume.VolumeAttributes != nil {
		replication.Volume.VolumeAttributes.IsDataProtection = true
	}
	updates["volume_attributes"] = replication.Volume.VolumeAttributes
	err := se.UpdateVolumeFields(ctx, replication.ReplicationAttributes.DestinationVolumeUUID, updates)
	return err
}
