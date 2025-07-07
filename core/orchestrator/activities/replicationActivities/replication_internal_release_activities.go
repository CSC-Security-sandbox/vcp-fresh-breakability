package replicationActivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
)

type InternalVolumeReplicationRowDeleteActivity struct {
	SE database.Storage
}

func (a *InternalVolumeReplicationRowDeleteActivity) DeleteVolumeReplicationRow(ctx context.Context, replication *datamodel.VolumeReplication) error {
	se := a.SE
	if _, err := se.DeleteVolumeReplication(ctx, replication); err != nil {
		return err
	}
	return nil
}

func (a *InternalVolumeReplicationRowDeleteActivity) UpdateReplicationStateInDBForRelease(ctx context.Context, volumeRep *datamodel.VolumeReplication) error {
	se := a.SE
	volumeRep.State = models.LifeCycleStateError
	volumeRep.StateDetails = models.LifeCycleStateCreationErrorDetails
	if err := se.UpdateVolumeReplicationStates(ctx, volumeRep); err != nil {
		return err
	}
	return nil
}
