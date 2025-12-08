package replicationActivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type InternalVolumeReplicationUpdateActivity struct {
	SE database.Storage
}

func (a *InternalVolumeReplicationUpdateActivity) UpdateVolumeReplicationOntap(ctx context.Context, params *common.UpdateVolumeReplicationInternalParams, node *models.Node, volumeRepExternalUUID string) (*vsa.VolumeReplication, error) {
	if params.ReplicationSchedule == nil {
		return nil, nil
	}
	logger := util.GetLogger(ctx)
	provider, err := activitiesGetProviderByNode(ctx, node)
	if err != nil {
		logger.Error("Failed to get provider by node", "error", err)
		return nil, utilerrors.NewNonRetryableErr(err.Error())
	}
	volRep := &vsa.VolumeReplication{
		RelationshipID:      volumeRepExternalUUID,
		ReplicationSchedule: *params.ReplicationSchedule,
	}
	res, err := provider.UpdateVolumeReplication(volRep)
	if err != nil {
		logger.Error("Failed to update volume replication", "error", err)
		return nil, err
	}
	return res, nil
}

func (a *InternalVolumeReplicationUpdateActivity) UpdateClusterPeeringClusterLocation(ctx context.Context, params *common.UpdateVolumeReplicationInternalParams, replication *datamodel.VolumeReplication) error {
	if params.ClusterLocation == nil {
		return nil
	}
	logger := util.GetLogger(ctx)
	if !replication.ClusterPeerId.Valid && replication.ClusterPeer == nil {
		logger.Debug("Replication does not have a cluster peering, skipping cluster location update")
		return nil
	}
	clusterPeer := replication.ClusterPeer

	// Update cluster location in attributes
	if clusterPeer.ClusterPeeringAttributes == nil {
		clusterPeer.ClusterPeeringAttributes = &datamodel.ClusterPeeringAttributes{}
	}
	clusterPeer.ClusterPeeringAttributes.ClusterLocation = params.ClusterLocation

	if err := a.SE.UpdateClusterPeeringRow(ctx, clusterPeer); err != nil {
		logger.Error("Failed to update cluster peering row", "error", err)
		return errors.NewVCPError(errors.ErrDatabaseDataUpdateError, err)
	}
	logger.Debugf("Successfully updated cluster location to %s for cluster peering ID %d", *params.ClusterLocation, clusterPeer.ID)
	return nil
}
