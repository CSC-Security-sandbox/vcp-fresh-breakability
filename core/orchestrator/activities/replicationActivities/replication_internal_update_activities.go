package replicationActivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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
		return nil, errors.NewNonRetryableErr(err.Error())
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
