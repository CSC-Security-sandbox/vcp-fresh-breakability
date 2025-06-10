package replicationActivities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	utilsGetSignedCallbackToken                                 = utils.GetSignedCallbackToken
	MapReplicationBetaToReplicationHydrateObject                = _mapReplicationBetaToReplicationHydrateObject
	mapReplicationLifeCycleStateBetaToReplicationHydrationState = _mapReplicationLifeCycleStateBetaToReplicationHydrationState
	mapVolumeBetaToVolumeHydrateObject                          = _mapVolumeBetaToVolumeHydrateObject
	hydrateReplicationCreate                                    = common.ReplicationCreate
	hydrateVolumeCreate                                         = common.VolumeCreate
	hydrateVolumeDelete                                         = common.VolumeDelete
	hydrateReplicationState                                     = common.HydrateReplicationState
	hydrateReplicationStateAndType                              = common.HydrateReplicationStateAndType
	hydrateReplicationDelete                                    = common.ReplicationDelete
	getQuotaLimit                                               = common.GetQuotaLimit
)

const (
	// VolumeV1betaServiceLevelFLEX captures enum value "FLEX"
	VolumeV1betaServiceLevelFLEX string = "FLEX"
)

func _mapVolumeBetaToVolumeHydrateObject(volume models.Volume, poolResourceId string) models.VolumeHydrateObject {
	quotaInBytes := float64(volume.QuotaInBytes)
	return models.VolumeHydrateObject{
		ResourceId:   volume.DisplayName,
		VolumeId:     volume.UUID,
		PoolId:       poolResourceId,
		Protocols:    volume.ProtocolTypes,
		State:        volume.LifeCycleState,
		QuotaInGib:   utils.ConvertBytesToGib(quotaInBytes),
		ServiceLevel: VolumeV1betaServiceLevelFLEX,
	}
}

func _mapReplicationBetaToReplicationHydrateObject(replication models.VolumeReplication) models.ReplicationHydrateObject {
	return models.ReplicationHydrateObject{
		ResourceId:       replication.Name,
		ReplicationState: mapReplicationLifeCycleStateBetaToReplicationHydrationState(replication.State),
	}
}

func generateCallbackToken(ctx context.Context) (string, error) {
	logger := util.GetLogger(ctx)
	callbackToken, err := utilsGetSignedCallbackToken()
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return "", err
	}
	return callbackToken, nil
}

func GetQuotaLimit(ctx context.Context, region string, project string) (int, error) {
	logger := util.GetLogger(ctx)
	callbackToken, err := generateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return 0, err
	}
	// Hydrate GetQuotaLimit to CFFE
	quota, err := getQuotaLimit(ctx, logger, region, project, callbackToken, common.ResourceTypeVolume)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return 0, err
	}
	return quota, nil
}

func VolumeReplicationHydration(ctx context.Context, createReplicationResponse models.VolumeReplication, project string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := generateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	replicationHydrateObject := MapReplicationBetaToReplicationHydrateObject(createReplicationResponse)
	// Hydrate Replication to CFFE
	err = hydrateReplicationCreate(ctx, logger, replicationHydrateObject, createReplicationResponse.ReplicationAttributes.DestinationRegion, project, createReplicationResponse.ReplicationAttributes.DestinationReplicationUUID, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func VolumeReplicationDeHydration(ctx context.Context, createReplicationResponse models.VolumeReplication, project string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := generateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	// DeHydrate Replication to CFFE
	err = hydrateReplicationDelete(ctx, logger, createReplicationResponse.UUID, createReplicationResponse.Volume.UUID, createReplicationResponse.ReplicationAttributes.DestinationRegion, project, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func VolumeHydration(ctx context.Context, destVolume models.Volume, project string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := generateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	var poolResourceId string
	// Hydrate Volume to CFFE
	hydrateVolume := mapVolumeBetaToVolumeHydrateObject(destVolume, poolResourceId)
	err = hydrateVolumeCreate(ctx, logger, hydrateVolume, destVolume.Region, project, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func VolumeDeHydration(ctx context.Context, destVolume models.Volume, project string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := generateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	// DeHydrate Volume to CFFE
	err = hydrateVolumeDelete(ctx, logger, destVolume.UUID, destVolume.Region, project, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func HydrateReplicationState(ctx context.Context, createReplicationResponse models.VolumeReplication, replicationState models.VolumeReplicationHydrateState, project string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := generateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	// Hydrate Replication State to CFFE
	err = hydrateReplicationState(ctx, logger, createReplicationResponse.ReplicationAttributes.DestinationRegion, project, createReplicationResponse.ReplicationAttributes.DestinationVolumeUUID, createReplicationResponse.UUID, replicationState, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func HydrateReplicationStateAndType(ctx context.Context, createReplicationResponse models.VolumeReplication, replicationState models.VolumeReplicationHydrateState, hybridReplicationType models.HybridReplicationHydrateType, project string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := generateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	// Hydrate Replication State & Type to CFFE
	err = hydrateReplicationStateAndType(ctx, logger, createReplicationResponse.ReplicationAttributes.DestinationRegion, project, createReplicationResponse.ReplicationAttributes.DestinationVolumeUUID, createReplicationResponse.UUID, replicationState, hybridReplicationType, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func _mapReplicationLifeCycleStateBetaToReplicationHydrationState(state string) string {
	switch state {
	case "creating":
		return "CREATING"
	case "available":
		return "READY"
	case "updating":
		return "UPDATING"
	case "disabled":
		return "STOPPED"
	case "deleting":
		return "DELETING"
	case "error":
		return "ERROR"
	default:
		return "STATE_UNSPECIFIED"
	}
}
