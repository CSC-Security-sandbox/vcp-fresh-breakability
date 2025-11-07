package flexcache_activities

import (
	"context"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

type FlexCacheVolumeDeleteActivity struct {
	SE database.Storage
}

// UnmountVolumeInOntapActivity deletes a FlexCache volume in ONTAP
func (a FlexCacheVolumeDeleteActivity) UnmountVolumeInOntapActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume
	node := result.Node

	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if volume.VolumeAttributes.ExternalUUID == "" {
		logger.Debug("no external UUID found for the volume, skipping unmount")
		return result, nil // No volume in ONTAP to unmount
	}

	response, err := provider.UnmountVolume(volume.VolumeAttributes.ExternalUUID)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrUnmountingFlexCacheVolume, err)
	}

	result.UnmountJobResponse = response
	logger.Debugf("FlexCache volume unmount job for volume with UUID %s initiated successfully", volume.UUID)

	return result, nil
}

// DeleteFlexCacheVolumeInOntapActivity deletes a FlexCache volume in ONTAP
func (a FlexCacheVolumeDeleteActivity) DeleteFlexCacheVolumeInOntapActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume
	node := result.Node

	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if volume.VolumeAttributes.ExternalUUID == "" {
		logger.Debug("no external UUID found for the volume, skipping delete")
		return result, nil // No volume in ONTAP to delete
	}

	response, err := provider.DeleteFlexCacheVolume(volume.VolumeAttributes.ExternalUUID, volume.Name)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDeletingFlexCacheVolume, err)
	}

	result.DeleteJobResponse = response
	logger.Debugf("FlexCache volume delete job for volume with UUID %s initiated successfully", volume.UUID)

	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) DeleteSVMPeeringInOntapActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume
	node := result.Node

	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if volume.SvmPeerUUID != nil {
		err = provider.DeleteSVMPeer(*volume.SvmPeerUUID, false)
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDeletingSVMPeer, err)
		}

		updates := map[string]interface{}{
			"svm_peer_uuid": nil,
		}
		if err := a.SE.UpdateVolumeFields(ctx, volume.UUID, updates); err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
		}
		logger.Debugf("SVM peering with UUID %s deleted successfully", *volume.SvmPeerUUID)
	}

	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) DeleteClusterPeerInOntapActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	clusterPeeringRow := result.ClusterPeeringRow
	node := result.Node

	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if clusterPeeringRow.OntapPeerUUID != "" {
		err = provider.DeleteClusterPeer(clusterPeeringRow.OntapPeerUUID)
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDeletingClusterPeer, err)
		}
		logger.Debugf("Cluster peering with UUID %s deleted successfully", clusterPeeringRow.OntapPeerUUID)
	}

	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) DeleteClusterPeeringRowInDBActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	clusterPeeringRow := result.ClusterPeeringRow

	if err := a.SE.DeleteClusterPeeringRow(ctx, clusterPeeringRow); err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataDeleteError, err)
	}

	logger.Debugf("Cluster peering row with ID %d deleted successfully", clusterPeeringRow.ID)
	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) GetClusterPeeringFromDBActivity(ctx context.Context,
	result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume
	cacheParams := volume.CacheParameters

	existingPeer, err := a.SE.GetClusterPeerByAccountIDExternalClusterAndPoolID(ctx, volume.Account.ID,
		cacheParams.PeerClusterName, volume.Pool.ID)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			logger.Debugf("Cluster peering row not found (account=%d cluster=%s pool=%d)",
				volume.Account.ID, cacheParams.PeerClusterName, volume.Pool.ID)
			return result, nil
		}
		logger.Errorf("Failed to get cluster peering row from database: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	result.ClusterPeeringRow = existingPeer
	logger.Debugf("Found existing cluster peering row in database: %s", existingPeer.UUID)
	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) GetFlexCacheAndReplicationCountsOnClusterPeeringActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	clusterPeeringRow := result.ClusterPeeringRow

	if clusterPeeringRow == nil {
		result.VolumeReplicationCountOnClusterPeering = 0
		result.FlexCacheVolumeCountOnClusterPeering = 0
		return result, nil
	}

	replicationCount, err := a.SE.GetVolumeReplicationCountByClusterPeerID(ctx, clusterPeeringRow.ID)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	flexCacheCount, err := a.SE.GetFlexCacheVolumeCountByClusterPeerID(ctx, clusterPeeringRow.ID)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	result.VolumeReplicationCountOnClusterPeering = replicationCount
	result.FlexCacheVolumeCountOnClusterPeering = flexCacheCount
	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) UpdateClusterPeeringRowStateDeletedInDBActivity(ctx context.Context,
	result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	clusterPeeringRow := result.ClusterPeeringRow
	se := a.SE
	clusterPeeringRow.State = coremodels.CvpClusterPeeringStatusDELETED
	if err := se.UpdateClusterPeeringRow(ctx, clusterPeeringRow); err != nil {
		logger.Errorf("Failed to update cluster peering row in DB: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Cluster peering row with UUID %s updated to state %s", clusterPeeringRow.UUID, clusterPeeringRow.State)
	return result, nil
}
