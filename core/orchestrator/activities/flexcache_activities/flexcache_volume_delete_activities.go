package flexcache_activities

import (
	"context"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
	volume := result.DBVolume
	node := result.Node

	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if volume.ClusterPeerUUID != nil {
		err = provider.DeleteClusterPeer(*volume.ClusterPeerUUID)
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDeletingClusterPeer, err)
		}

		updates := map[string]interface{}{
			"cluster_peer_uuid": nil,
		}
		if err := a.SE.UpdateVolumeFields(ctx, volume.UUID, updates); err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
		}
		logger.Debugf("Cluster peering with UUID %s deleted successfully", *volume.ClusterPeerUUID)
	}

	return result, nil
}
