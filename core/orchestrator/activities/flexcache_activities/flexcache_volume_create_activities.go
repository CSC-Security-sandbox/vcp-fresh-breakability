package flexcache_activities

import (
	"context"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"

	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

type FlexCacheVolumeCreateActivity struct {
	SE database.Storage
}

var (
	hyperscalerGetProviderByNode = hyperscaler.GetProviderByNode
)

// CreateFlexCacheVolumeInOntapActivity creates a FlexCache volume in ONTAP
func (a *FlexCacheVolumeCreateActivity) CreateFlexCacheVolumeInOntapActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	logger := util.GetLogger(ctx)
	volume := result.DBVolume
	cacheParams := volume.CacheParameters
	provider, err := hyperscalerGetProviderByNode(ctx, result.Node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	params := vsa.CreateFlexCacheVolumeParams{
		Name:             volume.Name,
		SvmName:          volume.Svm.Name,
		AggregateName:    activities.AggregateName,
		OriginSVMName:    cacheParams.PeerSvmName,
		OriginVolumeName: cacheParams.PeerVolumeName,
		JunctionPath:     &volume.VolumeAttributes.FileProperties.JunctionPath,
	}

	res, err := provider.CreateFlexCacheVolume(params)
	if err != nil {
		return nil, err
	}

	logger.Debug("flexcache volume created successfully")

	result.VolumeResponse = res

	return result, nil
}
