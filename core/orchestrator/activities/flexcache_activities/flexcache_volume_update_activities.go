package flexcache_activities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

type FlexCacheVolumeUpdateActivity struct {
	SE database.Storage
}

var (
	verifyAndGetFlexCacheUpdateParams = _verifyAndGetFlexCacheUpdateParams
)

// UpdateFlexCacheVolumeInONTAP updates an existing FlexCache volume in ONTAP
func (a FlexCacheVolumeUpdateActivity) UpdateFlexCacheVolumeInONTAP(ctx context.Context, volume *datamodel.Volume,
	params *common.UpdateVolumeParams, node *models.Node) error {
	logger := utilGetLogger(ctx)
	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return err
	}

	flexCacheUpdateVolumeParams, err := verifyAndGetFlexCacheUpdateParams(volume, params)
	if err != nil {
		return err
	}

	err = provider.UpdateFlexCacheVolume(*flexCacheUpdateVolumeParams)
	if err != nil {
		return err
	}

	logger.Debug("flexCache volume updated successfully")

	return nil
}

func _verifyAndGetFlexCacheUpdateParams(volume *datamodel.Volume, params *common.UpdateVolumeParams) (*vsa.UpdateFlexCacheVolumeParams, error) {
	if volume == nil {
		return nil, fmt.Errorf("volume is nil")
	}
	if params == nil || params.CacheParameters == nil || params.CacheParameters.CacheConfig == nil {
		return nil, fmt.Errorf("params or cache config is nil")
	}

	flexCacheUpdateVolumeParams := vsa.UpdateFlexCacheVolumeParams{}
	if params.CacheParameters.CacheConfig.CachePrePopulate != nil {
		prePop := params.CacheParameters.CacheConfig.CachePrePopulate
		if prePop.PathList != nil {
			flexCacheUpdateVolumeParams.PrepopulateDirPaths = common.ConvertStringSliceToPointerSlice(prePop.PathList)
		}
		if prePop.ExcludePathList != nil {
			flexCacheUpdateVolumeParams.PrepopulateExcludeDirPaths = common.ConvertStringSliceToPointerSlice(prePop.ExcludePathList)
		}
		flexCacheUpdateVolumeParams.IsRecursionEnabled = prePop.Recursion
	}
	if params.CacheParameters.CacheConfig.WritebackEnabled != nil {
		flexCacheUpdateVolumeParams.WritebackEnabled = params.CacheParameters.CacheConfig.WritebackEnabled
	}
	if params.CacheParameters.CacheConfig.AtimeScrubEnabled != nil {
		flexCacheUpdateVolumeParams.AtimeScrubEnabled = params.CacheParameters.CacheConfig.AtimeScrubEnabled
	}
	if params.CacheParameters.CacheConfig.AtimeScrubDays != nil {
		flexCacheUpdateVolumeParams.AtimeScrubPeriod = params.CacheParameters.CacheConfig.AtimeScrubDays
	}
	if params.CacheParameters.CacheConfig.CifsChangeNotifyEnabled != nil {
		flexCacheUpdateVolumeParams.CifsChangeNotifyEnabled = params.CacheParameters.CacheConfig.CifsChangeNotifyEnabled
	}
	flexCacheUpdateVolumeParams.UUID = volume.VolumeAttributes.ExternalUUID
	return &flexCacheUpdateVolumeParams, nil
}
