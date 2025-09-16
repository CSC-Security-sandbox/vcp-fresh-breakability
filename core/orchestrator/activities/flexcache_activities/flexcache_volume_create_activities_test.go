package flexcache_activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestFlexCacheVolumeCreateActivity_CreateFlexCacheVolumeInOntap(t *testing.T) {
	dbVolume := &datamodel.Volume{
		Svm:     &datamodel.Svm{Name: "svm-name"},
		Account: &datamodel.Account{Name: "account-name"},
		CacheParameters: &datamodel.CacheParameters{
			PeerSvmName:     "peer-svm",
			PeerVolumeName:  "peer-volume",
			PeerClusterName: "peer-cluster",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{},
		},
	}

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.CreateFlexCacheResult{
			DBVolume: dbVolume,
		}

		volumeResp := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "volume-name",
				ExternalUUID: "external-uuid",
			},
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().CreateFlexCacheVolume(mock.Anything).Return(volumeResp, nil)
		logger.EXPECT().Debug("flexcache volume created successfully")

		newResult, err := activity.CreateFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)

		assert.NoError(t, err, "No-op function should return nil")
		assert.Equal(tt, "volume-name", newResult.VolumeResponse.Name)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.CreateFlexCacheResult{
			DBVolume: dbVolume,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)

		_, err := activity.CreateFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)

		assert.Error(t, err, "Function should return an error when GetProviderByNode fails")
	})

	t.Run("WhenCreateFlexCacheVolumeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.CreateFlexCacheResult{
			DBVolume: dbVolume,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().CreateFlexCacheVolume(mock.Anything).Return(nil, assert.AnError)

		_, err := activity.CreateFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)

		assert.Error(t, err, "Function should return an error when CreateFlexCacheVolume fails")
	})
}
