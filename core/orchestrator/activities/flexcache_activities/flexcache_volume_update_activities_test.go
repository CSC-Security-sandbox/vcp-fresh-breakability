package flexcache_activities

import (
	"context"
	"testing"

	"github.com/go-openapi/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestFlexCacheVolumeUpdateActivity_UpdateFlexCacheVolumeInOntap(t *testing.T) {
	baseModel := datamodel.BaseModel{
		UUID: "uuid-123",
	}
	volume := &datamodel.Volume{
		BaseModel: baseModel,
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name: "default-snapshot-policy",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 1024,
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:   true,
			TieringPolicy:        "auto",
			RetrievalPolicy:      "default",
			CoolingThresholdDays: 10,
		},
		CacheParameters: &models.CacheParameters{
			CacheConfig: &models.CacheConfig{
				CachePrePopulate: &models.CachePrePopulate{
					PathList:        []string{"/data1", "/data2"},
					ExcludePathList: []string{"/data1/exclude"},
				},
			},
		},
	}
	expectedParams := vsa.UpdateFlexCacheVolumeParams{
		UUID:                       "uuid-123",
		PrepopulateDirPaths:        common.ConvertStringSliceToPointerSlice(params.CacheParameters.CacheConfig.CachePrePopulate.PathList),
		PrepopulateExcludeDirPaths: common.ConvertStringSliceToPointerSlice(params.CacheParameters.CacheConfig.CachePrePopulate.ExcludePathList),
	}

	t.Run("UpdateFlexCacheVolumeInONTAP_hyperScalerGetProviderByNode_error", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(t)
		logger := log.NewMockLogger(t)
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		activity := FlexCacheVolumeUpdateActivity{SE: database.NewMockStorage(t)}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		mm.EXPECT().utilGetLogger(mock.Anything).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, errors.New(500, "internal error"))

		err := activity.UpdateFlexCacheVolumeInONTAP(ctx, volume, params, node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "internal error")
	})

	t.Run("UpdateFlexCacheVolumeInONTAP_verifyAndGetFlexCacheUpdateParams_error", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(t)
		logger := log.NewMockLogger(t)
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		activity := FlexCacheVolumeUpdateActivity{SE: database.NewMockStorage(t)}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}

		mm.EXPECT().utilGetLogger(mock.Anything).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mm.EXPECT().verifyAndGetFlexCacheUpdateParams(volume, params).Return(nil, errors.New(400, "bad request"))

		err := activity.UpdateFlexCacheVolumeInONTAP(ctx, volume, params, node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "bad request")
	})

	t.Run("UpdateFlexCacheVolumeInONTAP_UpdateFlexCacheVolume_error", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(t)
		logger := log.NewMockLogger(t)
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		activity := FlexCacheVolumeUpdateActivity{SE: database.NewMockStorage(t)}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		mm.EXPECT().utilGetLogger(mock.Anything).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mm.EXPECT().verifyAndGetFlexCacheUpdateParams(volume, params).Return(&expectedParams, nil)
		mockProvider.On("UpdateFlexCacheVolume", expectedParams).Return(errors.New(500, "internal error"))

		err := activity.UpdateFlexCacheVolumeInONTAP(ctx, volume, params, node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "internal error")
	})

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(t)
		logger := log.NewMockLogger(t)
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		activity := FlexCacheVolumeUpdateActivity{SE: database.NewMockStorage(t)}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		mm.EXPECT().utilGetLogger(mock.Anything).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mm.EXPECT().verifyAndGetFlexCacheUpdateParams(volume, params).Return(&expectedParams, nil)
		logger.EXPECT().Debug("flexCache volume updated successfully")
		mockProvider.On("UpdateFlexCacheVolume", expectedParams).Return(nil)

		err := activity.UpdateFlexCacheVolumeInONTAP(ctx, volume, params, node)
		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
}

func Test_verifyAndGetFlexCacheUpdateParams(t *testing.T) {
	baseModel := datamodel.BaseModel{
		UUID: "uuid-123",
	}
	volume := &datamodel.Volume{
		BaseModel: baseModel,
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name: "default-snapshot-policy",
		},
	}
	t.Run("verifyAndGetFlexCacheUpdateParams_nil_volume", func(tt *testing.T) {
		_, err := _verifyAndGetFlexCacheUpdateParams(nil, &common.UpdateVolumeParams{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "volume is nil")
	})

	t.Run("verifyAndGetFlexCacheUpdateParams_nil_params", func(tt *testing.T) {
		_, err := _verifyAndGetFlexCacheUpdateParams(volume, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "params or cache config is nil")
	})

	t.Run("verifyAndGetFlexCacheUpdateParams_nil_cacheConfig", func(tt *testing.T) {
		_, err := _verifyAndGetFlexCacheUpdateParams(volume, &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "params or cache config is nil")
	})

	t.Run("Success_all_fields", func(tt *testing.T) {
		recursion := true
		writebackEnabled := true
		AtimeScrubEnabled := true
		atimeScrubDays := int16(5)
		CifsChangeNotifyEnabled := true
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					CachePrePopulate: &models.CachePrePopulate{
						PathList:        []string{"/data1", "/data2"},
						ExcludePathList: []string{"/data1/exclude"},
						Recursion:       &recursion,
					},
					WritebackEnabled:        &writebackEnabled,
					AtimeScrubEnabled:       &AtimeScrubEnabled,
					AtimeScrubDays:          &atimeScrubDays,
					CifsChangeNotifyEnabled: &CifsChangeNotifyEnabled,
				},
			},
		}
		expectedParams := &vsa.UpdateFlexCacheVolumeParams{
			UUID:                       "uuid-123",
			PrepopulateDirPaths:        common.ConvertStringSliceToPointerSlice(params.CacheParameters.CacheConfig.CachePrePopulate.PathList),
			PrepopulateExcludeDirPaths: common.ConvertStringSliceToPointerSlice(params.CacheParameters.CacheConfig.CachePrePopulate.ExcludePathList),
			IsRecursionEnabled:         params.CacheParameters.CacheConfig.CachePrePopulate.Recursion,
			WritebackEnabled:           params.CacheParameters.CacheConfig.WritebackEnabled,
			AtimeScrubEnabled:          params.CacheParameters.CacheConfig.AtimeScrubEnabled,
			AtimeScrubPeriod:           params.CacheParameters.CacheConfig.AtimeScrubDays,
			CifsChangeNotifyEnabled:    params.CacheParameters.CacheConfig.CifsChangeNotifyEnabled,
		}
		result, err := _verifyAndGetFlexCacheUpdateParams(volume, params)
		assert.NoError(t, err)
		assert.Equal(t, expectedParams, result)
	})

	t.Run("Success_partial_fields", func(tt *testing.T) {
		writebackEnabled := true
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					CachePrePopulate: &models.CachePrePopulate{
						PathList: []string{"/data1", "/data2"},
					},
					WritebackEnabled: &writebackEnabled,
				},
			},
		}
		expectedParams := &vsa.UpdateFlexCacheVolumeParams{
			UUID:                "uuid-123",
			PrepopulateDirPaths: common.ConvertStringSliceToPointerSlice(params.CacheParameters.CacheConfig.CachePrePopulate.PathList),
			WritebackEnabled:    params.CacheParameters.CacheConfig.WritebackEnabled,
		}
		result, err := _verifyAndGetFlexCacheUpdateParams(volume, params)
		assert.NoError(t, err)
		assert.Equal(t, expectedParams, result)
	})
}
