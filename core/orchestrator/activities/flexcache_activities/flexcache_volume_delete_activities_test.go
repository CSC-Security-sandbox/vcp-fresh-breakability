package flexcache_activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestFlexCacheVolumeDeleteActivity_UnmountVolumeInOntapActivity(t *testing.T) {
	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		Svm:       &datamodel.Svm{Name: "svm-name"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}
	node := &models.Node{
		Name: "test-node",
	}

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().UnmountVolume("external-uuid").Return(&vsa.OntapAsyncResponse{JobUUID: "unmount-job-uuid"}, nil)
		logger.EXPECT().Debugf("FlexCache volume unmount job for volume with UUID %s started successfully", "volume-uuid")

		resp, err := activity.UnmountVolumeInOntapActivity(ctx, flexcacheResult)

		assert.NoError(tt, err, "UnmountVolumeInOntapActivity should complete successfully")
		assert.NotNil(tt, resp)
		assert.NotNil(tt, resp.UnmountJobResponse)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)

		resp, err := activity.UnmountVolumeInOntapActivity(ctx, flexcacheResult)

		assert.Error(tt, err, "Function should return an error when GetProviderByNode fails")
		assert.Nil(tt, resp)
	})

	t.Run("WhenUnmountVolumeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().UnmountVolume("external-uuid").Return(nil, assert.AnError)

		resp, err := activity.UnmountVolumeInOntapActivity(ctx, flexcacheResult)

		assert.Error(tt, err, "Function should return an error when UnmountVolume fails")
		assert.Nil(tt, resp)
	})

	t.Run("WhenNoExternalUUID", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: &datamodel.Volume{
				Name: "test-volume",
				Svm:  &datamodel.Svm{Name: "svm-name"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "",
				},
			},
			Node: node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		logger.EXPECT().Debug("no external UUID found for the volume, skipping unmount")

		resp, err := activity.UnmountVolumeInOntapActivity(ctx, flexcacheResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
	})
}

func TestFlexCacheVolumeDeleteActivity_DeleteFlexCacheVolumeInOntapActivity(t *testing.T) {
	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		Svm:       &datamodel.Svm{Name: "svm-name"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}
	node := &models.Node{
		Name: "test-node",
	}

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().DeleteFlexCacheVolume("external-uuid", "test-volume").Return(&vsa.OntapAsyncResponse{JobUUID: "delete-job-uuid"}, nil)
		logger.EXPECT().Debugf("FlexCache volume delete job for volume with UUID %s started successfully", "volume-uuid")

		resp, err := activity.DeleteFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)

		assert.NoError(tt, err, "DeleteFlexCacheVolumeInOntapActivity should complete successfully")
		assert.NotNil(tt, resp)
		assert.NotNil(tt, resp.DeleteJobResponse)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)

		resp, err := activity.DeleteFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)

		assert.Error(tt, err, "Function should return an error when GetProviderByNode fails")
		assert.Nil(tt, resp)
	})

	t.Run("WhenDeleteFlexCacheVolumeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().DeleteFlexCacheVolume("external-uuid", "test-volume").Return(nil, assert.AnError)

		resp, err := activity.DeleteFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)

		assert.Error(tt, err, "Function should return an error when DeleteFlexCacheVolume fails")
		assert.Nil(tt, resp)
	})

	t.Run("WhenNoExternalUUID", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: &datamodel.Volume{
				Name: "test-volume",
				Svm:  &datamodel.Svm{Name: "svm-name"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "",
				},
			},
			Node: node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		logger.EXPECT().Debug("no external UUID found for the volume, skipping delete")

		resp, err := activity.DeleteFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Nil(tt, resp.DeleteJobResponse)
	})
}
