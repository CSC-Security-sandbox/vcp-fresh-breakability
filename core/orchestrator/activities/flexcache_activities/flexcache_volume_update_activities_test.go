package flexcache_activities

import (
	"context"
	"database/sql"
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
	vsaerror "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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

		result, err := activity.UpdateFlexCacheVolumeInONTAP(ctx, volume, params, node)
		assert.Error(t, err)
		assert.Nil(t, result)
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

		result, err := activity.UpdateFlexCacheVolumeInONTAP(ctx, volume, params, node)
		assert.Error(t, err)
		assert.Nil(t, result)
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
		mockProvider.On("UpdateFlexCacheVolume", expectedParams).Return(nil, errors.New(500, "internal error"))

		result, err := activity.UpdateFlexCacheVolumeInONTAP(ctx, volume, params, node)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "internal error")
	})

	t.Run("Success_SynchronousCompletion", func(tt *testing.T) {
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
		logger.EXPECT().Debug("FlexCache volume update initiated successfully")
		mockProvider.On("UpdateFlexCacheVolume", expectedParams).Return(nil, nil) // Synchronous completion

		result, err := activity.UpdateFlexCacheVolumeInONTAP(ctx, volume, params, node)
		assert.NoError(t, err)
		assert.Nil(t, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("Success_AsynchronousWithJobUUID", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(t)
		logger := log.NewMockLogger(t)
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		activity := FlexCacheVolumeUpdateActivity{SE: database.NewMockStorage(t)}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}

		expectedJobUUID := "async-job-123"
		asyncResponse := &vsa.OntapAsyncResponse{
			JobUUID: expectedJobUUID,
		}

		mm.EXPECT().utilGetLogger(mock.Anything).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mm.EXPECT().verifyAndGetFlexCacheUpdateParams(volume, params).Return(&expectedParams, nil)
		logger.EXPECT().Debug("FlexCache volume update initiated successfully")
		mockProvider.On("UpdateFlexCacheVolume", expectedParams).Return(asyncResponse, nil)

		result, err := activity.UpdateFlexCacheVolumeInONTAP(ctx, volume, params, node)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, expectedJobUUID, result.JobUUID)
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

	t.Run("Success_all_fields_without_prepopulate", func(tt *testing.T) {
		writebackEnabled := true
		atimeScrubEnabled := true
		atimeScrubDays := int16(5)
		cifsChangeNotifyEnabled := true
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					WritebackEnabled:        &writebackEnabled,
					AtimeScrubEnabled:       &atimeScrubEnabled,
					AtimeScrubDays:          &atimeScrubDays,
					CifsChangeNotifyEnabled: &cifsChangeNotifyEnabled,
				},
			},
		}
		expectedParams := &vsa.UpdateFlexCacheVolumeParams{
			UUID:                       "uuid-123",
			PrepopulateDirPaths:        nil,
			PrepopulateExcludeDirPaths: nil,
			IsRecursionEnabled:         nil,
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
					WritebackEnabled: &writebackEnabled,
				},
			},
		}
		expectedParams := &vsa.UpdateFlexCacheVolumeParams{
			UUID:                       "uuid-123",
			PrepopulateDirPaths:        nil,
			PrepopulateExcludeDirPaths: nil,
			IsRecursionEnabled:         nil,
			WritebackEnabled:           params.CacheParameters.CacheConfig.WritebackEnabled,
			AtimeScrubEnabled:          nil,
			AtimeScrubPeriod:           nil,
			CifsChangeNotifyEnabled:    nil,
		}
		result, err := _verifyAndGetFlexCacheUpdateParams(volume, params)
		assert.NoError(t, err)
		assert.Equal(t, expectedParams, result)
	})

	t.Run("Success_with_prepopulate_present_but_ignored", func(tt *testing.T) {
		recursion := true
		writebackEnabled := true
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					CachePrePopulate: &models.CachePrePopulate{
						PathList:        []string{"/data1", "/data2"},
						ExcludePathList: []string{"/data1/exclude"},
						Recursion:       &recursion,
					},
					WritebackEnabled: &writebackEnabled,
				},
			},
		}
		expectedParams := &vsa.UpdateFlexCacheVolumeParams{
			UUID:                       "uuid-123",
			PrepopulateDirPaths:        nil,
			PrepopulateExcludeDirPaths: nil,
			IsRecursionEnabled:         nil,
			WritebackEnabled:           &writebackEnabled,
		}
		result, err := _verifyAndGetFlexCacheUpdateParams(volume, params)
		assert.NoError(t, err)
		assert.Equal(t, expectedParams, result)
	})

	t.Run("Success_only_writebackEnabled", func(tt *testing.T) {
		writebackEnabled := false
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					WritebackEnabled: &writebackEnabled,
				},
			},
		}
		result, err := _verifyAndGetFlexCacheUpdateParams(volume, params)
		assert.NoError(t, err)
		assert.Equal(t, "uuid-123", result.UUID)
		assert.Equal(t, &writebackEnabled, result.WritebackEnabled)
		assert.Nil(t, result.AtimeScrubEnabled)
		assert.Nil(t, result.AtimeScrubPeriod)
		assert.Nil(t, result.CifsChangeNotifyEnabled)
	})

	t.Run("Success_only_atimeScrubEnabled", func(tt *testing.T) {
		atimeScrubEnabled := false
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					AtimeScrubEnabled: &atimeScrubEnabled,
				},
			},
		}
		result, err := _verifyAndGetFlexCacheUpdateParams(volume, params)
		assert.NoError(t, err)
		assert.Equal(t, "uuid-123", result.UUID)
		assert.Equal(t, &atimeScrubEnabled, result.AtimeScrubEnabled)
		assert.Nil(t, result.WritebackEnabled)
		assert.Nil(t, result.AtimeScrubPeriod)
		assert.Nil(t, result.CifsChangeNotifyEnabled)
	})

	t.Run("Success_only_atimeScrubDays", func(tt *testing.T) {
		atimeScrubDays := int16(10)
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					AtimeScrubDays: &atimeScrubDays,
				},
			},
		}
		result, err := _verifyAndGetFlexCacheUpdateParams(volume, params)
		assert.NoError(t, err)
		assert.Equal(t, "uuid-123", result.UUID)
		assert.Equal(t, &atimeScrubDays, result.AtimeScrubPeriod)
		assert.Nil(t, result.WritebackEnabled)
		assert.Nil(t, result.AtimeScrubEnabled)
		assert.Nil(t, result.CifsChangeNotifyEnabled)
	})

	t.Run("Success_only_cifsChangeNotifyEnabled", func(tt *testing.T) {
		cifsChangeNotifyEnabled := false
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					CifsChangeNotifyEnabled: &cifsChangeNotifyEnabled,
				},
			},
		}
		result, err := _verifyAndGetFlexCacheUpdateParams(volume, params)
		assert.NoError(t, err)
		assert.Equal(t, "uuid-123", result.UUID)
		assert.Equal(t, &cifsChangeNotifyEnabled, result.CifsChangeNotifyEnabled)
		assert.Nil(t, result.WritebackEnabled)
		assert.Nil(t, result.AtimeScrubEnabled)
		assert.Nil(t, result.AtimeScrubPeriod)
	})
}

func TestFlexCacheVolumeUpdateActivity_CreatePrepopulateJob(t *testing.T) {
	volumeUUID := "volume-uuid-123"
	volumeName := "test-volume"
	accountID := int64(456)
	ontapJobUUID := "ontap-job-uuid-789"

	volume := datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      volumeName,
		AccountID: accountID,
	}

	t.Run("Success", func(tt *testing.T) {
		logger := log.NewMockLogger(tt)
		allowAnyLogs(logger)
		mockStorage := database.NewMockStorage(tt)
		activity := FlexCacheVolumeUpdateActivity{SE: mockStorage}
		ctx := context.Background()

		createdJob := &datamodel.Job{
			BaseModel:     datamodel.BaseModel{UUID: "created-job-uuid"},
			Type:          string(models.JobTypeFlexCachePrePopulate),
			State:         string(models.JobsStateNEW),
			ResourceName:  volumeUUID,
			IsAdminJob:    false,
			AccountID:     sql.NullInt64{Int64: accountID, Valid: true},
			JobAttributes: &datamodel.JobAttributes{ResourceUUID: ontapJobUUID},
		}

		mockStorage.EXPECT().CreateJob(ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.Type == string(models.JobTypeFlexCachePrePopulate) &&
				job.State == string(models.JobsStateNEW) &&
				job.ResourceName == volumeUUID &&
				job.IsAdminJob == false &&
				job.AccountID.Int64 == accountID &&
				job.JobAttributes != nil &&
				job.JobAttributes.ResourceUUID == ontapJobUUID
		})).Return(createdJob, nil)

		result, err := activity.CreatePrepopulateJob(ctx, &volume, ontapJobUUID)

		assert.NoError(tt, err)
		assert.Equal(tt, "created-job-uuid", result)
	})

	t.Run("EmptyOntapJobUUID", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		allowAnyLogs(logger)
		activity := FlexCacheVolumeUpdateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		mm.EXPECT().utilGetLogger(ctx).Return(logger)

		result, err := activity.CreatePrepopulateJob(ctx, &volume, "")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "ontapJobUUID is required")
		assert.Empty(tt, result)
	})

	t.Run("CreateJobFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		allowAnyLogs(logger)
		mockStorage := database.NewMockStorage(tt)
		activity := FlexCacheVolumeUpdateActivity{SE: mockStorage}
		ctx := context.Background()

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mockStorage.EXPECT().CreateJob(ctx, mock.Anything).Return(nil, vsaerror.New("database error"))

		result, err := activity.CreatePrepopulateJob(ctx, &volume, ontapJobUUID)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to create job record")
		assert.Empty(tt, result)
	})
}

func TestFlexCacheVolumeUpdateActivity_StartFlexCachePrepopulate(t *testing.T) {
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	recursion := true
	params := &common.UpdateVolumeParams{
		CacheParameters: &models.CacheParameters{
			CacheConfig: &models.CacheConfig{
				CachePrePopulate: &models.CachePrePopulate{
					PathList:        []string{"/data1", "/data2"},
					ExcludePathList: []string{"/data1/exclude"},
					Recursion:       &recursion,
				},
			},
		},
	}

	node := &models.Node{Name: "test-node"}

	t.Run("Success_AsyncOperation", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := FlexCacheVolumeUpdateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, node).Return(mockProvider, nil)

		expectedParams := &vsa.UpdateFlexCacheVolumeParams{
			UUID:                       "external-uuid",
			PrepopulateDirPaths:        common.ConvertStringSliceToPointerSlice([]string{"/data1", "/data2"}),
			PrepopulateExcludeDirPaths: common.ConvertStringSliceToPointerSlice([]string{"/data1/exclude"}),
			IsRecursionEnabled:         &recursion,
		}

		logger.EXPECT().Infof("Starting prepopulate for volume %s (UUID: %s) with paths: %v",
			volume.Name, volume.UUID, expectedParams.PrepopulateDirPaths)
		mockProvider.EXPECT().UpdateFlexCacheVolume(*expectedParams).Return(&vsa.OntapAsyncResponse{JobUUID: "ontap-job-uuid"}, nil)
		logger.EXPECT().Infof("Prepopulate job created in ONTAP: %s", "ontap-job-uuid")

		result, err := activity.StartFlexCachePrepopulate(ctx, volume, params, node)

		assert.NoError(tt, err)
		assert.Equal(tt, "ontap-job-uuid", result)
	})

	t.Run("Success_SyncOperation", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := FlexCacheVolumeUpdateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, node).Return(mockProvider, nil)

		expectedParams := &vsa.UpdateFlexCacheVolumeParams{
			UUID:                       "external-uuid",
			PrepopulateDirPaths:        common.ConvertStringSliceToPointerSlice([]string{"/data1", "/data2"}),
			PrepopulateExcludeDirPaths: common.ConvertStringSliceToPointerSlice([]string{"/data1/exclude"}),
			IsRecursionEnabled:         &recursion,
		}

		logger.EXPECT().Infof("Starting prepopulate for volume %s (UUID: %s) with paths: %v",
			volume.Name, volume.UUID, expectedParams.PrepopulateDirPaths)
		mockProvider.EXPECT().UpdateFlexCacheVolume(*expectedParams).Return(nil, nil) // nil = synchronous completion
		logger.EXPECT().Infof("Prepopulate completed synchronously for volume %s", volume.Name)

		result, err := activity.StartFlexCachePrepopulate(ctx, volume, params, node)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})

	t.Run("GetProviderFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		activity := FlexCacheVolumeUpdateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, node).Return(nil, vsaerror.New("provider error"))

		result, err := activity.StartFlexCachePrepopulate(ctx, volume, params, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get provider")
		assert.Empty(tt, result)
	})

	t.Run("BuildParamsFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := FlexCacheVolumeUpdateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		invalidParams := &common.UpdateVolumeParams{}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, node).Return(mockProvider, nil)

		result, err := activity.StartFlexCachePrepopulate(ctx, volume, invalidParams, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to build prepopulate params")
		assert.Empty(tt, result)
	})

	t.Run("OntapPrepopulateFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := FlexCacheVolumeUpdateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, node).Return(mockProvider, nil)

		expectedParams := &vsa.UpdateFlexCacheVolumeParams{
			UUID:                       "external-uuid",
			PrepopulateDirPaths:        common.ConvertStringSliceToPointerSlice([]string{"/data1", "/data2"}),
			PrepopulateExcludeDirPaths: common.ConvertStringSliceToPointerSlice([]string{"/data1/exclude"}),
			IsRecursionEnabled:         &recursion,
		}

		logger.EXPECT().Infof("Starting prepopulate for volume %s (UUID: %s) with paths: %v",
			volume.Name, volume.UUID, expectedParams.PrepopulateDirPaths)
		mockProvider.EXPECT().UpdateFlexCacheVolume(*expectedParams).Return(nil, vsaerror.New("ONTAP error"))

		result, err := activity.StartFlexCachePrepopulate(ctx, volume, params, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "ONTAP prepopulate request failed")
		assert.Empty(tt, result)
	})
}

func TestFlexCacheVolumeUpdateActivity_UpdatePrepopulateState(t *testing.T) {
	volumeUUID := "volume-uuid"
	state := "running"

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		allowAnyLogs(logger)
		mockStorage := database.NewMockStorage(tt)
		activity := FlexCacheVolumeUpdateActivity{SE: mockStorage}
		ctx := context.Background()

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: volumeUUID},
			Name:      "test-volume",
			CacheParameters: &datamodel.CacheParameters{
				CacheConfig: &datamodel.CacheConfig{},
			},
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mockStorage.EXPECT().GetVolume(ctx, volumeUUID).Return(volume, nil)
		mockStorage.EXPECT().UpdateVolumeFields(ctx, volumeUUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
			cacheParams, ok := updates["cache_parameters"].(*datamodel.CacheParameters)
			if !ok {
				return false
			}
			return cacheParams.CacheConfig != nil &&
				cacheParams.CacheConfig.CachePrePopulateState != "" &&
				cacheParams.CacheConfig.CachePrePopulateState == state
		})).Return(nil)

		err := activity.UpdatePrepopulateState(ctx, volumeUUID, state)

		assert.NoError(tt, err)
	})

	t.Run("GetVolumeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := FlexCacheVolumeUpdateActivity{SE: mockStorage}
		ctx := context.Background()
		allowAnyLogs(logger)

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mockStorage.EXPECT().GetVolume(ctx, volumeUUID).Return(nil, vsaerror.New("volume not found"))

		err := activity.UpdatePrepopulateState(ctx, volumeUUID, state)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get volume")
	})

	t.Run("UpdateVolumeFieldsFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := FlexCacheVolumeUpdateActivity{SE: mockStorage}
		ctx := context.Background()
		allowAnyLogs(logger)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: volumeUUID},
			Name:      "test-volume",
			CacheParameters: &datamodel.CacheParameters{
				CacheConfig: &datamodel.CacheConfig{
					CachePrePopulate: &datamodel.CachePrePopulate{
						PathList:        []string{"/data1", "/data2"},
						ExcludePathList: []string{"/data1/exclude"},
					},
				},
			},
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mockStorage.EXPECT().GetVolume(ctx, volumeUUID).Return(volume, nil)
		mockStorage.EXPECT().UpdateVolumeFields(ctx, volumeUUID, mock.Anything).Return(vsaerror.New("update failed"))

		err := activity.UpdatePrepopulateState(ctx, volumeUUID, state)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to update prepopulate state")
	})
}

func Test_buildPrepopulateOnlyParams(t *testing.T) {
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}

	t.Run("Success_AllFields", func(tt *testing.T) {
		recursion := true
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					CachePrePopulate: &models.CachePrePopulate{
						PathList:        []string{"/data1", "/data2"},
						ExcludePathList: []string{"/data1/exclude"},
						Recursion:       &recursion,
					},
				},
			},
		}

		result, err := buildPrepopulateOnlyParams(volume, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "external-uuid", result.UUID)
		assert.Equal(tt, 2, len(result.PrepopulateDirPaths))
		assert.Equal(tt, "/data1", *result.PrepopulateDirPaths[0])
		assert.Equal(tt, "/data2", *result.PrepopulateDirPaths[1])
		assert.Equal(tt, 1, len(result.PrepopulateExcludeDirPaths))
		assert.Equal(tt, "/data1/exclude", *result.PrepopulateExcludeDirPaths[0])
		assert.Equal(tt, recursion, *result.IsRecursionEnabled)

		// Verify other fields are nil
		assert.Nil(tt, result.WritebackEnabled)
		assert.Nil(tt, result.RelativeSizeEnabled)
		assert.Nil(tt, result.AtimeScrubEnabled)
		assert.Nil(tt, result.CifsChangeNotifyEnabled)
	})

	t.Run("Success_OnlyPathList", func(tt *testing.T) {
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					CachePrePopulate: &models.CachePrePopulate{
						PathList: []string{"/data1"},
					},
				},
			},
		}

		result, err := buildPrepopulateOnlyParams(volume, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, 1, len(result.PrepopulateDirPaths))
		assert.Nil(tt, result.PrepopulateExcludeDirPaths)
		assert.Nil(tt, result.IsRecursionEnabled)
	})

	t.Run("Success_OnlyExcludePathList", func(tt *testing.T) {
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					CachePrePopulate: &models.CachePrePopulate{
						ExcludePathList: []string{"/exclude"},
					},
				},
			},
		}

		result, err := buildPrepopulateOnlyParams(volume, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Nil(tt, result.PrepopulateDirPaths)
		assert.Equal(tt, 1, len(result.PrepopulateExcludeDirPaths))
		assert.Nil(tt, result.IsRecursionEnabled)
	})

	t.Run("Success_OnlyRecursion", func(tt *testing.T) {
		recursion := false
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					CachePrePopulate: &models.CachePrePopulate{
						Recursion: &recursion,
					},
				},
			},
		}

		result, err := buildPrepopulateOnlyParams(volume, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Nil(tt, result.PrepopulateDirPaths)
		assert.Nil(tt, result.PrepopulateExcludeDirPaths)
		assert.Equal(tt, recursion, *result.IsRecursionEnabled)
	})

	t.Run("Success_EmptyPathLists", func(tt *testing.T) {
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					CachePrePopulate: &models.CachePrePopulate{
						PathList:        []string{},
						ExcludePathList: []string{},
					},
				},
			},
		}

		result, err := buildPrepopulateOnlyParams(volume, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Nil(tt, result.PrepopulateDirPaths)
		assert.Nil(tt, result.PrepopulateExcludeDirPaths)
	})

	t.Run("Success_NilPathLists", func(tt *testing.T) {
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					CachePrePopulate: &models.CachePrePopulate{},
				},
			},
		}

		result, err := buildPrepopulateOnlyParams(volume, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Nil(tt, result.PrepopulateDirPaths)
		assert.Nil(tt, result.PrepopulateExcludeDirPaths)
		assert.Nil(tt, result.IsRecursionEnabled)
	})

	t.Run("NilVolume", func(tt *testing.T) {
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{
					CachePrePopulate: &models.CachePrePopulate{},
				},
			},
		}

		result, err := buildPrepopulateOnlyParams(nil, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "volume is nil")
	})

	t.Run("NilParams", func(tt *testing.T) {
		result, err := buildPrepopulateOnlyParams(volume, nil)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "params or cache config is nil")
	})

	t.Run("NilCacheParameters", func(tt *testing.T) {
		params := &common.UpdateVolumeParams{}

		result, err := buildPrepopulateOnlyParams(volume, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "params or cache config is nil")
	})

	t.Run("NilCacheConfig", func(tt *testing.T) {
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{},
		}

		result, err := buildPrepopulateOnlyParams(volume, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "params or cache config is nil")
	})

	t.Run("NilCachePrePopulate", func(tt *testing.T) {
		params := &common.UpdateVolumeParams{
			CacheParameters: &models.CacheParameters{
				CacheConfig: &models.CacheConfig{},
			},
		}

		result, err := buildPrepopulateOnlyParams(volume, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "prepopulate config is nil")
	})
}
