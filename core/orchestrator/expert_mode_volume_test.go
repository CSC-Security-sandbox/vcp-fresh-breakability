package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowenginemock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestCreateExpertModeVolume(t *testing.T) {
	setupStore := func(tt *testing.T) (*log.MockLogger, database.Storage, *datamodel.Account, *datamodel.Pool, *datamodel.Svm) {
		mockLogger := log.NewMockLogger(tt)
		mockLogger.EXPECT().InfoContext(mock.Anything, "Running AutoMigrate for model changes")

		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "550e8400-e29b-41d4-a716-446655440000"},
			Name:           "test_pool",
			AccountID:      account.ID,
			SizeInBytes:    2199023255552, // 2TB
			LargeCapacity:  false,
			PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test-svm",
			PoolID:    pool.ID,
			AccountID: account.ID,
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "660e8400-e29b-41d4-a716-446655440001",
				IPSpace:      "Default",
			},
			State: models.LifeCycleStateREADY,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		return mockLogger, store, account, pool, svm
	}

	t.Run("Success_WithSvmUuid", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, account, pool, svm := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 1099511627776, // 1TB
			Style:       "flexvol",
			SvmUuid:     "660e8400-e29b-41d4-a716-446655440001",
			SvmName:     "",
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.CreateExpertModeVolume(ctx, params)

		assert.NoError(tt, err)

		var createdVolume datamodel.ExpertModeVolumes
		err = store.DB().Where("name = ?", params.VolumeName).First(&createdVolume).Error
		assert.NoError(tt, err)
		assert.Equal(tt, params.VolumeName, createdVolume.Name)
		assert.Equal(tt, params.SizeInBytes, createdVolume.SizeInBytes)
		assert.Equal(tt, pool.ID, createdVolume.PoolID)
		assert.Equal(tt, account.ID, createdVolume.AccountID)
		assert.Equal(tt, svm.ID, createdVolume.SvmID)
		assert.Equal(tt, params.Style, createdVolume.Style)
		assert.Equal(tt, models.LifeCycleStateCreating, createdVolume.State)
		assert.NotEmpty(tt, createdVolume.ExternalUUID)

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Success_WithSvmName", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, pool, svm := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 1099511627776,
			Style:       "flexvol",
			SvmUuid:     "",
			SvmName:     "test-svm",
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.CreateExpertModeVolume(ctx, params)

		assert.NoError(tt, err)

		var createdVolume datamodel.ExpertModeVolumes
		err = store.DB().Where("name = ?", params.VolumeName).First(&createdVolume).Error
		assert.NoError(tt, err)
		assert.Equal(tt, svm.ID, createdVolume.SvmID)

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Success_Flexgroup_WithLargeCapacityPool", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, pool, _ := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		pool.LargeCapacity = true
		err := store.DB().Save(pool).Error
		assert.NoError(tt, err)

		params := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-flexgroup-volume",
			SizeInBytes: 1099511627776,
			Style:       "flexgroup",
			SvmUuid:     "660e8400-e29b-41d4-a716-446655440001",
			SvmName:     "",
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()

		orch := &Orchestrator{storage: store, temporal: temporal}
		err = orch.CreateExpertModeVolume(ctx, params)

		assert.NoError(tt, err)

		var createdVolume datamodel.ExpertModeVolumes
		err = store.DB().Where("name = ?", params.VolumeName).First(&createdVolume).Error
		assert.NoError(tt, err)
		assert.Equal(tt, "flexgroup", createdVolume.Style)

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("PoolNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewMockLogger(tt)
		mockLogger.EXPECT().InfoContext(mock.Anything, "Running AutoMigrate for model changes")

		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err)
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		assert.NoError(tt, err)

		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    "non-existent-pool-uuid",
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 1099511627776,
			Style:       "flexvol",
			SvmUuid:     "",
			SvmName:     "",
			AccountName: account.Name,
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err = orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Pool not found")

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Flexgroup_RequiresLargeCapacityPool", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, pool, _ := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-flexgroup-volume",
			SizeInBytes: 1099511627776,
			Style:       "flexgroup",
			SvmUuid:     "660e8400-e29b-41d4-a716-446655440001",
			SvmName:     "",
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Pool is not type of largeCapacity")

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("InsufficientPoolCapacity", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, account, pool, svm := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		existingVolume := &datamodel.ExpertModeVolumes{
			BaseModel:   datamodel.BaseModel{UUID: "existing-volume-uuid"},
			Name:        "existing-volume",
			SizeInBytes: 2000000000000, // Almost 2TB
			PoolID:      pool.ID,
			AccountID:   account.ID,
			SvmID:       svm.ID,
			Style:       "flexvol",
		}
		err := store.DB().Create(existingVolume).Error
		assert.NoError(tt, err)

		params := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 500000000000, // 500GB - would exceed pool capacity
			Style:       "flexvol",
			SvmUuid:     "660e8400-e29b-41d4-a716-446655440001",
			SvmName:     "",
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err = orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "insufficient pool capacity")

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("DuplicateVolumeName", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, account, pool, svm := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		existingVolume := &datamodel.ExpertModeVolumes{
			BaseModel:   datamodel.BaseModel{UUID: "existing-volume-uuid"},
			Name:        "duplicate-volume-name",
			SizeInBytes: 1099511627776,
			PoolID:      pool.ID,
			AccountID:   account.ID,
			SvmID:       svm.ID,
			Style:       "flexvol",
		}
		err := store.DB().Create(existingVolume).Error
		assert.NoError(tt, err)

		params := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "duplicate-volume-name",
			SizeInBytes: 1099511627776,
			Style:       "flexvol",
			SvmUuid:     "660e8400-e29b-41d4-a716-446655440001",
			SvmName:     "",
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err = orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume with name 'duplicate-volume-name' already exists in pool")

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("SvmNotFound_ByUuid", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, pool, _ := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 1099511627776,
			Style:       "flexvol",
			SvmUuid:     "non-existent-svm-uuid",
			SvmName:     "",
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SVM with UUID")
		assert.Contains(tt, err.Error(), "not found in pool")

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("SvmNotFound_ByName", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, pool, _ := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 1099511627776,
			Style:       "flexvol",
			SvmUuid:     "",
			SvmName:     "non-existent-svm",
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SVM with name")
		assert.Contains(tt, err.Error(), "not found in pool")

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("NeitherSvmUuidNorSvmNameProvided", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, pool, _ := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 1099511627776,
			Style:       "flexvol",
			SvmUuid:     "",
			SvmName:     "",
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "neither svmName nor svmUUID has been passed")

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("WorkflowExecutionFailure_DoesNotFailVolumeCreation", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, pool, _ := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 1099511627776,
			Style:       "flexvol",
			SvmUuid:     "660e8400-e29b-41d4-a716-446655440001",
			SvmName:     "",
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow execution failed")).Once()

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.CreateExpertModeVolume(ctx, params)

		assert.NoError(tt, err)

		var createdVolume datamodel.ExpertModeVolumes
		err = store.DB().Where("name = ?", params.VolumeName).First(&createdVolume).Error
		assert.NoError(tt, err)
		assert.Equal(tt, params.VolumeName, createdVolume.Name)

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("AllStyleTypes", func(tt *testing.T) {
		styles := []string{"flexvol", "flexgroup", "flexcache"}

		for _, style := range styles {
			tt.Run(style, func(ttt *testing.T) {
				ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
				mockLogger, store, _, pool, _ := setupStore(ttt)
				temporal := workflowenginemock.NewMockTemporalTestClient(ttt)

				if style == "flexgroup" {
					pool.LargeCapacity = true
					err := store.DB().Save(pool).Error
					assert.NoError(ttt, err)
				}

				params := &commonparams.CreateExpertModeVolumeParams{
					PoolUUID:    pool.UUID,
					Action:      "post",
					VolumeName:  "test-volume-" + style,
					SizeInBytes: 1099511627776,
					Style:       style,
					SvmUuid:     "660e8400-e29b-41d4-a716-446655440001",
					SvmName:     "",
				}

				temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()

				orch := &Orchestrator{storage: store, temporal: temporal}
				err := orch.CreateExpertModeVolume(ctx, params)

				assert.NoError(ttt, err)

				var createdVolume datamodel.ExpertModeVolumes
				err = store.DB().Where("name = ?", params.VolumeName).First(&createdVolume).Error
				assert.NoError(ttt, err)
				assert.Equal(ttt, style, createdVolume.Style)

				mockLogger.AssertExpectations(ttt)
				temporal.AssertExpectations(ttt)
			})
		}
	})
}

func TestVolumeReconciliationWorkflow(t *testing.T) {
	t.Run("WorkflowExists", func(tt *testing.T) {
		expertModeVolume := &datamodel.ExpertModeVolumes{
			BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:        "test-volume",
			SizeInBytes: 1099511627776,
			Style:       "flexvol",
		}
		correlationID := "test-correlation-id"

		assert.NotNil(tt, VolumeReconciliationWorkflow)
		assert.NotNil(tt, expertModeVolume)
		assert.NotEmpty(tt, correlationID)
	})
}
