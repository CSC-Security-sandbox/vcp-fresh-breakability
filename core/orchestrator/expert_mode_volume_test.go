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
	expertModeWorkflows "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/expertMode"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowenginemock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"gorm.io/gorm"
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

		params := &commonparams.ExpertModeVolumeParams{
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
		// ExternalUUID is only populated after the volume is fetched from ONTAP in the workflow
		assert.Empty(tt, createdVolume.ExternalUUID)

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Success_WithSvmName", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, pool, svm := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
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

		params := &commonparams.ExpertModeVolumeParams{
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

		params := &commonparams.ExpertModeVolumeParams{
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

		params := &commonparams.ExpertModeVolumeParams{
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

		params := &commonparams.ExpertModeVolumeParams{
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

		params := &commonparams.ExpertModeVolumeParams{
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

		params := &commonparams.ExpertModeVolumeParams{
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

		params := &commonparams.ExpertModeVolumeParams{
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

		params := &commonparams.ExpertModeVolumeParams{
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

	t.Run("GetExpertModeVolumeByUUID_Fails", func(tt *testing.T) {
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

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "550e8400-e29b-41d4-a716-446655440000"},
			Name:           "test_pool",
			AccountID:      account.ID,
			SizeInBytes:    2199023255552, // 2TB
			LargeCapacity:  false,
			PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"},
		}
		err = store.DB().Create(pool).Error
		assert.NoError(tt, err)

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
		assert.NoError(tt, err)

		params := &commonparams.ExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 1099511627776, // 1TB
			Style:       "flexvol",
			SvmUuid:     "660e8400-e29b-41d4-a716-446655440001",
			SvmName:     "",
			AccountName: account.Name,
		}

		// Create a mock storage that will fail on GetExpertModeVolumeByUUID
		mockStorage := database.NewMockStorage(tt)

		// Mock getAccountWithName to return the account
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		// Mock all the calls that happen before GetExpertModeVolumeByUUID
		mockStorage.EXPECT().GetPool(ctx, params.PoolUUID, account.ID).Return(&datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: pool.ID},
				Name:           pool.Name,
				AccountID:      account.ID,
				SizeInBytes:    pool.SizeInBytes,
				LargeCapacity:  pool.LargeCapacity,
				PoolAttributes: pool.PoolAttributes,
			},
		}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()
		mockStorage.EXPECT().GetExpertModePoolUsedCapacityAndVolumeCount(ctx, pool.ID).Return(&database.ExpertModePoolCapacity{TotalSize: 0, VolumeCount: 0}, nil).Once()
		mockStorage.EXPECT().GetSvmByExternalUUID(ctx, params.SvmUuid, pool.ID).Return(svm, nil).Once()
		createdVolume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		}
		mockStorage.EXPECT().CreateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(createdVolume, nil).Once()
		// This is where we simulate the failure
		mockStorage.EXPECT().GetExpertModeVolumeByUUID(ctx, createdVolume.UUID).Return(nil, errors.New("failed to get volume with preloads")).Once()

		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		orch := &Orchestrator{storage: mockStorage, temporal: temporal}
		err = orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get volume with preloads")

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WorkflowExecutionFailure_ReturnsError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, pool, _ := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
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

		// When workflow fails to start, the function should return the error
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "workflow execution failed")

		// Volume should still be created in DB (in CREATING state)
		var createdVolume datamodel.ExpertModeVolumes
		err = store.DB().Where("name = ?", params.VolumeName).First(&createdVolume).Error
		assert.NoError(tt, err)
		assert.Equal(tt, params.VolumeName, createdVolume.Name)
		assert.Equal(tt, models.LifeCycleStateCreating, createdVolume.State)

		// Job should be marked as ERROR
		var job datamodel.Job
		err = store.DB().Where("resource_name = ?", params.VolumeName).First(&job).Error
		assert.NoError(tt, err)
		assert.Equal(tt, string(models.JobsStateERROR), job.State)

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

				params := &commonparams.ExpertModeVolumeParams{
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

		// Verify the workflow function exists in the expertMode package
		assert.NotNil(tt, expertModeWorkflows.VolumeCreateReconciliationWorkflow)
		assert.NotNil(tt, expertModeVolume)
		assert.NotEmpty(tt, correlationID)
	})
}

func TestDeleteExpertModeVolume(t *testing.T) {
	setupStore := func(tt *testing.T) (*log.MockLogger, database.Storage, *datamodel.Account, *datamodel.Pool, *datamodel.Svm, *datamodel.ExpertModeVolumes) {
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

		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:         "test-volume",
			SizeInBytes:  1099511627776, // 1TB
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexvol",
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "770e8400-e29b-41d4-a716-446655440002",
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		return mockLogger, store, account, pool, svm, volume
	}

	t.Run("Success_VolumeDeleted", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, _, _, volume := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			AccountName: "test_account",
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)

		assert.NoError(tt, err)

		// Verify volume state is updated to DELETING
		var updatedVolume datamodel.ExpertModeVolumes
		err = store.DB().Where("uuid = ?", volume.UUID).First(&updatedVolume).Error
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateDeleting, updatedVolume.State)

		// Verify job was created
		var job datamodel.Job
		err = store.DB().Where("resource_name = ?", volume.Name).First(&job).Error
		assert.NoError(tt, err)
		assert.Equal(tt, string(models.JobTypeDeleteExpertModeVolume), job.Type)
		assert.Equal(tt, string(models.JobsStateNEW), job.State)

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeUUIDRequired", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, _, _, _ := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  "", // Empty UUID
			AccountName: "test_account",
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "VolumeUUID is required for delete operation")

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, _, _, _ := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  "non-existent-volume-uuid",
			AccountName: "test_account",
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		// The underlying storage layer returns gorm's "record not found" error
		assert.Contains(tt, err.Error(), "record not found")

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Success_VolumeAlreadyDeleted", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, _, _, volume := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mark volume as already deleted
		volume.State = models.LifeCycleStateDeleted
		err := store.DB().Save(volume).Error
		assert.NoError(tt, err)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			AccountName: "test_account",
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err = orch.DeleteExpertModeVolume(ctx, params)

		// Should return nil without starting workflow
		assert.NoError(tt, err)

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_WorkflowExecutionFails_RevertsState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, _, _, volume := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalState := volume.State

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			AccountName: "test_account",
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow execution failed")).Once()

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)

		// Should return error
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "workflow execution failed")

		// Verify volume state is reverted to original state
		var updatedVolume datamodel.ExpertModeVolumes
		err = store.DB().Where("uuid = ?", volume.UUID).First(&updatedVolume).Error
		assert.NoError(tt, err)
		assert.Equal(tt, originalState, updatedVolume.State)

		// Verify job was marked as ERROR
		var job datamodel.Job
		err = store.DB().Where("resource_name = ?", volume.Name).First(&job).Error
		assert.NoError(tt, err)
		assert.Equal(tt, string(models.JobsStateERROR), job.State)

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_AccountNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockStorage := database.NewMockStorage(tt)

		// Mock getAccountWithName to return error
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("Account not found")
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  "some-volume-uuid",
			AccountName: "non_existent_account",
		}

		orch := &Orchestrator{storage: mockStorage, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Account not found")

		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_UpdateVolumeStateFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "550e8400-e29b-41d4-a716-446655440000"},
			Name:      "test_pool",
		}

		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			PoolID:       pool.ID,
			AccountID:    account.ID,
			Style:        "flexvol",
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "770e8400-e29b-41d4-a716-446655440002",
			Pool:         pool,
		}

		mockStorage := database.NewMockStorage(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(nil, errors.New("failed to update volume state")).Once()

		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			AccountName: account.Name,
		}

		orch := &Orchestrator{storage: mockStorage, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to update volume state")

		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_CreateJobFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "550e8400-e29b-41d4-a716-446655440000"},
			Name:      "test_pool",
		}

		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			PoolID:       pool.ID,
			AccountID:    account.ID,
			Style:        "flexvol",
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "770e8400-e29b-41d4-a716-446655440002",
			Pool:         pool,
		}

		mockStorage := database.NewMockStorage(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(nil, errors.New("failed to create job")).Once()

		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			AccountName: account.Name,
		}

		orch := &Orchestrator{storage: mockStorage, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to create job")

		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("VolumeInDeletingState_StartsWorkflow", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, _, _, volume := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Set volume to DELETING state (not DELETED)
		volume.State = models.LifeCycleStateDeleting
		err := store.DB().Save(volume).Error
		assert.NoError(tt, err)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			AccountName: "test_account",
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()

		orch := &Orchestrator{storage: store, temporal: temporal}
		err = orch.DeleteExpertModeVolume(ctx, params)

		// Should still start the workflow (only DELETED state returns early)
		assert.NoError(tt, err)

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})
}
