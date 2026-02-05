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
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowenginemock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

// mockWorkflowRun is a simple mock for client.WorkflowRun
type mockWorkflowRun struct {
	mock.Mock
}

func (m *mockWorkflowRun) GetID() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockWorkflowRun) GetRunID() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	args := m.Called(ctx, valuePtr)
	return args.Error(0)
}

func (m *mockWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, options client.WorkflowRunGetOptions) error {
	args := m.Called(ctx, valuePtr, options)
	return args.Error(0)
}

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

	t.Run("Failure_GetExpertModeVolumeByNameAndPoolID_NonRecordNotFoundError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}

		pool := &datamodel.Pool{
			BaseModel:     datamodel.BaseModel{ID: 1, UUID: "550e8400-e29b-41d4-a716-446655440000"},
			Name:          "test_pool",
			AccountID:     account.ID,
			SizeInBytes:   2199023255552,
			LargeCapacity: false,
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

		mockStorage.EXPECT().GetPool(ctx, "550e8400-e29b-41d4-a716-446655440000", account.ID).Return(&datamodel.PoolView{
			Pool: *pool,
		}, nil).Once()
		// Simulate a database error (not ErrRecordNotFound)
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, "my-expert-volume", pool.ID).Return(nil, errors.New("database connection error")).Once()

		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 1099511627776,
			SvmUuid:     "",
			SvmName:     "",
			AccountName: account.Name,
		}

		orch := &Orchestrator{storage: mockStorage, temporal: temporal}
		err := orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database connection error")

		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_CanFitInPool_SizeZero", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, account, pool, _ := setupStore(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 0, // Zero size should fail
			SvmUuid:     "660e8400-e29b-41d4-a716-446655440001",
			SvmName:     "",
			AccountName: account.Name,
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume size must be greater than 0")

		mockLogger.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_CanFitInPool_GetCapacityError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}

		pool := &datamodel.Pool{
			BaseModel:     datamodel.BaseModel{ID: 1, UUID: "550e8400-e29b-41d4-a716-446655440000"},
			Name:          "test_pool",
			AccountID:     account.ID,
			SizeInBytes:   2199023255552,
			LargeCapacity: false,
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

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{
			Pool: *pool,
		}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, "my-expert-volume", pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()
		// Simulate error getting capacity
		mockStorage.EXPECT().GetExpertModePoolUsedCapacityAndVolumeCount(ctx, pool.ID).Return(nil, errors.New("failed to get capacity")).Once()

		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
			PoolUUID:    pool.UUID,
			Action:      "post",
			VolumeName:  "my-expert-volume",
			SizeInBytes: 1099511627776,
			SvmUuid:     "",
			SvmName:     "",
			AccountName: account.Name,
		}

		orch := &Orchestrator{storage: mockStorage, temporal: temporal}
		err := orch.CreateExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get capacity")

		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
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
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
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
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "either volumeUUID or (volumeName and poolUUID) is required")

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
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		orch := &Orchestrator{storage: store, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		// The underlying storage layer returns gorm's "record not found" error
		assert.Contains(tt, err.Error(), "not found")

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
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
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
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
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
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		orch := &Orchestrator{storage: mockStorage, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Account not found")

		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_GetPoolFails", func(tt *testing.T) {
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
			ExternalUUID: "770e8400-e29b-41d4-a716-446655440002",
			Pool:         pool,
		}
		mockStorage := database.NewMockStorage(tt)
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(nil, errors.New("failed to get pool")).Once()
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			AccountName: account.Name,
			PoolUUID:    pool.UUID,
		}
		orch := &Orchestrator{storage: mockStorage, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get pool")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeNotAssociatedToPool", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		volumePool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "aaaaaaaa-e29b-41d4-a716-446655440001"},
			Name:      "volume_pool",
		}
		requestPool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "550e8400-e29b-41d4-a716-446655440000"},
			Name:      "request_pool",
		}
		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
			Name:         "test-volume",
			ExternalUUID: "770e8400-e29b-41d4-a716-446655440002",
			PoolID:       volumePool.ID,
			Pool:         volumePool,
		}
		mockStorage := database.NewMockStorage(tt)
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.EXPECT().GetPool(ctx, requestPool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *requestPool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			AccountName: account.Name,
			PoolUUID:    requestPool.UUID,
		}
		orch := &Orchestrator{storage: mockStorage, temporal: temporal}
		err := orch.DeleteExpertModeVolume(ctx, params)
		assert.Error(tt, err)
		assert.True(tt, customerrors.IsBadRequestErr(err))
		assert.Contains(tt, err.Error(), "not associated to the specified pool")
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

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(nil, errors.New("failed to update volume state")).Once()

		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
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

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(nil, errors.New("failed to create job")).Once()

		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
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
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
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

func TestGetExpertModeVolumeByExternalUUID(t *testing.T) {
	setupStoreForGet := func(tt *testing.T) (*log.MockLogger, database.Storage, *datamodel.Account, *datamodel.Pool, *datamodel.Svm, *datamodel.ExpertModeVolumes) {
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
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
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

		expertModeVolume := &datamodel.ExpertModeVolumes{
			Name:         "test-expert-volume",
			SizeInBytes:  1099511627776, // 1TB
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexvol",
			State:        models.LifeCycleStateREADY,
			ExternalUUID: "770e8400-e29b-41d4-a716-446655440002",
		}
		err = store.DB().Create(expertModeVolume).Error
		if err != nil {
			tt.Fatalf("Failed to create expert mode volume: %v", err)
		}

		return mockLogger, store, account, pool, svm, expertModeVolume
	}

	t.Run("Success_VolumeExists", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, _, _, _, volume := setupStoreForGet(tt)

		orch := &Orchestrator{storage: store}
		result, err := orch.GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, volume.ExternalUUID, result.ExternalUUID)
		assert.Equal(tt, volume.Name, result.Name)
		assert.Equal(tt, volume.SizeInBytes, result.SizeInBytes)
		assert.Equal(tt, volume.Style, result.Style)
		assert.Equal(tt, volume.State, result.State)

		mockLogger.AssertExpectations(tt)
	})

	t.Run("Error_VolumeNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
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

		orch := &Orchestrator{storage: store}
		result, err := orch.GetExpertModeVolumeByExternalUUID(ctx, "non-existent-uuid")

		assert.Error(tt, err)
		assert.Nil(tt, result)

		mockLogger.AssertExpectations(tt)
	})

	t.Run("Error_EmptyUUID", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
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

		orch := &Orchestrator{storage: store}
		result, err := orch.GetExpertModeVolumeByExternalUUID(ctx, "")

		assert.Error(tt, err)
		assert.Nil(tt, result)

		mockLogger.AssertExpectations(tt)
	})

	t.Run("Error_StorageError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)

		expectedErr := errors.New("database connection error")
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, "test-uuid").Return(nil, expectedErr).Once()

		orch := &Orchestrator{storage: mockStorage}
		result, err := orch.GetExpertModeVolumeByExternalUUID(ctx, "test-uuid")

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, expectedErr, err)

		mockStorage.AssertExpectations(tt)
	})
}

func Test_updateExpertModeVolume(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	setupTestData := func() (*datamodel.Account, *datamodel.Pool, *datamodel.ExpertModeVolumes) {
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}

		pool := &datamodel.Pool{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "550e8400-e29b-41d4-a716-446655440000"},
			Name:        "test_pool",
			AccountID:   account.ID,
			SizeInBytes: 2199023255552, // 2TB
		}

		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
			Name:         "test-volume",
			SizeInBytes:  1099511627776, // 1TB
			PoolID:       pool.ID,
			AccountID:    account.ID,
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "770e8400-e29b-41d4-a716-446655440002",
			Pool:         pool,
			Account:      account,
		}

		return account, pool, volume
	}

	t.Run("Success_UpdateSizeOnly", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		newSize := int64(2199023255552) // 2TB
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: newSize,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
			TrackingID: 1,
		}
		mockWorkflowRun := &mockWorkflowRun{}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModePoolUsedCapacityAndVolumeCount(ctx, pool.ID).Return(&database.ExpertModePoolCapacity{TotalSize: 0, VolumeCount: 0}, nil).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil).Once()
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockWorkflowRun, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Success_UpdateNameOnly", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		newName := "updated-volume-name"
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			VolumeName:  newName,
			SizeInBytes: 0, // No size update
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
			TrackingID: 1,
		}
		mockWorkflowRun := &mockWorkflowRun{}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, newName, pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil).Once()
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockWorkflowRun, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Success_UpdateBothNameAndSize", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		newName := "updated-volume-name"
		newSize := int64(2199023255552) // 2TB
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			VolumeName:  newName,
			SizeInBytes: newSize,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
			TrackingID: 1,
		}
		mockWorkflowRun := &mockWorkflowRun{}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, newName, pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()
		mockStorage.EXPECT().GetExpertModePoolUsedCapacityAndVolumeCount(ctx, pool.ID).Return(&database.ExpertModePoolCapacity{TotalSize: 0, VolumeCount: 0}, nil).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil).Once()
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockWorkflowRun, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeUUIDRequired", func(tt *testing.T) {
		account, pool, _ := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  "", // Empty UUID
			SizeInBytes: 2199023255552,
			AccountName: "test_account",
		}
		mockStorage.EXPECT().GetPool(ctx, "", account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "either volumeUUID or (volumeName and poolUUID) is required")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeNotFound", func(tt *testing.T) {
		account, pool, _ := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  "non-existent-volume-uuid",
			SizeInBytes: 2199023255552,
			AccountName: "test_account",
			PoolUUID:    pool.UUID,
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, params.VolumeUUID).Return(nil, gorm.ErrRecordNotFound).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume with UUID 'non-existent-volume-uuid' not found")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeNotFound_NotFoundError", func(tt *testing.T) {
		account, pool, _ := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  "non-existent-volume-uuid",
			SizeInBytes: 2199023255552,
			AccountName: "test_account",
			PoolUUID:    pool.UUID,
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, params.VolumeUUID).Return(nil, customerrors.NewNotFoundErr("volume not found", nil)).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume with UUID 'non-existent-volume-uuid' not found")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeStateDeleted", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		volume.State = models.LifeCycleStateDeleted
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			AccountName: "test_account",
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "is deleted")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeStateError", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		volume.State = models.LifeCycleStateError
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			AccountName: "test_account",
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "is deleted")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeStateCreating", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		volume.State = models.LifeCycleStateCreating
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			AccountName: "test_account",
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "is in a transitional state")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeStateDeleting", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		volume.State = models.LifeCycleStateDeleting
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			AccountName: "test_account",
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "is in a transitional state")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeStateUpdating", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		volume.State = models.LifeCycleStateUpdating
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			AccountName: "test_account",
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "is in a transitional state")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_SizeNegative", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: -1, // Negative size
			AccountName: "test_account",
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Volume size must be greater than or equal to 0")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_DuplicateVolumeName", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		// Create another volume with the duplicate name
		duplicateVolume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{ID: 2, UUID: "other-volume-uuid"},
			Name:         "duplicate-name",
			ExternalUUID: "other-external-uuid",
		}

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			VolumeName:  "duplicate-name",
			SizeInBytes: 0,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, "duplicate-name", pool.ID).Return(duplicateVolume, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume with name 'duplicate-name' already exists in pool")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Success_SameVolumeName_NoDuplicate", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		// Using the same name as the volume being updated (should not be considered duplicate)
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			VolumeName:  volume.Name, // Same name
			SizeInBytes: 0,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
			TrackingID: 1,
		}
		mockWorkflowRun := &mockWorkflowRun{}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, volume.Name, pool.ID).Return(volume, nil).Once() // Returns same volume
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil).Once()
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockWorkflowRun, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_AccountNotFound", func(tt *testing.T) {
		_, _, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName to return error
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("Account not found")
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			AccountName: "non_existent_account",
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Account not found")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_GetPoolFails", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(nil, errors.New("failed to get pool")).Once()
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			AccountName: account.Name,
			PoolUUID:    pool.UUID,
		}
		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get pool")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_AccountMismatch", func(tt *testing.T) {
		_, pool, volume := setupTestData()
		otherAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "other-account-uuid"},
			Name:      "other_account",
		}
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName to return different account
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return otherAccount, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			AccountName: otherAccount.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, "550e8400-e29b-41d4-a716-446655440000", otherAccount.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume does not belong to the specified account")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_AccountMismatch_UsingAccountID", func(tt *testing.T) {
		_, pool, volume := setupTestData()
		otherAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "other-account-uuid"},
			Name:      "other_account",
		}
		// Remove Account relationship to test AccountID path
		volume.Account = nil
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName to return different account
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return otherAccount, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			AccountName: otherAccount.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, "550e8400-e29b-41d4-a716-446655440000", otherAccount.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume does not belong to the specified account")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_InsufficientPoolCapacity", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		// Try to increase size beyond pool capacity
		// Pool size: 2TB, existing volume: 1TB, trying to add another 2TB = 3TB total (exceeds pool)
		newSize := int64(3298534883328) // 3TB
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: newSize,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		// Pool has 1TB used capacity (the existing volume), trying to add 2TB more (3TB - 1TB = 2TB increase)
		// Pool size is 2TB, so 1TB + 2TB = 3TB > 2TB, should fail
		mockStorage.EXPECT().GetExpertModePoolUsedCapacityAndVolumeCount(ctx, pool.ID).Return(&database.ExpertModePoolCapacity{TotalSize: volume.SizeInBytes, VolumeCount: 1}, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "insufficient pool capacity")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Success_SizeDecrease_NoCapacityCheck", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		// Decrease size (should not check pool capacity)
		newSize := int64(549755813888) // 512GB (half of original 1TB)
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: newSize,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
			TrackingID: 1,
		}
		mockWorkflowRun := &mockWorkflowRun{}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		// Should not call GetExpertModePoolUsedCapacityAndVolumeCount for size decrease
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil).Once()
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockWorkflowRun, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_GetExpertModeVolumeByNameAndPoolID_NonRecordNotFoundError", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			VolumeName:  "new-name",
			SizeInBytes: 0,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		// Simulate a database error (not ErrRecordNotFound)
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, "new-name", pool.ID).Return(nil, errors.New("database connection error")).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database connection error")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_GetExpertModePoolUsedCapacityAndVolumeCount_Error", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		newSize := int64(2199023255552) // 2TB
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: newSize,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModePoolUsedCapacityAndVolumeCount(ctx, pool.ID).Return(nil, errors.New("failed to get capacity")).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get capacity")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Success_SizeZero_NoSizeUpdate", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 0, // Zero means no size update
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
			TrackingID: 1,
		}
		mockWorkflowRun := &mockWorkflowRun{}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		// Should not call GetExpertModePoolUsedCapacityAndVolumeCount for zero size
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil).Once()
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockWorkflowRun, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_GetExpertModeVolumeByExternalUUID_DatabaseError", func(tt *testing.T) {
		account, pool, _ := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  "test-uuid",
			SizeInBytes: 2199023255552,
			AccountName: "test_account",
			PoolUUID:    pool.UUID,
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, params.VolumeUUID).Return(nil, errors.New("database connection error")).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database connection error")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_CreateJobFails", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		newSize := int64(2199023255552) // 2TB
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: newSize,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModePoolUsedCapacityAndVolumeCount(ctx, pool.ID).Return(&database.ExpertModePoolCapacity{TotalSize: 0, VolumeCount: 0}, nil).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(nil, errors.New("failed to create job")).Once()
		// Defer function should revert the state
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to create job")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_ExecuteWorkflowFails", func(tt *testing.T) {
		account, pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		// Mock getAccountWithName
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getAccountWithName = originalGetAccountWithName
		}()

		newSize := int64(2199023255552) // 2TB
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: newSize,
			AccountName: account.Name,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
			TrackingID: 1,
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(&datamodel.PoolView{Pool: *pool}, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByExternalUUID(ctx, volume.ExternalUUID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModePoolUsedCapacityAndVolumeCount(ctx, pool.ID).Return(&database.ExpertModePoolCapacity{TotalSize: 0, VolumeCount: 0}, nil).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil).Once()
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to start workflow")).Once()
		// Defer function should update job status and revert volume state
		mockStorage.EXPECT().UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), createdJob.TrackingID, mock.AnythingOfType("string")).Return(nil).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()

		err := _updateExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to start workflow")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})
}

func TestRenameExpertModeVolume(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	setupRenameTestData := func() (*datamodel.Account, *datamodel.Pool, *datamodel.Svm, *datamodel.ExpertModeVolumes) {
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		pool := &datamodel.Pool{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "550e8400-e29b-41d4-a716-446655440000"},
			Name:        "test_pool",
			AccountID:   account.ID,
			SizeInBytes: 2199023255552,
		}
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "svm-uuid"},
			Name:      "test-svm",
			PoolID:    pool.ID,
			AccountID: account.ID,
		}
		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "770e8400-e29b-41d4-a716-446655440002",
			Pool:         pool,
			Account:      account,
			Svm:          svm,
		}
		return account, pool, svm, volume
	}

	poolViewFromPool := func(pool *datamodel.Pool) *datamodel.PoolView {
		return &datamodel.PoolView{Pool: *pool}
	}

	t.Run("Success_RenameVolume", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed-volume",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: account.Name,
		}
		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
			TrackingID: 1,
		}
		mockWorkflowRun := &mockWorkflowRun{}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.NewName, pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil).Once()
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockWorkflowRun, nil).Once()

		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Success_OrchestratorRenameExpertModeVolume", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed-volume",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: account.Name,
		}
		createdJob := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}, WorkflowID: "workflow-id", TrackingID: 1}
		mockWorkflowRun := &mockWorkflowRun{}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.NewName, pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil).Once()
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockWorkflowRun, nil).Once()

		orch := &Orchestrator{storage: mockStorage, temporal: temporal}
		err := orch.RenameExpertModeVolume(ctx, params)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeNotFound", func(tt *testing.T) {
		account, pool, _, _ := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  "nonexistent",
			NewName:     "renamed",
			PoolUUID:    pool.UUID,
			SvmName:     "test-svm",
			AccountName: account.Name,
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()

		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.True(tt, customerrors.IsBadRequestErr(err))
		assert.Contains(tt, err.Error(), "not found")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeHasNoSvm", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		volume.Svm = nil
		volume.SvmID = 0
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed-volume",
			PoolUUID:    pool.UUID,
			SvmName:     "test-svm",
			AccountName: account.Name,
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(volume, nil).Once()

		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.True(tt, customerrors.IsBadRequestErr(err))
		assert.Contains(tt, err.Error(), "no SVM")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_SvmNameMismatch", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed-volume",
			PoolUUID:    pool.UUID,
			SvmName:     "wrong-svm-name",
			AccountName: account.Name,
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(volume, nil).Once()

		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.True(tt, customerrors.IsBadRequestErr(err))
		assert.Contains(tt, err.Error(), "SVM name")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_NewNameAlreadyExists", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		existingVolume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: "other-uuid"},
			Name:         "renamed-volume",
			ExternalUUID: "other-external-uuid",
			PoolID:       pool.ID,
		}
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed-volume",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: account.Name,
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.NewName, pool.ID).Return(existingVolume, nil).Once()

		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.True(tt, customerrors.IsBadRequestErr(err))
		assert.Contains(tt, err.Error(), "already exists")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeInTransitionalState", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		volume.State = models.LifeCycleStateUpdating
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed-volume",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: account.Name,
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(volume, nil).Once()

		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.True(tt, customerrors.IsBadRequestErr(err))
		assert.Contains(tt, err.Error(), "transitional")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_ExecuteWorkflowFails_RevertsState", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)

		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed-volume",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: account.Name,
		}
		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
			TrackingID: 1,
		}

		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.NewName, pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil).Once()
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow failed")).Once()
		mockStorage.EXPECT().UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), createdJob.TrackingID, mock.AnythingOfType("string")).Return(nil).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()

		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "workflow failed")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_AccountNotFound", func(tt *testing.T) {
		_, pool, _, volume := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()
		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: "missing",
		}
		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "account not found")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_GetPoolFails", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()
		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(nil, errors.New("failed to get pool by UUID")).Once()
		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: account.Name,
		}
		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get pool by UUID")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_GetVolumeByName_DatabaseError", func(tt *testing.T) {
		account, pool, _, _ := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()
		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, "my-vol", pool.ID).Return(nil, errors.New("database connection error")).Once()
		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  "my-vol",
			NewName:     "renamed",
			PoolUUID:    pool.UUID,
			SvmName:     "test-svm",
			AccountName: account.Name,
		}
		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database connection error")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeDoesNotBelongToAccount", func(tt *testing.T) {
		_, pool, _, volume := setupRenameTestData()
		volume.AccountID = 1
		otherAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "other-account-uuid"},
			Name:      "other_account",
		}
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return otherAccount, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()
		mockStorage.EXPECT().GetPool(ctx, pool.UUID, otherAccount.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, volume.Name, pool.ID).Return(volume, nil).Once()
		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: otherAccount.Name,
		}
		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.True(tt, customerrors.IsBadRequestErr(err))
		assert.Contains(tt, err.Error(), "does not belong to the specified account")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_CheckNewNameExists_DatabaseError", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()
		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, volume.Name, pool.ID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, "new-name", pool.ID).Return(nil, errors.New("db error")).Once()
		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "new-name",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: account.Name,
		}
		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "db error")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_CreateJobFails", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()
		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, volume.Name, pool.ID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, "renamed", pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(nil, errors.New("failed to create job for expert mode volume rename")).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: account.Name,
		}
		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to create job")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_UpdateExpertModeVolumeFails", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()
		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, volume.Name, pool.ID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, "renamed", pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(nil, errors.New("Failed to update volume name and state for rename")).Once()
		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: account.Name,
		}
		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Failed to update volume name and state for rename")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})

	t.Run("Failure_ExecuteWorkflowFails_UpdateJobFails", func(tt *testing.T) {
		account, pool, _, volume := setupRenameTestData()
		mockStorage := database.NewMockStorage(tt)
		temporal := workflowenginemock.NewMockTemporalTestClient(tt)
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()
		createdJob := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}, WorkflowID: "workflow-id", TrackingID: 1}
		mockStorage.EXPECT().GetPool(ctx, pool.UUID, account.ID).Return(poolViewFromPool(pool), nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, volume.Name, pool.ID).Return(volume, nil).Once()
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, "renamed", pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		mockStorage.EXPECT().CreateJob(ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil).Once()
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow failed")).Once()
		mockStorage.EXPECT().UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), createdJob.TrackingID, mock.AnythingOfType("string")).Return(errors.New("failed to update job status to error")).Once()
		mockStorage.EXPECT().UpdateExpertModeVolume(ctx, mock.AnythingOfType("*datamodel.ExpertModeVolumes")).Return(volume, nil).Once()
		params := &commonparams.ExpertModeVolumeRenameParams{
			VolumeName:  volume.Name,
			NewName:     "renamed",
			PoolUUID:    pool.UUID,
			SvmName:     volume.Svm.Name,
			AccountName: account.Name,
		}
		err := _renameExpertModeVolume(ctx, mockStorage, temporal, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "workflow failed")
		mockStorage.AssertExpectations(tt)
		temporal.AssertExpectations(tt)
	})
}

func TestGetExpertModeVolume(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	t.Run("Failure_ByName_DatabaseError", func(tt *testing.T) {
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test_account"}
		pool := &datamodel.Pool{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "550e8400-e29b-41d4-a716-446655440000"},
			Name:        "test_pool",
			AccountID:   account.ID,
			SizeInBytes: 2199023255552,
		}
		dbPoolView := &datamodel.PoolView{Pool: *pool}
		mockStorage := database.NewMockStorage(tt)
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  "",
			VolumeName:  "my-volume",
			AccountName: account.Name,
			PoolUUID:    pool.UUID,
		}
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, "my-volume", pool.ID).Return(nil, errors.New("database error")).Once()
		vol, err := getExpertModeVolume(ctx, mockStorage, params, dbPoolView)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
		assert.Contains(tt, err.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Failure_ByName_NilPoolView", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID: "",
			VolumeName: "my-volume",
			PoolUUID:   "550e8400-e29b-41d4-a716-446655440000",
		}
		vol, err := getExpertModeVolume(ctx, mockStorage, params, nil)
		assert.Error(tt, err)
		assert.Nil(tt, vol)
		assert.True(tt, customerrors.IsBadRequestErr(err))
		assert.Contains(tt, err.Error(), "volume not found")
		mockStorage.AssertExpectations(tt)
	})
}

func TestValidateUpdateParams(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	setupTestData := func() (*datamodel.Pool, *datamodel.ExpertModeVolumes) {
		pool := &datamodel.Pool{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "550e8400-e29b-41d4-a716-446655440000"},
			Name:        "test_pool",
			SizeInBytes: 2199023255552, // 2TB
		}

		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
			Name:         "test-volume",
			SizeInBytes:  1099511627776, // 1TB
			PoolID:       pool.ID,
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "770e8400-e29b-41d4-a716-446655440002",
			Pool:         pool,
		}

		return pool, volume
	}

	t.Run("Success_ValidParams", func(tt *testing.T) {
		_, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Success_ValidParams_WithVolumeName_NoConflict", func(tt *testing.T) {
		pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			VolumeName:  "new-volume-name",
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(nil, gorm.ErrRecordNotFound).Once()

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Success_ValidParams_WithVolumeName_SameVolume", func(tt *testing.T) {
		pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			VolumeName:  volume.Name, // Same name as current volume
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		// When checking for existing volume, it returns the same volume (same ExternalUUID)
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(volume, nil).Once()

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Failure_IncorrectPoolUUID", func(tt *testing.T) {
		_, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  "550e8400", // Empty UUID
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440001",
		}

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume is not associated to the pool for update operation")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Failure_NegativeSizeInBytes", func(tt *testing.T) {
		_, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: -1, // Negative size
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Volume size must be greater than or equal to 0")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeStateDeleted", func(tt *testing.T) {
		_, volume := setupTestData()
		volume.State = models.LifeCycleStateDeleted
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "is deleted")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeStateError", func(tt *testing.T) {
		_, volume := setupTestData()
		volume.State = models.LifeCycleStateError
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "is deleted")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeStateCreating", func(tt *testing.T) {
		_, volume := setupTestData()
		volume.State = models.LifeCycleStateCreating
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "is in a transitional state")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeStateDeleting", func(tt *testing.T) {
		_, volume := setupTestData()
		volume.State = models.LifeCycleStateDeleting
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "is in a transitional state")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeStateUpdating", func(tt *testing.T) {
		_, volume := setupTestData()
		volume.State = models.LifeCycleStateUpdating
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "is in a transitional state")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Failure_VolumeNameConflict_DifferentVolume", func(tt *testing.T) {
		pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)

		duplicateVolume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{ID: 2, UUID: "different-volume-uuid"},
			Name:         "duplicate-name",
			ExternalUUID: "880e8400-e29b-41d4-a716-446655440003", // Different UUID
			PoolID:       pool.ID,
		}

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			VolumeName:  "duplicate-name",
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(duplicateVolume, nil).Once()

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume with name 'duplicate-name' already exists in pool")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Failure_GetExpertModeVolumeByNameAndPoolID_DatabaseError", func(tt *testing.T) {
		pool, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			VolumeName:  "new-name",
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		dbError := errors.New("database connection error")
		mockStorage.EXPECT().GetExpertModeVolumeByNameAndPoolID(ctx, params.VolumeName, pool.ID).Return(nil, dbError).Once()

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.Error(tt, err)
		assert.Equal(tt, dbError, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Success_ZeroSizeInBytes", func(tt *testing.T) {
		_, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			SizeInBytes: 0, // Zero size is allowed
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Success_EmptyVolumeName", func(tt *testing.T) {
		_, volume := setupTestData()
		mockStorage := database.NewMockStorage(tt)

		params := &commonparams.ExpertModeVolumeParams{
			VolumeUUID:  volume.ExternalUUID,
			VolumeName:  "", // Empty name should not trigger name validation
			SizeInBytes: 2199023255552,
			PoolUUID:    "550e8400-e29b-41d4-a716-446655440000",
		}

		err := validateUpdateParams(ctx, mockStorage, params, volume)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestGetBackupConfigsForPool(t *testing.T) {
	setupStoreForBackupConfigs := func(tt *testing.T) (*log.MockLogger, database.Storage, *datamodel.Account, *datamodel.Pool, *datamodel.Svm) {
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
			SizeInBytes:    2199023255552,
			APIAccessMode:  "ONTAP",
			DeploymentName: "test-deployment",
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

	t.Run("Success_WithBackupConfigs", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, account, pool, svm := setupStoreForBackupConfigs(tt)
		defer mockLogger.AssertExpectations(tt)

		// Create volumes with backup configs
		backupConfig1 := &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
			ScheduledBackupEnabled: func() *bool {
				b := true
				return &b
			}(),
		}
		volume1 := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:         "volume-1",
			SizeInBytes:  1099511627776,
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexvol",
			ExternalUUID: "external-uuid-1",
			State:        models.LifeCycleStateREADY,
			BackupConfig: backupConfig1,
		}
		err := store.DB().Create(volume1).Error
		assert.NoError(tt, err)

		backupConfig2 := &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-2",
			ScheduledBackupEnabled: func() *bool {
				b := false
				return &b
			}(),
		}
		volume2 := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: "volume-uuid-2"},
			Name:         "volume-2",
			SizeInBytes:  536870912000,
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexgroup",
			ExternalUUID: "external-uuid-2",
			State:        models.LifeCycleStateREADY,
			BackupConfig: backupConfig2,
		}
		err = store.DB().Create(volume2).Error
		assert.NoError(tt, err)

		orch := &Orchestrator{storage: store}
		backupConfigs, err := orch.GetBackupConfigsForPool(ctx, pool.UUID, account.Name)

		assert.NoError(tt, err)
		assert.NotNil(tt, backupConfigs)
		assert.Equal(tt, 2, len(backupConfigs))

		// Create map for easier verification
		configMap := make(map[string]*models.ExpertModeVolumeBackupConfig)
		for _, config := range backupConfigs {
			configMap[config.VolumeID] = config
		}

		// Verify volume 1 config
		config1, exists := configMap["external-uuid-1"]
		assert.True(tt, exists)
		assert.NotNil(tt, config1.BackupVaultID)
		assert.Equal(tt, "backup-vault-uuid-1", *config1.BackupVaultID)

		// Verify volume 2 config
		config2, exists := configMap["external-uuid-2"]
		assert.True(tt, exists)
		assert.NotNil(tt, config2.BackupVaultID)
		assert.Equal(tt, "backup-vault-uuid-2", *config2.BackupVaultID)
	})

	t.Run("Success_MixedBackupConfigs", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, account, pool, svm := setupStoreForBackupConfigs(tt)
		defer mockLogger.AssertExpectations(tt)

		// Volume with backup config
		backupConfig := &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		}
		volume1 := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:         "volume-1",
			SizeInBytes:  1099511627776,
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexvol",
			ExternalUUID: "external-uuid-1",
			State:        models.LifeCycleStateREADY,
			BackupConfig: backupConfig,
		}
		err := store.DB().Create(volume1).Error
		assert.NoError(tt, err)

		// Volume without backup config
		volume2 := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: "volume-uuid-2"},
			Name:         "volume-2",
			SizeInBytes:  536870912000,
			PoolID:       pool.ID,
			AccountID:    account.ID,
			SvmID:        svm.ID,
			Style:        "flexvol",
			ExternalUUID: "external-uuid-2",
			State:        models.LifeCycleStateREADY,
		}
		err = store.DB().Create(volume2).Error
		assert.NoError(tt, err)

		orch := &Orchestrator{storage: store}
		backupConfigs, err := orch.GetBackupConfigsForPool(ctx, pool.UUID, account.Name)

		assert.NoError(tt, err)
		assert.NotNil(tt, backupConfigs)
		assert.Equal(tt, 2, len(backupConfigs))

		// Create map for easier verification
		configMap := make(map[string]*models.ExpertModeVolumeBackupConfig)
		for _, config := range backupConfigs {
			configMap[config.VolumeID] = config
		}

		// Verify volume 1 has backup config
		config1, exists := configMap["external-uuid-1"]
		assert.True(tt, exists)
		assert.NotNil(tt, config1.BackupVaultID)
		assert.Equal(tt, "backup-vault-uuid-1", *config1.BackupVaultID)

		// Verify volume 2 has no backup config
		config2, exists := configMap["external-uuid-2"]
		assert.True(tt, exists)
		assert.Nil(tt, config2.BackupVaultID)
	})

	t.Run("Success_EmptyPool", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, account, pool, _ := setupStoreForBackupConfigs(tt)
		defer mockLogger.AssertExpectations(tt)

		orch := &Orchestrator{storage: store}
		backupConfigs, err := orch.GetBackupConfigsForPool(ctx, pool.UUID, account.Name)

		assert.NoError(tt, err)
		assert.NotNil(tt, backupConfigs)
		assert.Equal(tt, 0, len(backupConfigs))
	})

	t.Run("Error_PoolNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, account, _, _ := setupStoreForBackupConfigs(tt)
		defer mockLogger.AssertExpectations(tt)

		orch := &Orchestrator{storage: store}
		backupConfigs, err := orch.GetBackupConfigsForPool(ctx, "non-existent-pool-uuid", account.Name)

		assert.Error(tt, err)
		assert.Nil(tt, backupConfigs)
	})

	t.Run("Error_PoolNotONTAPMode", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger, store, account, _, _ := setupStoreForBackupConfigs(tt)
		defer mockLogger.AssertExpectations(tt)

		// Create a non-ONTAP pool
		nonOntapPool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "non-ontap-pool-uuid"},
			Name:           "non_ontap_pool",
			AccountID:      account.ID,
			SizeInBytes:    2199023255552,
			APIAccessMode:  "REST",
			DeploymentName: "non-ontap-deployment",
			PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"},
		}
		err := store.DB().Create(nonOntapPool).Error
		assert.NoError(tt, err)

		orch := &Orchestrator{storage: store}
		backupConfigs, err := orch.GetBackupConfigsForPool(ctx, nonOntapPool.UUID, account.Name)

		assert.Error(tt, err)
		assert.Nil(tt, backupConfigs)
		assert.Contains(tt, err.Error(), "backup configurations are only available for ONTAP pools")
	})
}
