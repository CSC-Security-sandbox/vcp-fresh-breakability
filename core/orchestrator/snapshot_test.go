package orchestrator

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"gorm.io/gorm"
)

// MockStorage wraps the real storage to inject errors for testing
type MockStorage struct {
	database.Storage
	getJobsWithConditionError      error
	getSnapshotsWithConditionError error
}

// GetJobsWithCondition overrides the real method to return an error when needed
func (m *MockStorage) GetJobsWithCondition(ctx context.Context, filter utils2.Filter) ([]*datamodel.Job, error) {
	if m.getJobsWithConditionError != nil {
		return nil, m.getJobsWithConditionError
	}
	return m.Storage.GetJobsWithCondition(ctx, filter)
}

// GetSnapshotsWithCondition overrides the real method to return an error when needed
func (m *MockStorage) GetSnapshotsWithCondition(ctx context.Context, filter utils2.Filter) ([]*datamodel.Snapshot, error) {
	if m.getSnapshotsWithConditionError != nil {
		return nil, m.getSnapshotsWithConditionError
	}
	return m.Storage.GetSnapshotsWithCondition(ctx, filter)
}

func assertErrContainsOriginal(t *testing.T, err error, substring string) {
	t.Helper()
	var customErr *vsaerrors.CustomError
	if vsaerrors.As(err, &customErr) && customErr.Unwrap() != nil {
		assert.Contains(t, customErr.Unwrap().Error(), substring)
		return
	}
	assert.ErrorContains(t, err, substring)
}

func TestOrchestrator_CreateSnapshot(t *testing.T) {
	t.Run("WhenSnapshotCreationSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		params := &common.CreateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			Name:            "test_snapshot",
			IsAppConsistent: false,
			Description:     "test",
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()

		snapshot, _, err := orch.CreateSnapshot(ctx, params)
		assert.NoError(tt, err, "Failed to create snapshot")
		assert.NotNil(tt, snapshot, "Expected snapshot to be created")
		assert.Equal(tt, snapshot.Name, "test_snapshot")
		assert.Equal(tt, snapshot.VolumeUUID, "test-volume-uuid")
	})

	t.Run("WhenSnapshotCreationReturnsOngoingJobs", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		existingSnapshot := &datamodel.Snapshot{
			BaseModel:   datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:        "test_snapshot",
			Description: "desc",
			AccountID:   account.ID,
			VolumeID:    volume.ID,
			Account:     account,
			Volume:      volume,
			State:       models.LifeCycleStateCreating,
		}
		err = store.DB().Create(existingSnapshot).Error
		assert.NoError(tt, err)

		job := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "test-job-uuid"},
			Type:         string(models.JobTypeCreateSnapshot),
			State:        string(models.JobsStatePROCESSING),
			ResourceName: "test_snapshot",
			AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: volume.UUID,
			},
		}
		err = store.DB().Create(job).Error
		assert.NoError(tt, err)

		params := &common.CreateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			Name:            "test_snapshot",
			IsAppConsistent: false,
			Description:     "test",
		}

		snapshot, jobUUID, err := orch.CreateSnapshot(ctx, params)
		assert.NoError(tt, err, "Failed to create snapshot")
		assert.Equal(tt, "test-job-uuid", jobUUID, "Expected job UUID to be returned")
		assert.NotNil(tt, snapshot, "Expected snapshot to be created")
		assert.Equal(tt, snapshot.Name, "test_snapshot")
		assert.Equal(tt, snapshot.VolumeUUID, "test-volume-uuid")
	})

	t.Run("WhenSnapshotCreationFailsAsVolumeHasAppConsistentSnapshot", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		existingSnapshot := &datamodel.Snapshot{
			BaseModel:       datamodel.BaseModel{UUID: "another-test-snapshot-uuid"},
			Name:            "another_test_snapshot",
			Description:     "desc",
			AccountID:       account.ID,
			VolumeID:        volume.ID,
			Account:         account,
			Volume:          volume,
			State:           models.LifeCycleStateREADY,
			IsAppConsistent: true,
		}
		err = store.DB().Create(existingSnapshot).Error
		assert.NoError(tt, err)

		params := &common.CreateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			Name:            "test_snapshot",
			IsAppConsistent: true,
			Description:     "test",
		}

		snapshot, _, err := orch.CreateSnapshot(ctx, params)
		assert.Error(tt, err, "Volume already has an app consistent snapshot")
		assert.Nil(tt, snapshot, "Expected snapshot to be created")
	})

	t.Run("WhenSnapshotCreationFailsDueToOwnershipCheck", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID + 1,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		params := &common.CreateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			Name:            "test_snapshot",
			IsAppConsistent: false,
			Description:     "test",
		}

		snapshot, _, err := orch.CreateSnapshot(ctx, params)
		assert.Error(tt, err, "Failed to create snapshot")
		assert.Nil(tt, snapshot, "Expected nil snapshot")
	})

	t.Run("WhenSnapshotCreationFailsDueToVolumeNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		params := &common.CreateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "volume.UUID",
				AccountName: account.Name,
			},
			Name:            "test_snapshot",
			IsAppConsistent: false,
			Description:     "test",
		}

		snapshot, _, err := orch.CreateSnapshot(ctx, params)
		assert.Nil(tt, snapshot, "Expected nil snapshot")
		assert.ErrorContains(tt, err, "not found")
	})

	t.Run("WhenSnapshotCreationFailsDueToAccountNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		params := &common.CreateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: "account.Name",
			},
			Name:            "test_snapshot",
			IsAppConsistent: false,
			Description:     "test",
		}
		snapshot, _, err := orch.CreateSnapshot(ctx, params)
		assert.Nil(tt, snapshot, "Expected nil snapshot")
		if !errors.IsNotFoundErr(err) {
			t.Errorf("Expected not found error, got %v", err)
		}
	})

	t.Run("WhenSnapshotCreationFailsDueToWorkflowError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		params := &common.CreateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			Name:            "test_snapshot",
			IsAppConsistent: false,
			Description:     "test",
		}

		// Mock ExecuteWorkflow to fail with a retryable error (connection refused is retryable)
		// WorkflowExecutor will retry up to 3 times for retryable errors
		workflowErr := errors.New("connection refused")
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowErr).Times(3)

		snapshot, _, err := orch.CreateSnapshot(ctx, params)
		assert.Nil(tt, snapshot, "Expected nil snapshot")
		assert.Contains(tt, err.Error(), "connection refused")
	})

	t.Run("WhenSnapshotCreationWithSameNameInDifferentVolumes", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		// Create two different volumes
		volume1 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-1-uuid"},
			Name:      "test_volume_1",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume1).Error
		if err != nil {
			tt.Fatalf("Failed to create volume1: %v", err)
		}

		volume2 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-2-uuid"},
			Name:      "test_volume_2",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume2).Error
		if err != nil {
			tt.Fatalf("Failed to create volume2: %v", err)
		}

		// Create existing snapshot in volume1
		existingSnapshot1 := &datamodel.Snapshot{
			BaseModel:   datamodel.BaseModel{UUID: "test-snapshot-1-uuid"},
			Name:        "test_snapshot",
			Description: "desc",
			AccountID:   account.ID,
			VolumeID:    volume1.ID,
			Account:     account,
			Volume:      volume1,
			State:       models.LifeCycleStateCreating,
		}
		err = store.DB().Create(existingSnapshot1).Error
		assert.NoError(tt, err)

		// Create job for volume1 snapshot
		job1 := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "test-job-1-uuid"},
			Type:         string(models.JobTypeCreateSnapshot),
			State:        string(models.JobsStatePROCESSING),
			ResourceName: "test_snapshot",
			AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: volume1.UUID,
			},
		}
		err = store.DB().Create(job1).Error
		assert.NoError(tt, err)

		// Create existing snapshot in volume2
		existingSnapshot2 := &datamodel.Snapshot{
			BaseModel:   datamodel.BaseModel{UUID: "test-snapshot-2-uuid"},
			Name:        "test_snapshot",
			Description: "desc",
			AccountID:   account.ID,
			VolumeID:    volume2.ID,
			Account:     account,
			Volume:      volume2,
			State:       models.LifeCycleStateCreating,
		}
		err = store.DB().Create(existingSnapshot2).Error
		assert.NoError(tt, err)

		// Create job for volume2 snapshot
		job2 := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "test-job-2-uuid"},
			Type:         string(models.JobTypeCreateSnapshot),
			State:        string(models.JobsStatePROCESSING),
			ResourceName: "test_snapshot",
			AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: volume2.UUID,
			},
		}
		err = store.DB().Create(job2).Error
		assert.NoError(tt, err)

		// Test creating snapshot in volume1 - should return job1
		params1 := &common.CreateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume1.UUID,
				AccountName: account.Name,
			},
			Name:            "test_snapshot",
			IsAppConsistent: false,
			Description:     "test",
		}

		snapshot1, jobUUID1, err := orch.CreateSnapshot(ctx, params1)
		assert.NoError(tt, err, "Failed to create snapshot for volume1")
		assert.Equal(tt, "test-job-1-uuid", jobUUID1, "Expected job1 UUID to be returned for volume1")
		assert.NotNil(tt, snapshot1, "Expected snapshot to be returned for volume1")
		assert.Equal(tt, snapshot1.Name, "test_snapshot")
		assert.Equal(tt, snapshot1.VolumeUUID, "test-volume-1-uuid")

		// Test creating snapshot in volume2 - should return job2 (not job1)
		params2 := &common.CreateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume2.UUID,
				AccountName: account.Name,
			},
			Name:            "test_snapshot",
			IsAppConsistent: false,
			Description:     "test",
		}

		snapshot2, jobUUID2, err := orch.CreateSnapshot(ctx, params2)
		assert.NoError(tt, err, "Failed to create snapshot for volume2")
		assert.Equal(tt, "test-job-2-uuid", jobUUID2, "Expected job2 UUID to be returned for volume2")
		assert.NotNil(tt, snapshot2, "Expected snapshot to be returned for volume2")
		assert.Equal(tt, snapshot2.Name, "test_snapshot")
		assert.Equal(tt, snapshot2.VolumeUUID, "test-volume-2-uuid")

		// Verify that different job UUIDs are returned for different volumes
		assert.NotEqual(tt, jobUUID1, jobUUID2, "Expected different job UUIDs for different volumes")
	})

	t.Run("WhenSnapshotCreationFailsDueToGetSnapshotsWithConditionError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		// Mock the storage to return an error when GetSnapshotsWithCondition is called
		origStorage := orch.storage
		mockStorage := &MockStorage{
			Storage:                        origStorage,
			getSnapshotsWithConditionError: errors.New("database connection failed"),
		}
		orch.storage = mockStorage
		defer func() { orch.storage = origStorage }()

		params := &common.CreateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			Name:            "test_snapshot",
			IsAppConsistent: false,
			Description:     "test",
		}

		snapshot, jobUUID, err := orch.CreateSnapshot(ctx, params)
		assert.Error(tt, err, "Expected error when GetSnapshotsWithCondition fails")
		assert.Contains(tt, err.Error(), "database connection failed", "Expected specific error message")
		assert.Nil(tt, snapshot, "Expected nil snapshot response")
		assert.Empty(tt, jobUUID, "Expected empty job UUID")
	})
}

func TestConvertDatastoreSnapshotToModel(t *testing.T) {
	t.Run("WhenSnapshotIsNil", func(tt *testing.T) {
		result := ConvertDatastoreSnapshotToModel(nil)
		assert.Nil(tt, result, "Expected nil result when input snapshot is nil")
	})

	t.Run("WhenSnapshotHasAllFields", func(tt *testing.T) {
		input := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-snapshot-uuid",
				DeletedAt: nil,
			},
			Name:        "test_snapshot",
			Description: "test description",
			VolumeID:    123,
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{
					UUID: "test-volume-uuid",
				},
			},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					UUID: "test-account-uuid",
				},
				Name: "test_account",
			},
			State:        "READY",
			StateDetails: "Snapshot is ready",
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes: 1234,
			},
		}

		expected := &models.Snapshot{
			BaseModel: models.BaseModel{
				UUID: "test-snapshot-uuid",
			},
			Name:           "test_snapshot",
			Description:    "test description",
			VolumeUUID:     "test-volume-uuid",
			LifeCycleState: "READY",
			AccountName:    "test_account",
			StorageClass:   STORAGE_CLASS_SOFTWARE,
			SizeInBytes:    1234,
		}

		result := ConvertDatastoreSnapshotToModel(input)
		assert.NotNil(tt, result, "Expected non-nil result")
		assert.Equal(tt, expected.Name, result.Name, "Expected result to match the expected snapshot model")
		assert.Equal(tt, expected.SizeInBytes, result.SizeInBytes, "Expected result to match the expected snapshot model")
		assert.Equal(tt, expected.StorageClass, result.StorageClass, "Expected result to match the expected snapshot model")
	})
}

func TestVolumeOwnershipCheck(t *testing.T) {
	t.Run("WhenAccountIDIsIncorrect", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			assert.FailNow(tt, "Failed to create test storage: "+err.Error())
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: 1,
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "test-account-uuid"},
			Name:      "test_account",
		}

		_, err = VolumeOwnershipCheck(ctx, store, volume.UUID, account.Name)
		assert.ErrorContains(tt, err, "Volume not found. Please ensure the volume exists and belongs to your account.")
	})

	t.Run("WhenVolumeIsIncorrect", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			assert.FailNow(tt, "Failed to create test storage: "+err.Error())
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: 2,
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}

		_, err = VolumeOwnershipCheck(ctx, store, volume.UUID, account.Name)
		assert.ErrorContains(tt, err, "Volume not found. Please ensure the volume exists and belongs to your account.")
	})
}

func TestValidateSnapshotName(t *testing.T) {
	t.Run("WhenFailsWithEmptyString", func(tt *testing.T) {
		err := ValidateSnapshotName("")
		if err == nil {
			tt.Error("No error returned")
		} else if !errors.IsUserInputValidationErr(err) {
			tt.Error("Wrong error type returned")
		} else if err.Error() != "Snapshot name must not be empty." {
			tt.Errorf("Wrong error message returned, got: %s", err.Error())
		}
	})
	t.Run("WhenFailsWithInvalidCharacters", func(tt *testing.T) {
		invalid := []string{`'`, `""`, "?", "!", "#", "$", "%", "&", "/"}
		for _, c := range invalid {
			err := ValidateSnapshotName("invalid" + c)
			if err == nil {
				tt.Error("No error returned")
			} else if !errors.IsUserInputValidationErr(err) {
				tt.Error("Wrong error type returned")
			} else if err.Error() != "Snapshot name can only include alphanumeric characters and the following special characters: ()-_+." {
				tt.Errorf("Wrong error message returned, got: %s", err.Error())
			}
		}
	})
	t.Run("WhenFailsWithInvalidNames", func(tt *testing.T) {
		invalid := []string{`ref_ss_volmove`, `snapmirror`}
		for _, c := range invalid {
			err := ValidateSnapshotName(c)
			if err == nil {
				tt.Error("No error returned")
			}
			if !errors.IsUserInputValidationErr(err) {
				tt.Error("Wrong error type returned")
			}
		}
	})
	t.Run("WhenFailsWithExactInvalidNames", func(tt *testing.T) {
		invalid := []string{`ref_ss_volmove`, `snapmirror`, `hourly.`, `daily.`, `weekly.`, `monthly.`}
		for _, c := range invalid {
			err := ValidateSnapshotName(c)
			if err == nil {
				tt.Error("No error returned")
			}
			if !errors.IsUserInputValidationErr(err) {
				tt.Error("Wrong error type returned")
			}
			assert.EqualError(tt, err, `Snapshot name cannot start with the following: "ref_ss_volmove", "snapmirror", "hourly.", "daily.", "weekly." or "monthly.".`)
		}
	})
	t.Run("WhenFailsWithExtraInvalidNames", func(tt *testing.T) {
		invalid := []string{`ref_ss_volmove.something_more`, `snapmirror.snapshot`, `hourly.2024-05-01-1400`, `daily.2024-05-01-1400`, `weekly.2024-05-01-1400`, `monthly.2024-05-01-1400`}
		for _, c := range invalid {
			err := ValidateSnapshotName(c)
			if err == nil {
				tt.Error("No error returned")
			}
			if !errors.IsUserInputValidationErr(err) {
				tt.Error("Wrong error type returned")
			}
			assert.EqualError(tt, err, `Snapshot name cannot start with the following: "ref_ss_volmove", "snapmirror", "hourly.", "daily.", "weekly." or "monthly.".`)
		}
	})
	t.Run("WhenFailsWithConsecutiveDots", func(tt *testing.T) {
		err := ValidateSnapshotName("..")
		if err == nil {
			tt.Error("No error returned")
		} else if !errors.IsUserInputValidationErr(err) {
			tt.Error("Wrong error type returned")
		} else if err.Error() != "Snapshot name cannot include consecutive dots: .." {
			tt.Errorf("Wrong error message returned, got: %s", err.Error())
		}
	})
	t.Run("WhenFailsWithSingleDot", func(tt *testing.T) {
		err := ValidateSnapshotName(".")
		if err == nil {
			tt.Error("No error returned")
		} else if !errors.IsUserInputValidationErr(err) {
			tt.Error("Wrong error type returned")
		} else if err.Error() != "Snapshot name cannot be a single dot." {
			tt.Errorf("Wrong error message returned, got: %s", err.Error())
		}
	})
	t.Run("WhenSuccessfulWithAllValidCharacters", func(tt *testing.T) {
		err := ValidateSnapshotName("valid123_-+.()")
		if err != nil {
			tt.Error("Unexpected error returned")
		}
	})
}

func TestValidateCreateSnapshotOperation(t *testing.T) {
	t.Run("WhenParamsNameIsNil", func(tt *testing.T) {
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
		}
		params := &common.CreateSnapshotParams{}

		err := validateCreateSnapshotOperation(volume, params, nil)
		assert.ErrorContains(tt, err, "Snapshot name is empty")
	})

	t.Run("WhenVolumeStateIsCreating", func(tt *testing.T) {
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: 1,
			State:     models.LifeCycleStateCreating,
		}
		params := &common.CreateSnapshotParams{
			Name: "test_snapshot",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		}
		err := validateCreateSnapshotOperation(volume, params, account)
		assert.ErrorContains(tt, err, "volume is in creating stage.")
	})

	t.Run("WhenVolumeStateIsDeleting", func(tt *testing.T) {
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: 1,
			State:     models.LifeCycleStateDeleting,
		}
		params := &common.CreateSnapshotParams{
			Name: "test_snapshot",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		}
		err := validateCreateSnapshotOperation(volume, params, account)
		assert.ErrorContains(tt, err, "volume is in deleting stage.")
	})
}

func TestGetSnapshot(t *testing.T) {
	t.Run("WhenSnapshotDoesNotExist", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: 1,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		params := &common.GetSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "test-volume-uuid",
				AccountName: "test_account",
			},
			SnapshotUUID: "non-existent-uuid",
		}

		orch := Orchestrator{
			storage: store,
		}

		snapshot, err := orch.GetSnapshot(ctx, params)
		assertErrContainsOriginal(tt, err, "snapshot 'non-existent-uuid' not found")
		assert.Nil(tt, snapshot, "Expected nil snapshot")
	})

	t.Run("WhenSnapshotExists", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		snapshot := &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:         "test_snapshot",
			Description:  "Test snapshot description",
			AccountID:    account.ID,
			VolumeID:     volume.ID,
			Account:      account,
			Volume:       volume,
			State:        models.LifeCycleStateAvailable,
			StateDetails: "Available",
		}
		err = store.DB().Create(snapshot).Error
		assert.NoError(tt, err, "Failed to create snapshot")

		params := &common.GetSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "test-volume-uuid",
				AccountName: "test_account",
			},
			SnapshotUUID: "test-snapshot-uuid",
		}

		result, err := orch.GetSnapshot(ctx, params)
		assert.NoError(tt, err, "Failed to get snapshot")
		assert.Equal(tt, snapshot.Name, result.Name)
		assert.Equal(tt, volume.UUID, result.VolumeUUID)
		assert.Equal(tt, volume.Name, result.VolumeName)
	})

	t.Run("WhenSnapshotIsDeleted", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to clean up test storage")

		orch := Orchestrator{
			storage: store,
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.DB().Create(account).Error, "Failed to create account")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		assert.NoError(tt, store.DB().Create(volume).Error, "Failed to create volume")

		// Create a deleted snapshot
		deletedAt := &gorm.DeletedAt{Time: time.Now(), Valid: true}
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-snapshot-uuid",
				DeletedAt: deletedAt,
			},
			Name:        "test_snapshot",
			Description: "Test snapshot description",
			AccountID:   account.ID,
			VolumeID:    volume.ID,
			Account:     account,
			Volume:      volume,
		}
		assert.NoError(tt, store.DB().Create(snapshot).Error, "Failed to create snapshot")

		params := &common.GetSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "test-volume-uuid",
				AccountName: "test_account",
			},
			SnapshotUUID: "test-snapshot-uuid",
		}

		result, err := orch.GetSnapshot(ctx, params)
		assert.Nil(tt, result, "Expected nil snapshot")
		assert.Error(tt, err, "not found")
	})

	t.Run("WhenSnapshotGetFailsDueToVolumeOwnershipCheck", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		assert.NoError(tt, err, "Failed to create account")

		params := &common.GetSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "test-volume-uuid",
				AccountName: "test_account",
			},
			SnapshotUUID: "test-snapshot-uuid",
		}

		// Patch VolumeOwnershipCheck to return an error
		orig := VolumeOwnershipCheck
		VolumeOwnershipCheck = func(ctx context.Context, se database.Storage, volumeUUID, accountName string) (*datamodel.Volume, error) {
			return nil, errors.New("failed to validate volume ownership")
		}
		defer func() { VolumeOwnershipCheck = orig }()

		snapshot, err := orch.GetSnapshot(ctx, params)
		assert.Nil(tt, snapshot)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to validate volume ownership")
	})
}

func TestListSnapshots(t *testing.T) {
	t.Run("WhenOwnershipCheckFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err)
		orch := Orchestrator{storage: store}

		params := &common.ListSnapshotsParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "vol-uuid",
				AccountName: "acc",
			},
		}

		// Patch VolumeOwnershipCheck to return false
		orig := VolumeOwnershipCheck
		VolumeOwnershipCheck = func(ctx context.Context, se database.Storage, volumeUUID, accountName string) (*datamodel.Volume, error) {
			return nil, errors.New("failed to validate volume ownership")
		}
		defer func() { VolumeOwnershipCheck = orig }()

		snaps, err := orch.ListSnapshots(ctx, params)
		assert.Nil(tt, snaps)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to validate volume ownership")
	})

	t.Run("WhenVolumeNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err)
		orch := Orchestrator{storage: store}

		params := &common.ListSnapshotsParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "non-existent-vol",
				AccountName: "acc",
			},
		}

		// Patch VolumeOwnershipCheck to return true
		orig := VolumeOwnershipCheck
		VolumeOwnershipCheck = func(ctx context.Context, se database.Storage, volumeUUID, accountName string) (*datamodel.Volume, error) {
			return nil, errors.NewNotFoundErr("volume", nil)
		}
		defer func() { VolumeOwnershipCheck = orig }()

		snaps, err := orch.ListSnapshots(ctx, params)
		assert.Nil(tt, snaps)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume")
	})

	t.Run("WhenSnapshotsExist", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err)
		orch := Orchestrator{storage: store}

		// Setup account, volume, and snapshots
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "acc-uuid"},
			Name:      "acc",
		}
		assert.NoError(tt, store.DB().Create(account).Error)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			Name:      "vol",
			AccountID: account.ID,
		}
		assert.NoError(tt, store.DB().Create(volume).Error)
		snap1 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "snap-uuid-1"},
			Name:      "snap1",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Account:   account,
			Volume:    volume,
		}
		snap2 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "snap-uuid-2"},
			Name:      "snap2",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Account:   account,
			Volume:    volume,
		}
		assert.NoError(tt, store.DB().Create(snap1).Error)
		assert.NoError(tt, store.DB().Create(snap2).Error)

		params := &common.ListSnapshotsParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
		}

		// Patch VolumeOwnershipCheck to return true
		orig := VolumeOwnershipCheck
		VolumeOwnershipCheck = func(ctx context.Context, se database.Storage, volumeUUID, accountName string) (*datamodel.Volume, error) {
			return volume, nil
		}
		defer func() { VolumeOwnershipCheck = orig }()

		snaps, err := orch.ListSnapshots(ctx, params)
		assert.NoError(tt, err)
		assert.Len(tt, snaps, 2)
		names := []string{snaps[0].Name, snaps[1].Name}
		assert.Contains(tt, names, "snap1")
		assert.Contains(tt, names, "snap2")
	})
}

func TestOrchestrator_GetMultipleSnapshots(t *testing.T) {
	t.Run("WhenAccountNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err)
		orch := Orchestrator{storage: store}
		snapshots, err := orch.GetMultipleSnapshots(ctx, "vol-uuid", "non-existent-account", []string{"snap-uuid-1"})
		assert.NoError(tt, err)
		assert.NotNil(tt, snapshots)
		assert.Len(tt, snapshots, 0)
	})

	t.Run("WhenGetMultipleSnapshotsSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err)
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err)

		orch := Orchestrator{storage: store}

		// Create account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "acc-uuid"},
			Name:      "acc",
		}
		assert.NoError(tt, store.DB().Create(account).Error)

		// Create volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			Name:      "vol",
			AccountID: account.ID,
		}
		assert.NoError(tt, store.DB().Create(volume).Error)

		// Create snapshots
		snap1 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "snap-uuid-1"},
			Name:      "snap1",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Account:   account,
			Volume:    volume,
		}
		snap2 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "snap-uuid-2"},
			Name:      "snap2",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Account:   account,
			Volume:    volume,
		}
		assert.NoError(tt, store.DB().Create(snap1).Error)
		assert.NoError(tt, store.DB().Create(snap2).Error)

		// Patch getAccountWithName to return the created account
		orig := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = orig }()

		snapshots, err := orch.GetMultipleSnapshots(ctx, volume.UUID, account.Name, []string{"snap-uuid-1", "snap-uuid-2"})
		assert.NoError(tt, err)
		assert.NotNil(tt, snapshots)
		assert.Len(tt, snapshots, 2)
		names := []string{snapshots[0].Name, snapshots[1].Name}
		assert.Contains(tt, names, "snap1")
		assert.Contains(tt, names, "snap2")
	})

	t.Run("WhenSnapshotIsDeleted", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "acc-uuid"},
			Name:      "acc",
		}
		assert.NoError(tt, store.DB().Create(account).Error)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			Name:      "vol",
			AccountID: account.ID,
		}
		assert.NoError(tt, store.DB().Create(volume).Error)

		deletedAt := &gorm.DeletedAt{Time: time.Now(), Valid: true}
		snapshot := &datamodel.Snapshot{
			BaseModel:   datamodel.BaseModel{UUID: "test-snapshot-uuid", DeletedAt: deletedAt},
			Name:        "test_snapshot",
			Description: "desc",
			AccountID:   account.ID,
			VolumeID:    volume.ID,
			Account:     account,
			Volume:      volume,
			State:       models.LifeCycleStateREADY,
		}
		assert.NoError(tt, store.DB().Create(snapshot).Error)

		orch := Orchestrator{storage: store}
		params := &common.UpdateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "test-volume-uuid",
				AccountName: "test_account",
			},
			SnapshotUUID: "test-snapshot-uuid",
			Description:  "new_desc",
		}
		_, _, err = orch.UpdateSnapshot(ctx, params)
		assert.Error(tt, err, "not found")
	})

	t.Run("WhenSnapshotIsUpdateFailsDueToOwnershipCheck", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: 1,
		}
		snap1 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "snap-uuid-1"},
			Name:      "snap1",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Account:   account,
			Volume:    volume,
		}

		snap2 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "snap-uuid-2"},
			Name:      "snap2",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Account:   account,
			Volume:    volume,
		}
		assert.NoError(tt, store.DB().Create(snap1).Error)
		assert.NoError(tt, store.DB().Create(snap2).Error)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = _getAccountWithName }()

		orch := Orchestrator{storage: store}
		_, err = orch.GetMultipleSnapshots(ctx, volume.UUID, account.Name, []string{"snap-uuid-1", "snap-uuid-2"})
		assert.NoError(tt, err)

		params := &common.UpdateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "non-existent-vol-uuid",
				AccountName: "test_account",
			},
			SnapshotUUID: "snap-uuid-1",
			Description:  "updated_desc",
		}
		result, jobID, err := orch.UpdateSnapshot(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Volume not found")
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
	})

	t.Run("WhenUpdateSnapshotSuccessful", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: 1,
		}
		assert.NoError(tt, store.DB().Create(volume).Error)
		snap1 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "snap-uuid-1"},
			Name:      "snap1",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Account:   account,
			Volume:    volume,
		}

		snap2 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "snap-uuid-2"},
			Name:      "snap2",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Account:   account,
			Volume:    volume,
		}
		assert.NoError(tt, store.DB().Create(snap1).Error)
		assert.NoError(tt, store.DB().Create(snap2).Error)

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = _getAccountWithName }()

		orch := Orchestrator{storage: store}
		_, err = orch.GetMultipleSnapshots(ctx, volume.UUID, account.Name, []string{"snap-uuid-1", "snap-uuid-2"})
		assert.NoError(tt, err)

		params := &common.UpdateSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: "test_account",
			},
			SnapshotUUID: "snap-uuid-1",
			Description:  "updated_desc",
		}
		result, jobID, err := orch.UpdateSnapshot(ctx, params)
		assert.NoError(tt, err)
		assert.Empty(tt, jobID)
		assert.Equal(tt, result.Description, "updated_desc")
	})

	t.Run("WhenGetMultipleSnapshotsNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err)
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err)

		orch := Orchestrator{storage: store}

		// Create account and volume, but do NOT create any snapshots
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "acc-uuid"},
			Name:      "acc",
		}
		assert.NoError(tt, store.DB().Create(account).Error)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			Name:      "vol",
			AccountID: account.ID,
		}
		assert.NoError(tt, store.DB().Create(volume).Error)

		// Patch getAccountWithName to return the created account
		orig := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = orig }()

		// Try to fetch non-existent snapshots
		snapshots, err := orch.GetMultipleSnapshots(ctx, volume.UUID, account.Name, []string{"non-existent-uuid-1", "non-existent-uuid-2"})
		assert.NoError(tt, err)
		assert.NotNil(tt, snapshots)
		assert.Len(tt, snapshots, 0)
	})
}

func TestDeleteSnapshot(t *testing.T) {
	t.Run("WhenSnapshotDeletionSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool: &datamodel.Pool{
				VendorID: "/projects/project123/locations/location123/pools/pool123",
			},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}
		params := &common.DeleteSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			SnapshotID: "test-snapshot-uuid",
		}
		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()
		snapshotResp, _, err := orch.DeleteSnapshot(ctx, params)
		assert.NoError(tt, err, "Failed to delete snapshot")
		assert.NotNil(tt, snapshotResp, "Expected snapshot to be deleted")
		assert.Equal(tt, snapshotResp.Name, "test_snapshot")
		assert.Equal(tt, snapshotResp.VolumeUUID, "test-volume-uuid")
	})

	t.Run("WhenSnapshotDeletionFailsDueToVolumeNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}
		params := &common.DeleteSnapshotParams{
			SnapshotID: "test-snapshot-uuid",
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "volume.UUID",
				AccountName: account.Name,
			},
		}
		snapshot, _, err := orch.DeleteSnapshot(ctx, params)
		assert.Nil(tt, snapshot, "Expected nil snapshot")
		assertErrContainsOriginal(tt, err, "not found")
	})

	t.Run("WhenSnapshotDeletionFailsDueToAccountNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}
		params := &common.DeleteSnapshotParams{
			SnapshotID: "test-snapshot-uuid",
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: "account.Name",
			},
		}
		snapshot, _, err := orch.DeleteSnapshot(ctx, params)
		assert.Nil(tt, snapshot, "Expected nil snapshot")
		assertErrContainsOriginal(tt, err, "Volume not found. Please ensure the volume exists and belongs to your account.")
	})

	t.Run("WhenSnapshotDeletionFailsDueToWorkflowError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool: &datamodel.Pool{
				VendorID: "/projects/project123/locations/location123/pools/pool123",
			},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.DeleteSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			SnapshotID: snapshot.UUID,
		}

		// Mock SignalWithStartWorkflow to fail with a retryable error (connection refused is retryable)
		// WorkflowExecutor will retry up to 3 times for retryable errors (triggers line 496 error logging)
		// DeleteSnapshot uses ExecuteSequentialWorkflow which calls SignalWithStartWorkflow internally
		workflowErr := errors.New("connection refused")
		temporal.EXPECT().SignalWithStartWorkflow(
			mock.Anything, // ctx
			mock.Anything, // controlWorkflowID
			mock.Anything, // signal name
			mock.Anything, // SignalWorkflowParams
			mock.Anything, // StartWorkflowOptions
			mock.Anything, // workflow function
		).Return(nil, workflowErr).Times(3)

		_, _, err = orch.DeleteSnapshot(ctx, params)
		assert.Contains(tt, err.Error(), "connection refused")
	})

	t.Run("WhenSnapshotDeletionFailsAndSnapshotStateReverted", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool: &datamodel.Pool{
				VendorID: "/projects/project123/locations/location123/pools/pool123",
			},
			Account: account,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		// Create snapshot in READY state, it will be set to DELETING by DeletingSnapshot
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			State:     models.LifeCycleStateREADY,
			Account:   account,
			Volume:    volume,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.DeleteSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			SnapshotID: snapshot.UUID,
		}

		// Mock SignalWithStartWorkflow to fail with a retryable error (triggers line 496 error logging)
		// DeleteSnapshot uses ExecuteSequentialWorkflow which calls SignalWithStartWorkflow internally
		workflowErr := errors.New("connection refused")
		temporal.EXPECT().SignalWithStartWorkflow(
			mock.Anything, // ctx
			mock.Anything, // controlWorkflowID
			mock.Anything, // signal name
			mock.Anything, // SignalWorkflowParams
			mock.Anything, // StartWorkflowOptions
			mock.Anything, // workflow function
		).Return(nil, workflowErr).Times(3)

		_, _, err = orch.DeleteSnapshot(ctx, params)
		assert.Contains(tt, err.Error(), "connection refused")

		// Verify snapshot state was reverted to READY after workflow failure (lines 451-456)
		updatedSnapshot, _ := store.GetSnapshotByUUID(ctx, snapshot.UUID, account.ID, volume.ID)
		if updatedSnapshot != nil {
			assert.Equal(tt, models.LifeCycleStateREADY, updatedSnapshot.State)
			assert.Equal(tt, models.LifeCycleStateAvailableDetails, updatedSnapshot.StateDetails)
		}
	})

	t.Run("WhenAccountExistsButVolumeNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err)
		orch := Orchestrator{storage: store}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "acc-uuid"},
			Name:      "acc",
		}
		assert.NoError(tt, store.DB().Create(account).Error)

		// Patch getAccountWithName to return the account
		orig := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = orig }()

		// No volume created in DB
		snapshots, err := orch.GetMultipleSnapshots(ctx, "non-existent-vol-uuid", account.Name, []string{"snap-uuid-1"})
		assert.NoError(tt, err)
		assert.NotNil(tt, snapshots)
		assert.Len(tt, snapshots, 0)
	})

	t.Run("WhenSnapshotDeletionFailsDueToVolumeInDeletingState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			State:     models.LifeCycleStateDeleting, // Volume is in deleting state
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		params := &common.DeleteSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			SnapshotID: "test-snapshot-uuid",
		}

		snapshot, _, err := orch.DeleteSnapshot(ctx, params)
		assert.Nil(tt, snapshot, "Expected nil snapshot")
		assert.Error(tt, err, "Expected error when volume is in deleting state")
		assert.Contains(tt, err.Error(), "Volume of the snapshot is being deleted")
	})

	t.Run("WhenSnapshotDeletionFailsDueToBackupAdhocType", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool: &datamodel.Pool{
				VendorID: "/projects/project123/locations/location123/pools/pool123",
			},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Type:      "backup", // Set snapshot type to backup
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.DeleteSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			SnapshotID: "test-snapshot-uuid",
		}

		snapshotResp, _, err := orch.DeleteSnapshot(ctx, params)
		assert.Nil(tt, snapshotResp, "Expected nil snapshot")
		assert.Error(tt, err, "Expected error when snapshot is backup-adhoc type")
		assert.Contains(tt, err.Error(), "Cannot delete a snapshot that was generated for backups")
	})

	t.Run("WhenSnapshotDeletionReturnsOngoingJob", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool: &datamodel.Pool{
				VendorID: "/projects/project123/locations/location123/pools/pool123",
			},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Volume:    volume,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		// Create an ongoing delete job for the snapshot
		job := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "test-job-uuid"},
			Type:         string(models.JobTypeDeleteSnapshot),
			State:        string(models.JobsStatePROCESSING),
			ResourceName: "test_snapshot",
			AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: volume.UUID,
			},
		}
		err = store.DB().Create(job).Error
		assert.NoError(tt, err)

		params := &common.DeleteSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			SnapshotID: "test-snapshot-uuid",
		}

		snapshotResp, jobUUID, err := orch.DeleteSnapshot(ctx, params)
		assert.NoError(tt, err, "Failed to delete snapshot")
		assert.Equal(tt, "test-job-uuid", jobUUID, "Expected job UUID to be returned")
		assert.NotNil(tt, snapshotResp, "Expected snapshot to be returned")
		assert.Equal(tt, snapshotResp.Name, "test_snapshot")
		assert.Equal(tt, snapshotResp.VolumeUUID, "test-volume-uuid")
	})

	t.Run("WhenSnapshotDeletionWithIdempotencyCheck", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool: &datamodel.Pool{
				VendorID: "/projects/project123/locations/location123/pools/pool123",
			},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Volume:    volume,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.DeleteSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			SnapshotID: "test-snapshot-uuid",
		}

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

		// Test the idempotency behavior - this should work with real storage
		snapshotResp, jobUUID, err := orch.DeleteSnapshot(ctx, params)
		assert.NoError(tt, err, "Expected no error for successful delete")
		assert.NotEmpty(tt, jobUUID, "Expected job UUID to be returned")
		assert.NotNil(tt, snapshotResp, "Expected snapshot to be returned")
		assert.Equal(tt, snapshotResp.Name, "test_snapshot")
		assert.Equal(tt, snapshotResp.VolumeUUID, "test-volume-uuid")
	})

	t.Run("WhenSnapshotDeletionWithSameNameInDifferentVolumes", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		// Create two different volumes
		volume1 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-1-uuid"},
			Name:      "test_volume_1",
			AccountID: account.ID,
			Pool: &datamodel.Pool{
				VendorID: "/projects/project123/locations/location123/pools/pool123",
			},
		}
		err = store.DB().Create(volume1).Error
		if err != nil {
			tt.Fatalf("Failed to create volume1: %v", err)
		}

		volume2 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-2-uuid"},
			Name:      "test_volume_2",
			AccountID: account.ID,
			Pool: &datamodel.Pool{
				VendorID: "/projects/project123/locations/location123/pools/pool123",
			},
		}
		err = store.DB().Create(volume2).Error
		if err != nil {
			tt.Fatalf("Failed to create volume2: %v", err)
		}

		// Create snapshots with same name in both volumes
		snapshot1 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-1-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume1.ID,
			Volume:    volume1,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(snapshot1).Error
		assert.NoError(tt, err)

		snapshot2 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-2-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume2.ID,
			Volume:    volume2,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(snapshot2).Error
		assert.NoError(tt, err)

		// Create delete jobs for both snapshots
		job1 := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "test-delete-job-1-uuid"},
			Type:         string(models.JobTypeDeleteSnapshot),
			State:        string(models.JobsStatePROCESSING),
			ResourceName: "test_snapshot",
			AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: volume1.UUID,
			},
		}
		err = store.DB().Create(job1).Error
		assert.NoError(tt, err)

		job2 := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "test-delete-job-2-uuid"},
			Type:         string(models.JobTypeDeleteSnapshot),
			State:        string(models.JobsStatePROCESSING),
			ResourceName: "test_snapshot",
			AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: volume2.UUID,
			},
		}
		err = store.DB().Create(job2).Error
		assert.NoError(tt, err)

		// Test deleting snapshot from volume1 - should return job1
		params1 := &common.DeleteSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume1.UUID,
				AccountName: account.Name,
			},
			SnapshotID: "test-snapshot-1-uuid",
		}

		snapshotResp1, jobUUID1, err := orch.DeleteSnapshot(ctx, params1)
		assert.NoError(tt, err, "Failed to delete snapshot for volume1")
		assert.Equal(tt, "test-delete-job-1-uuid", jobUUID1, "Expected job1 UUID to be returned for volume1")
		assert.NotNil(tt, snapshotResp1, "Expected snapshot to be returned for volume1")
		assert.Equal(tt, snapshotResp1.Name, "test_snapshot")
		assert.Equal(tt, snapshotResp1.VolumeUUID, "test-volume-1-uuid")

		// Test deleting snapshot from volume2 - should return job2 (not job1)
		params2 := &common.DeleteSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume2.UUID,
				AccountName: account.Name,
			},
			SnapshotID: "test-snapshot-2-uuid",
		}

		snapshotResp2, jobUUID2, err := orch.DeleteSnapshot(ctx, params2)
		assert.NoError(tt, err, "Failed to delete snapshot for volume2")
		assert.Equal(tt, "test-delete-job-2-uuid", jobUUID2, "Expected job2 UUID to be returned for volume2")
		assert.NotNil(tt, snapshotResp2, "Expected snapshot to be returned for volume2")
		assert.Equal(tt, snapshotResp2.Name, "test_snapshot")
		assert.Equal(tt, snapshotResp2.VolumeUUID, "test-volume-2-uuid")

		// Verify that different job UUIDs are returned for different volumes
		assert.NotEqual(tt, jobUUID1, jobUUID2, "Expected different job UUIDs for different volumes")
	})

	t.Run("WhenSnapshotDeletionFailsDueToGetJobsWithConditionError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool: &datamodel.Pool{
				VendorID: "/projects/project123/locations/location123/pools/pool123",
			},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Volume:    volume,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		// Mock the storage to return an error when GetJobsWithCondition is called
		origStorage := orch.storage
		mockStorage := &MockStorage{
			Storage:                   origStorage,
			getJobsWithConditionError: errors.New("database connection failed"),
		}
		orch.storage = mockStorage
		defer func() { orch.storage = origStorage }()

		params := &common.DeleteSnapshotParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    volume.UUID,
				AccountName: account.Name,
			},
			SnapshotID: "test-snapshot-uuid",
		}

		snapshotResp, jobUUID, err := orch.DeleteSnapshot(ctx, params)
		assert.Error(tt, err, "Expected error when GetJobsWithCondition fails")
		assert.Contains(tt, err.Error(), "database connection failed", "Expected specific error message")
		assert.Nil(tt, snapshotResp, "Expected nil snapshot response")
		assert.Empty(tt, jobUUID, "Expected empty job UUID")
	})
}

func TestDeleteSnapshots(t *testing.T) {
	t.Run("WhenSnapshotDeletionSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "snapmirror.1",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Volume:    volume,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}
		params := &common.SnapshotsInternalDeleteParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "test-volume-uuid",
				AccountName: account.Name,
			},
			SnapshotsFromDB: []*datamodel.Snapshot{snapshot},
		}
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		jobId, err := orch.DeleteSnapmirrorSnapshots(ctx, params)
		assert.NoError(tt, err, "Failed to delete snapshot")
		assert.NotEmpty(tt, jobId)
	})
	t.Run("WhenSnapshotDeletionFailsDueToVolumeNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}
		params := &common.SnapshotsInternalDeleteParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "volume.UUID",
				AccountName: account.Name,
			},
		}
		_, err = orch.DeleteSnapmirrorSnapshots(ctx, params)
		assert.ErrorContains(tt, err, "Volume not found")
	})
	t.Run("WhenSnapshotDeletionFailsDueToAccountNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		params := &common.SnapshotsInternalDeleteParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "volume.UUID",
				AccountName: account.Name,
			},
		}
		_, err = orch.DeleteSnapmirrorSnapshots(ctx, params)
		assert.ErrorContains(tt, err, "Volume not found")
	})
	t.Run("WhenSnapshotDeletionFailsDueToWorkflowError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(tt)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "snapmirror.1",
			AccountID: account.ID,
			VolumeID:  volume.ID,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.SnapshotsInternalDeleteParams{
			//	SnapshotID: "test-snapshot-uuid",
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "test-volume-uuid",
				AccountName: account.Name,
			},
		}

		// Mock ExecuteWorkflow to fail with a retryable error (connection refused is retryable)
		// WorkflowExecutor will retry up to 3 times for retryable errors
		workflowErr := errors.New("connection refused")
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, workflowErr).Times(3)

		_, err = orch.DeleteSnapmirrorSnapshots(ctx, params)
		assert.Contains(tt, err.Error(), "connection refused")
	})

	t.Run("WhenSnapshotDeletionFailsDueToWrongState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			State:     models.LifeCycleStateDeleting,
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}
		// Create a snapshot in a non-deletable state (e.g., CREATING)
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			State:     models.LifeCycleStateCreating,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}
		params := &common.SnapshotsInternalDeleteParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "test-volume-uuid",
				AccountName: account.Name,
			},
		}
		_, err = orch.DeleteSnapmirrorSnapshots(ctx, params)
		assert.ErrorContains(tt, err, "Volume of the snapshot is being deleted.")
	})
	t.Run("WhenSnapshotDeletionFailsDueToVolumeInRetainedState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			State:     models.LifeCycleStateRetained,
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}
		// Create a snapshot in a non-deletable state (e.g., CREATING)
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			State:     models.LifeCycleStateCreating,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}
		params := &common.SnapshotsInternalDeleteParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "test-volume-uuid",
				AccountName: account.Name,
			},
		}
		_, err = orch.DeleteSnapmirrorSnapshots(ctx, params)
		assert.ErrorContains(tt, err, "Volume not found")
	})
	t.Run("WhenSnapshotDeletionFailsDueToVolumeInOfflineState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			State:     models.VolumeStateOffline,
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}
		// Create a snapshot in a non-deletable state (e.g., CREATING)
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			State:     models.LifeCycleStateCreating,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}
		params := &common.SnapshotsInternalDeleteParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "test-volume-uuid",
				AccountName: account.Name,
			},
		}
		_, err = orch.DeleteSnapmirrorSnapshots(ctx, params)
		assert.ErrorContains(tt, err, "Volume is offline.")
	})
	t.Run("WhenSnapshotDeletionFailsDueToVolumeInRetainedState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
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
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			State:     models.LifeCycleStateRetained,
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}
		// Create a snapshot in a non-deletable state (e.g., CREATING)
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			State:     models.LifeCycleStateCreating,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}
		params := &common.SnapshotsInternalDeleteParams{
			SnapshotBaseParams: common.SnapshotBaseParams{
				VolumeID:    "test-volume-uuid",
				AccountName: account.Name,
			},
		}
		_, err = orch.DeleteSnapmirrorSnapshots(ctx, params)
		assert.ErrorContains(tt, err, "Volume not found")
	})
}
