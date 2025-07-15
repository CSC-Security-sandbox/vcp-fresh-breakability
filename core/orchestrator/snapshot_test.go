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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"gorm.io/gorm"
)

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

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow error")).Once()

		snapshot, _, err := orch.CreateSnapshot(ctx, params)
		assert.Nil(tt, snapshot, "Expected nil snapshot")
		assert.EqualError(tt, err, "workflow error")
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
		}

		result := ConvertDatastoreSnapshotToModel(input)
		assert.NotNil(tt, result, "Expected non-nil result")
		assert.Equal(tt, expected.Name, result.Name, "Expected result to match the expected snapshot model")
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
		assert.ErrorContains(tt, err, "failed to validate volume ownership")
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
		assert.ErrorContains(tt, err, "failed to validate volume ownership")
	})
}

func TestValidateCreateSnapshotOperation(t *testing.T) {
	t.Run("WhenParamsNameIsNil", func(tt *testing.T) {
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
		}
		params := &common.CreateSnapshotParams{}

		err := validateCreatSnapshotOperation(volume, params, nil)
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
		err := validateCreatSnapshotOperation(volume, params, account)
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
		err := validateCreatSnapshotOperation(volume, params, account)
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
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, err, "[0] undefined error: snapshot 'non-existent-uuid' not found")
		}
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
				VolumeID:    "test-volume-uuid",
				AccountName: "test_account",
			},
			SnapshotUUID: "test-snapshot-uuid",
			Description:  "updated_desc",
		}
		result, jobID, err := orch.UpdateSnapshot(ctx, params)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, err, "[0] undefined error: volume 'test-volume-uuid' not found")
		}
		assert.Nil(tt, result)
		assert.Empty(tt, jobID)
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
		assert.ErrorContains(tt, err, "not found")
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
		assert.ErrorContains(tt, err, "failed to validate volume ownership")
	})

	t.Run("WhenSnapshotDeletionFailsDueToWorkflowError", func(tt *testing.T) {
		mockLogger := log.NewLogger()
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
		assert.ErrorContains(tt, err, "volume.UUID' not found")
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
		assert.ErrorContains(tt, err, "volume.UUID' not found")
	})
	t.Run("WhenSnapshotDeletionFailsDueToWorkflowError", func(tt *testing.T) {
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

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow error")).Once()

		_, err = orch.DeleteSnapmirrorSnapshots(ctx, params)
		assert.EqualError(tt, err, "workflow error")
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
