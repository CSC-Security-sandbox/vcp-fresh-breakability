package orchestrator

import (
	"context"
	"gorm.io/gorm"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestOrchestrator_CreateSnapshot(t *testing.T) {
	t.Run("WhenSnapshotCreationSuccess", func(tt *testing.T) {
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

	t.Run("WhenSnapshotCreationFailsDueToVolumeNotFound", func(tt *testing.T) {
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
		assert.ErrorContains(tt, err, "failed to validate volume ownership")
	})

	t.Run("WhenSnapshotCreationFailsDueToAccountNotFound", func(tt *testing.T) {
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
		result := convertDatastoreSnapshotToModel(nil)
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

		result := convertDatastoreSnapshotToModel(input)
		assert.NotNil(tt, result, "Expected non-nil result")
		assert.Equal(tt, expected.Name, result.Name, "Expected result to match the expected snapshot model")
	})
}

func TestVolumeOwnershipCheck(t *testing.T) {
	t.Run("WhenAccountIDIsIncorrect", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
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
		assert.ErrorContains(tt, err, "not found")
	})

	t.Run("WhenVolumeIsIncorrect", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
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
		assert.ErrorContains(tt, err, "not found")
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
		store, err := database.NewTestStorage(mockLogger)
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
		assert.EqualError(tt, err, "snapshot 'non-existent-uuid' not found")
		assert.Nil(tt, snapshot, "Expected nil snapshot")
	})

	t.Run("WhenSnapshotExists", func(tt *testing.T) {
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
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

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
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

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
		assert.EqualError(tt, err, "snapshot 'test-snapshot-uuid' not found")
		assert.Nil(tt, result, "Expected nil snapshot")
	})
}

func TestListSnapshots(t *testing.T) {
	t.Run("WhenOwnershipCheckFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
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
		store, err := database.NewTestStorage(mockLogger)
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
		store, err := database.NewTestStorage(mockLogger)
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

func TestDeleteSnapshot(t *testing.T) {
	t.Run("WhenSnapshotDeletionSuccess", func(tt *testing.T) {
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
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		snapshotResp, _, err := orch.DeleteSnapshot(ctx, params)
		assert.NoError(tt, err, "Failed to delete snapshot")
		assert.NotNil(tt, snapshotResp, "Expected snapshot to be deleted")
		assert.Equal(tt, snapshotResp.Name, "test_snapshot")
		assert.Equal(tt, snapshotResp.VolumeUUID, "test-volume-uuid")
	})
	t.Run("WhenSnapshotDeletionFailsDueToVolumeNotFound", func(tt *testing.T) {
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
		assert.ErrorContains(tt, err, "failed to validate volume ownership")
	})
	t.Run("WhenSnapshotDeletionFailsDueToAccountNotFound", func(tt *testing.T) {
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
			SnapshotID: "test-snapshot-uuid",
			SnapshotBaseParams: common.SnapshotBaseParams{
				AccountName: account.Name,
				VolumeID:    volume.UUID,
			},
		}

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow error")).Once()

		snapshotRes, _, err := orch.DeleteSnapshot(ctx, params)
		assert.Nil(tt, snapshotRes, "Expected nil snapshot")
		assert.EqualError(tt, err, "workflow error")
	})
}
