package database

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

func TestCreatingSnapshot(t *testing.T) {
	logger := log.NewLogger()
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, logger)

	t.Run("WhenSnapshotIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err, "Failed to clean up test database")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
		}
		err = store.db.Create(volume).Error()
		assert.NoError(t, err, "Failed to create volume")

		snapshot := &datamodel.Snapshot{
			Name:     "test_snapshot",
			VolumeID: volume.ID,
		}
		createdSnapshot, err := store.CreatingSnapshot(ctx, snapshot)
		assert.NoError(t, err, "Expected no error, got %v", err)
		assert.Equal(t, snapshot.Name, createdSnapshot.Name, "Expected snapshot name %v, got %v", snapshot.Name, createdSnapshot.Name)
		assert.Equal(t, models.LifeCycleStateCreating, createdSnapshot.State, "Expected snapshot state %v, got %v", models.LifeCycleStateCreating, createdSnapshot.State)
	})

	t.Run("WhenSnapshotCreationFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err, "Failed to clean up test database")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
		}
		err = store.db.Create(volume).Error()
		if err != nil {
			t.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot1",
			VolumeID:  volume.ID,
		}
		err = store.db.Create(snapshot).Error()
		if err != nil {
			t.Fatalf("Failed to create snapshot: %v", err)
		}

		_, err = store.CreatingSnapshot(context.Background(), snapshot)
		assert.ErrorContains(t, err, "Snapshot already exists", "Expected error 'Snapshot already exists', got %v", err)
	})
}

func TestUpdateSnapshot(t *testing.T) {
	t.Run("WhenUpdateIsSuccessful", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
		}
		err = store.db.Create(volume).Error()
		assert.Equal(t, err, nil)

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			VolumeID:  volume.ID,
			State:     models.LifeCycleStateCreating,
		}
		err = store.db.Create(snapshot).Error()
		if err != nil {
			t.Fatalf("Failed to create snapshot: %v", err)
		}

		updatedSnapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			State:     models.LifeCycleStateAvailable,
		}

		dbSnapshot, err := store.UpdateSnapshot(context.Background(), updatedSnapshot)
		assert.Equal(t, err, nil)

		result := &datamodel.Snapshot{}
		err = store.db.GORM().First(result, "uuid = ?", updatedSnapshot.UUID).Error
		assert.Equal(t, err, nil)
		assert.Equal(t, updatedSnapshot.State, result.State, "Expected state %v, got %v", updatedSnapshot.State, result.State)
		assert.Equal(t, updatedSnapshot.State, dbSnapshot.State, "Expected state %v, got %v", updatedSnapshot.State, dbSnapshot.State)
	})

	t.Run("WhenSnapshotDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		updatedSnapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"},
			State:     models.LifeCycleStateAvailable,
		}

		dbSnapshot, err := store.UpdateSnapshot(context.Background(), updatedSnapshot)
		assert.ErrorContains(t, err, "not found", "Expected error 'not found', got %v", err)
		assert.Nil(tt, dbSnapshot)
	})
}

func TestGetAppConsistentSnapshotsForVolume(t *testing.T) {
	t.Run("WhenAppConsistentSnapshotExistForVolume", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: 1,
		}
		err = store.db.Create(volume).Error()
		assert.Equal(t, err, nil)

		snapshot := &datamodel.Snapshot{
			BaseModel:       datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			VolumeID:        volume.ID,
			State:           models.LifeCycleStateAvailable,
			IsAppConsistent: true,
			AccountID:       1,
		}
		err = store.db.Create(snapshot).Error()
		if err != nil {
			t.Fatalf("Failed to create snapshot: %v", err)
		}
		forVolume, err := store.GetAppConsistentSnapshotsForVolume(context.Background(), 1, volume.ID)
		if err != nil {
			t.Fatalf("Failed to get volume: %v", err)
		}
		assert.Equal(tt, 1, len(forVolume))
	})

	t.Run("WhenAppConsistentSnapshotDoesnotExistForVolume", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: 1,
		}
		err = store.db.Create(volume).Error()
		assert.Equal(t, err, nil)

		snapshot := &datamodel.Snapshot{
			BaseModel:       datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			VolumeID:        volume.ID,
			State:           models.LifeCycleStateAvailable,
			IsAppConsistent: false,
			AccountID:       1,
		}
		err = store.db.Create(snapshot).Error()
		if err != nil {
			t.Fatalf("Failed to create snapshot: %v", err)
		}
		forVolume, err := store.GetAppConsistentSnapshotsForVolume(context.Background(), 1, volume.ID)
		if err != nil {
			t.Fatalf("Failed to get volume: %v", err)
		}
		assert.Equal(tt, 0, len(forVolume))
	})
}

func TestGetSnapshot(t *testing.T) {
	logger := log.NewLogger()
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, logger)

	t.Run("WhenSnapshotExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create a volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-uuid",
				ID:   1,
			},
			Name:      "test_volume",
			AccountID: 1,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Create a snapshot
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			VolumeID:  volume.ID,
			State:     models.LifeCycleStateAvailable,
			AccountID: 1,
		}
		err = store.db.Create(snapshot).Error()
		assert.NoError(tt, err, "Failed to create snapshot")

		// Get the snapshot
		retrievedSnapshot, err := store.GetSnapshotByUUID(ctx, snapshot.UUID, 1, volume.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, retrievedSnapshot, "Expected snapshot to be retrieved")
		assert.Equal(tt, snapshot.UUID, retrievedSnapshot.UUID, "Expected UUID %v, got %v", snapshot.UUID, retrievedSnapshot.UUID)
		assert.Equal(tt, snapshot.Name, retrievedSnapshot.Name, "Expected name %v, got %v", snapshot.Name, retrievedSnapshot.Name)
		assert.Equal(tt, snapshot.State, retrievedSnapshot.State, "Expected state %v, got %v", snapshot.State, retrievedSnapshot.State)
		assert.Equal(tt, snapshot.VolumeID, retrievedSnapshot.VolumeID, "Expected VolumeID %v, got %v", snapshot.VolumeID, retrievedSnapshot.VolumeID)
		assert.Equal(tt, volume.Name, retrievedSnapshot.Volume.Name, "Expected VolumeName %v, got %v", volume.Name, retrievedSnapshot.Volume.Name)
		assert.Equal(tt, account.Name, retrievedSnapshot.Account.Name, "Expected AccountName %v, got %v", account.Name, retrievedSnapshot.Account.Name)
	})

	t.Run("WhenSnapshotDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to get a non-existent snapshot
		nonExistentUUID := "non-existent-uuid"
		_, err = store.GetSnapshotByUUID(ctx, nonExistentUUID, 1, 1)
		assert.Error(tt, err, "Expected error when snapshot does not exist")
		assert.ErrorContains(tt, err, "not found", "Expected error 'not found', got %v", err)
	})
}

func TestGetSnapshotByPoolID(t *testing.T) {
	logger := log.NewLogger()
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, logger)

	t.Run("WhenSnapshotExistsForPoolID", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create a pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create a volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 1},
			Name:      "test_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Create a snapshot
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			VolumeID:  volume.ID,
			AccountID: account.ID,
		}
		err = store.db.Create(snapshot).Error()
		assert.NoError(tt, err, "Failed to create snapshot")

		// Get the snapshot by pool ID
		retrievedSnapshot, err := store.GetSnapshotByPoolID(ctx, snapshot.UUID, account.ID, pool.ID, false)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, retrievedSnapshot, "Expected snapshot to be retrieved")
		assert.Equal(tt, snapshot.UUID, retrievedSnapshot.UUID, "Expected UUID %v, got %v", snapshot.UUID, retrievedSnapshot.UUID)
		assert.Equal(tt, snapshot.Name, retrievedSnapshot.Name, "Expected name %v, got %v", snapshot.Name, retrievedSnapshot.Name)
		assert.Equal(tt, snapshot.VolumeID, retrievedSnapshot.VolumeID, "Expected VolumeID %v, got %v", snapshot.VolumeID, retrievedSnapshot.VolumeID)
		assert.Equal(tt, volume.PoolID, retrievedSnapshot.Volume.PoolID, "Expected PoolID %v, got %v", volume.PoolID, retrievedSnapshot.Volume.PoolID)
	})

	t.Run("WhenSnapshotDoesNotExistForPoolID", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create a pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create a volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 1},
			Name:      "test_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Create a snapshot
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			VolumeID:  volume.ID,
			AccountID: account.ID,
		}
		err = store.db.Create(snapshot).Error()
		assert.NoError(tt, err, "Failed to create snapshot")

		// Try to get the snapshot with a non-matching pool ID
		retrievedSnapshot, err := store.GetSnapshotByPoolID(ctx, snapshot.UUID, account.ID, 999, false)
		assert.Error(tt, err, "Expected error when pool ID does not match")
		assert.Nil(tt, retrievedSnapshot, "Expected nil snapshot")
		assert.ErrorContains(tt, err, "Restore snapshots across pool is not supported", "Expected error 'snapshot not found for the given pool ID', got %v", err)
	})

	t.Run("WhenSnapshotDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to get a non-existent snapshot
		nonExistentUUID := "non-existent-uuid"
		retrievedSnapshot, err := store.GetSnapshotByPoolID(ctx, nonExistentUUID, 1, 1, false)
		assert.Error(tt, err, "Expected error when snapshot does not exist")
		assert.Nil(tt, retrievedSnapshot, "Expected nil snapshot")
		assert.ErrorContains(tt, err, "snapshot 'non-existent-uuid' not found", "Expected error 'snapshot 'non-existent-uuid' not found', got %v", err)
	})
}

func TestDeleteSnapshot(t *testing.T) {
	t.Run("WhenSnapshotIsDeletedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			Name:    "test_pool",
			Account: account,
		}

		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.db.Create(volume).Error()
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			VolumeID:  volume.ID,
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(snapshot).Error()
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		deletedSnapshot, err := store.DeleteSnapshot(context.Background(), snapshot.UUID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, snapshot.Name, deletedSnapshot.Name, "Expected snapshot name %v, got %v", snapshot.Name, deletedSnapshot.Name)
		assert.NotNil(tt, deletedSnapshot.DeletedAt, "Expected snapshot to be deleted, got %v", deletedSnapshot.DeletedAt)
		assert.Equal(tt, models.LifeCycleStateDeleted, deletedSnapshot.State, "Expected snapshot state %v, got %v", models.LifeCycleStateDeleted, deletedSnapshot.State)
		assert.Equal(tt, models.LifeCycleStateDeletedDetails, deletedSnapshot.StateDetails, "Expected snapshot state details %v, got %v", "", deletedSnapshot.StateDetails)

		_, err = store.GetSnapshotByUUID(context.Background(), snapshot.UUID, account.ID, volume.ID)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
	t.Run("WhenSnapshotIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		deletedSnapshot, err := store.DeleteSnapshot(context.Background(), "dummy")
		assert.Nil(tt, deletedSnapshot, "Expected nil snapshot, got %v", deletedSnapshot)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
	t.Run("ReturnsErrorWhenSnapshotIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Mock startTransaction to return an error
		origStartTransaction := startTransaction
		startTransaction = func(db *gorm.DB) (*gorm.DB, error) {
			return nil, errors.New("failed to start transaction")
		}
		defer func() { startTransaction = origStartTransaction }()

		deletedSnapshot, err := store.DeleteSnapshot(context.Background(), "non-existent-uuid")
		assert.Nil(tt, deletedSnapshot, "Expected nil snapshot, got %v", deletedSnapshot)
		assert.Error(tt, err, "Expected error when snapshot does not exist")
		assert.ErrorContains(tt, err, "failed to start transaction")
	})
}

func TestDeletingSnapshot(t *testing.T) {
	t.Run("ReturnsErrorWhenSnapshotDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		snapshot := &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"}}

		err = store.DeletingSnapshot(context.Background(), snapshot)
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		assert.Error(tt, err)
	})

	t.Run("UpdatesSnapshotStateToDeletingSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:         "test_snapshot",
			VolumeID:     1,
			AccountID:    account.ID,
			Account:      account,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailable,
		}
		err = store.db.Create(snapshot).Error()
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		err = store.DeletingSnapshot(context.Background(), snapshot)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		updatedSnapshot := &datamodel.Snapshot{}
		err = store.db.GORM().First(updatedSnapshot, "uuid = ?", snapshot.UUID).Error
		if err != nil {
			tt.Fatalf("Failed to fetch updated pool: %v", err)
		}
		if updatedSnapshot.State != models.LifeCycleStateDeleting {
			tt.Errorf("Expected state %v, got %v", models.LifeCycleStateDeleting, updatedSnapshot.State)
		}
		if updatedSnapshot.StateDetails != models.LifeCycleStateDeletingDetails {
			tt.Errorf("Expected state details %v, got %v", models.LifeCycleStateDeletingDetails, updatedSnapshot.StateDetails)
		}
	})
}

func TestBatchDeleteSnapshots(t *testing.T) {
	t.Run("BatchDeleteSnapshotsSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		snapshot1 := &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{UUID: "test-snapshot-uuid-1"},
			Name:         "test_snapshot-1",
			VolumeID:     1,
			AccountID:    account.ID,
			Account:      account,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailable,
		}
		snapshot2 := &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{UUID: "test-snapshot-uuid-2"},
			Name:         "test_snapshot-2",
			VolumeID:     1,
			AccountID:    account.ID,
			Account:      account,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailable,
		}

		err = store.db.Create(snapshot1).Error()
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}
		err = store.db.Create(snapshot2).Error()
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		deletedSnapshots, err := store.BatchDeleteSnapshots(context.Background(), []int64{snapshot1.ID, snapshot2.ID})
		if err != nil {
			tt.Fatalf("Failed to batch delete snapshots: %v", err)
		}
		assert.Len(tt, deletedSnapshots, 2)

		updatedSnapshot1 := &datamodel.Snapshot{}
		err = store.db.GORM().Unscoped().First(updatedSnapshot1, "uuid = ?", snapshot1.UUID).Error
		if err != nil {
			tt.Fatalf("Failed to fetch updated snapshot1: %v", err)
		}

		updatedSnapshot2 := &datamodel.Snapshot{}
		err = store.db.GORM().Unscoped().First(updatedSnapshot2, "uuid = ?", snapshot2.UUID).Error
		if err != nil {
			tt.Fatalf("Failed to fetch updated snapshot2: %v", err)
		}
		assert.NotNil(tt, updatedSnapshot1.DeletedAt, "Expected DeletedAt to be not nil")
		assert.NotNil(tt, updatedSnapshot2.DeletedAt, "Expected DeletedAt to be not nil")

		assert.Equal(tt, models.LifeCycleStateDeletedDetails, updatedSnapshot1.StateDetails)
		assert.Equal(tt, models.LifeCycleStateDeletedDetails, updatedSnapshot2.StateDetails)

		assert.Equal(tt, models.LifeCycleStateDeleted, updatedSnapshot1.State)
		assert.Equal(tt, models.LifeCycleStateDeleted, updatedSnapshot2.State)
	})
}

func TestGetSnapshotsByVolumeID(t *testing.T) {
	t.Run("WhenSnapshotsExistForVolume", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create a volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Create snapshots for the volume
		snapshot1 := &datamodel.Snapshot{
			Name:     "snap1",
			VolumeID: volume.ID,
		}
		snapshot2 := &datamodel.Snapshot{
			Name:     "snap2",
			VolumeID: volume.ID,
		}
		_, err = store.CreatingSnapshot(context.Background(), snapshot1)
		assert.NoError(tt, err, "Failed to create snapshot1")
		_, err = store.CreatingSnapshot(context.Background(), snapshot2)
		assert.NoError(tt, err, "Failed to create snapshot2")

		// Get snapshots by volume ID
		snapshots, err := store.GetSnapshotsByVolumeID(context.Background(), volume.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, snapshots, "Expected non-nil snapshots")
		assert.GreaterOrEqual(tt, len(snapshots), 2, "Expected at least 2 snapshots")
		var foundSnap1, foundSnap2 bool
		for _, s := range snapshots {
			if s.Name == "snap1" {
				foundSnap1 = true
			}
			if s.Name == "snap2" {
				foundSnap2 = true
			}
		}
		assert.True(tt, foundSnap1, "Expected to find snap1")
		assert.True(tt, foundSnap2, "Expected to find snap2")
	})

	t.Run("WhenNoSnapshotsExistForVolume", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create a volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Get snapshots by volume ID (should be none)
		snapshots, err := store.GetSnapshotsByVolumeID(context.Background(), volume.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, snapshots, "Expected non-nil snapshots slice")
		assert.Equal(tt, 0, len(snapshots), "Expected 0 snapshots")
	})
}

func TestGetSnapshotsByVolumeIDs(t *testing.T) {
	t.Run("GetSnapshotsByVolumeIDsSuccessful", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		snapshot1 := &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{UUID: "test-snapshot-uuid-1"},
			Name:         "test_snapshot-1",
			VolumeID:     1,
			AccountID:    account.ID,
			Account:      account,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailable,
		}
		snapshot2 := &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{UUID: "test-snapshot-uuid-2"},
			Name:         "test_snapshot-2",
			VolumeID:     1,
			AccountID:    account.ID,
			Account:      account,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailable,
		}

		err = store.db.Create(snapshot1).Error()
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}
		err = store.db.Create(snapshot2).Error()
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		snapshots, err := store.GetSnapshotsByVolumeIDs(context.Background(), []int64{snapshot1.ID, snapshot2.ID, 3})
		assert.NoError(tt, err)
		assert.Len(tt, snapshots, 2)
	})
}

func TestGetReplicationSnapshotsByVolumeID(t *testing.T) {
	t.Run("WhenSnapshotsExistForVolume", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create a volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Create snapshots for the volume
		snapshot1 := &datamodel.Snapshot{
			Name:     "snap1",
			VolumeID: volume.ID,
			BaseModel: datamodel.BaseModel{
				UUID: "snapmirror-snapshot-uuid5",
				//	DeletedAt: nil,
			},
		}
		snapshot2 := &datamodel.Snapshot{
			Name:     "snapmirror.snap2",
			VolumeID: volume.ID,
			BaseModel: datamodel.BaseModel{
				UUID: "snapmirror-snapshot-uuid",
				DeletedAt: &gorm.DeletedAt{
					Time: time.Now(),
				},
			},
		}

		_, err = store.CreatingSnapshot(context.Background(), snapshot1)
		assert.NoError(tt, err, "Failed to create snapshot1")
		_, err = store.CreatingSnapshot(context.Background(), snapshot2)
		assert.NoError(tt, err, "Failed to create snapshot2")

		// Get snapshots by volume ID
		snapshots, err := store.GetReplicationSnapshotsByVolumeID(context.Background(), volume.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, snapshots, "Expected non-nil snapshots")
		assert.Equal(tt, len(snapshots), 1)
	})
	t.Run("WhenNoSnapshotsExistForVolume", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create a volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Get snapshots by volume ID (should be none)
		snapshots, err := store.GetReplicationSnapshotsByVolumeID(context.Background(), volume.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, snapshots, "Expected non-nil snapshots slice")
		assert.Equal(tt, 0, len(snapshots), "Expected 0 snapshots")
	})
}

func TestGetSnapshotsWithConditions(t *testing.T) {
	logger := log.NewLogger()
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, logger)

	t.Run("WhenSnapshotExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create a volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: 1,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Create a snapshot
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			VolumeID:  volume.ID,
			State:     models.LifeCycleStateAvailable,
			AccountID: 1,
		}
		err = store.db.Create(snapshot).Error()
		assert.NoError(tt, err, "Failed to create snapshot")

		// Query for the snapshot using conditions
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", 1),
			utils.NewFilterCondition("volume_id", "=", volume.ID),
			utils.NewFilterCondition("name", "=", "test_snapshot"),
		)
		snapshots, err := store.GetSnapshotsWithCondition(ctx, *filter)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, snapshots, 1, "Expected 1 snapshot, got %d", len(snapshots))
		assert.Equal(tt, snapshot.UUID, snapshots[0].UUID, "Expected UUID %v, got %v", snapshot.UUID, snapshots[0].UUID)
		assert.Equal(tt, snapshot.Name, snapshots[0].Name, "Expected name %v, got %v", snapshot.Name, snapshots[0].Name)
		assert.Equal(tt, snapshot.State, snapshots[0].State, "Expected state %v, got %v", snapshot.State, snapshots[0].State)
		assert.Equal(tt, volume.Name, snapshots[0].Volume.Name, "Expected VolumeName %v, got %v", volume.Name, snapshots[0].Volume.Name)
	})

	t.Run("WhenSnapshotDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Query for a snapshot that does not exist
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", 999),
			utils.NewFilterCondition("volume_id", "=", 999),
			utils.NewFilterCondition("name", "=", "non-existent-snapshot"),
		)
		snapshots, err := store.GetSnapshotsWithCondition(ctx, *filter)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, snapshots, 0, "Expected 0 snapshots, got %d", len(snapshots))
	})
	t.Run("WhenFilterConditionsAreInvalid", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err, "Failed to clean up test database")

		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account", "=", 999),
		)

		_, err = store.GetSnapshotsWithCondition(context.Background(), *filter)
		assert.Error(t, err)
	})
}

func TestUnDeleteSnapshot(t *testing.T) {
	t.Run("RestoresDeletedSnapshotSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		snapshot := &datamodel.Snapshot{
			BaseModel:    datamodel.BaseModel{UUID: "test-snapshot-uuid", DeletedAt: &gorm.DeletedAt{}},
			VolumeID:     volume.ID,
			State:        models.LifeCycleStateDeleted,
			StateDetails: models.LifeCycleStateDeletedDetails,
		}
		err = store.db.Create(snapshot).Error()
		assert.NoError(tt, err, "Failed to create snapshot")

		err = store.UnDeleteSnapshot(context.Background(), snapshot)
		assert.NoError(tt, err)

		updated := &datamodel.Snapshot{}
		err = store.db.GORM().First(updated, "uuid = ?", snapshot.UUID).Error
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateREADY, updated.State)
		assert.Equal(tt, models.LifeCycleStateReadyDetails, updated.StateDetails)
		assert.True(tt, updated.DeletedAt == nil || !updated.DeletedAt.Valid)
	})

	t.Run("ReturnsErrorWhenSnapshotIsNil", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		err = store.UnDeleteSnapshot(context.Background(), nil)
		assert.Error(tt, err)
	})
}

func TestGetWronglyDeletedSnapshot(t *testing.T) {
	t.Run("ReturnsErrorWhenNoMatch", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		result, err := store.GetWronglyDeletedSnapshot(context.Background(), "non-existent-external-uuid")
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}
