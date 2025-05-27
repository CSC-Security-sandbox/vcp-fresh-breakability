package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
		assert.ErrorContains(t, err, "snapshot already exists", "Expected error 'snapshot already exists', got %v", err)
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

		err = store.UpdateSnapshot(context.Background(), updatedSnapshot)
		assert.Equal(t, err, nil)

		result := &datamodel.Snapshot{}
		err = store.db.GORM().First(result, "uuid = ?", updatedSnapshot.UUID).Error
		assert.Equal(t, err, nil)
		assert.Equal(t, updatedSnapshot.State, result.State, "Expected state %v, got %v", updatedSnapshot.State, result.State)
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

		err = store.UpdateSnapshot(context.Background(), updatedSnapshot)
		assert.ErrorContains(t, err, "not found", "Expected error 'not found', got %v", err)
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
