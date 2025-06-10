package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"gorm.io/gorm"
)

func TestCreateVolumeReplication(t *testing.T) {
	t.Run("WhenVolumeReplicationIsCreatedSuccessfully", func(tt *testing.T) {
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

		volumeRep := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:      "test_volume_rep",
			Account:   account,
			Volume:    volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "test-volume-rep-uuid",
			},
		}

		createdVolumeRep, err := store.CreateVolumeReplication(context.Background(), volumeRep)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volumeRep.Name, createdVolumeRep.Name, "Expected volume name %v, got %v", volumeRep.Name, createdVolumeRep.Name)
		assert.Equal(tt, createdVolumeRep.State, models.LifeCycleStateCreating, "Expected volume state %v, got %v", models.LifeCycleStateCreating, createdVolumeRep.State)
		assert.Equal(tt, createdVolumeRep.StateDetails, models.LifeCycleStateCreatingDetails, "Expected volume state %v, got %v", models.LifeCycleStateCreatingDetails, createdVolumeRep.State)
	})
	t.Run("WhenVolumeReplicationAlreadyExists", func(tt *testing.T) {
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

		volumeRep := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:      "test_volume_rep",
			Account:   account,
			Volume:    volume,
		}
		err = store.db.Create(volumeRep).Error()
		if err != nil {
			tt.Fatalf("Failed to create volume replication: %v", err)
		}

		createdVolumeRep, err1 := store.CreateVolumeReplication(context.Background(), volumeRep)
		assert.EqualError(tt, err1, "replication already exists for this volume", "Expected error 'replication already exists for this volume', got %v", err1)
		assert.Nil(tt, createdVolumeRep, "Expected nil volume, got %v", createdVolumeRep)
	})
}

func TestGetVolumeReplication(t *testing.T) {
	t.Run("WhenVolumeReplicationExists", func(tt *testing.T) {
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

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		volumeRep := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:      "test_volume_rep",
			Account:   account,
			Volume:    volume,
		}
		err = store.db.Create(volumeRep).Error()
		assert.NoError(tt, err, "Failed to create volume replication")

		result, err := store.GetVolumeReplication(context.Background(), "test-volume-rep-uuid")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volumeRep.Name, result.Name, "Expected volume name %v, got %v", volumeRep.Name, result.Name)
	})
	t.Run("WhenVolumeDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		volumeRep, err := store.GetVolumeReplication(context.Background(), "test-volume-uuid")
		assert.Nil(tt, volumeRep, "Expected nil volume replication, got %v", volumeRep)
		if err == gorm.ErrRecordNotFound {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}

func TestDeleteVolumeReplication(t *testing.T) {
	t.Run("WhenVolumeReplicationIsDeletedSuccessfully", func(tt *testing.T) {
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

		volumeRep := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:      "test_volume_rep",
			Account:   account,
			Volume:    volume,
		}
		err = store.db.Create(volumeRep).Error()
		if err != nil {
			tt.Fatalf("Failed to create volume replication: %v", err)
		}

		deletedVolumeRep, err := store.DeleteVolumeReplication(context.Background(), volumeRep.UUID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volumeRep.Name, deletedVolumeRep.Name, "Expected volume name %v, got %v", volumeRep.Name, deletedVolumeRep.Name)
		assert.NotNil(tt, deletedVolumeRep.DeletedAt, "Expected volume to be deleted, got %v", deletedVolumeRep.DeletedAt)
		assert.Equal(tt, models.LifeCycleStateDeleted, deletedVolumeRep.State, "Expected volume state %v, got %v", models.LifeCycleStateDeleted, deletedVolumeRep.State)
		assert.Equal(tt, "", deletedVolumeRep.StateDetails, "Expected volume state details %v, got %v", "", deletedVolumeRep.StateDetails)

		_, err = store.GetVolumeReplication(context.Background(), volumeRep.UUID)
		assert.EqualError(tt, err, "volume replication not found", "Expected no error, got %v", err)
	})
	t.Run("WhenVolumeReplicationIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		deletedVolumeRep, err := store.DeleteVolumeReplication(context.Background(), "dummy")
		assert.Nil(tt, deletedVolumeRep, "Expected nil volume replication, got %v", deletedVolumeRep)
		assert.EqualError(tt, err, "volume replication not found", "Expected no error, got %v", err)
	})
}

func TestUpdateVolumeReplication(t *testing.T) {
	t.Run("WhenVolumeReplicationIsUpdatedSuccessfully", func(tt *testing.T) {
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

		volumeRep := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:      "test_volume_rep",
			Account:   account,
			Volume:    volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "test-volume-rep-external-uuid",
			},
		}
		err = store.db.Create(volumeRep).Error()
		assert.NoError(tt, err, "Failed to create volume replication")

		mirrorState := "snapmirrored"
		relationshipStatus := "success"
		var lastTransferSize int64 = 100
		updateVolumeRep := &datamodel.VolumeReplication{
			BaseModel:          datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:               "test_volume_rep",
			State:              models.LifeCycleStateUpdating,
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
			LastTransferSize:   lastTransferSize,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "test-volume-rep-external-uuid",
			},
		}
		err = store.UpdateVolumeReplication(context.Background(), updateVolumeRep)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		updatedVolumeRep, err1 := store.GetVolumeReplication(context.Background(), volumeRep.UUID)
		assert.NoError(tt, err1, "Expected no error, got %v", err1)
		assert.Equal(tt, models.LifeCycleStateUpdating, updatedVolumeRep.State, "Expected volume state %v, got %v", models.LifeCycleStateUpdating, updatedVolumeRep.State)
		assert.Equal(tt, lastTransferSize, updatedVolumeRep.LastTransferSize, "Expected volume last transfer size %v, got %v", lastTransferSize, updatedVolumeRep.LastTransferSize)
	})
	t.Run("WhenVolumeReplicationIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		updateVolumeRep := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "dummy"},
			Name:      "test_volume_rep",
			State:     models.LifeCycleStateUpdating,
		}
		err = store.UpdateVolumeReplication(context.Background(), updateVolumeRep)
		assert.EqualError(tt, err, "volume replication not found", "Expected no error, got %v", err)
	})
}

func TestUpdateVolumeReplicationStates(t *testing.T) {
	t.Run("WhenVolumeReplicationStateIsUpdatedSuccessfully", func(tt *testing.T) {
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

		volumeRep := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:      "test_volume_rep",
			Account:   account,
			Volume:    volume,
		}
		err = store.db.Create(volumeRep).Error()
		assert.NoError(tt, err, "Failed to create volume replication")

		updateVolumeRep := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:      "test_volume_rep",
			State:     models.LifeCycleStateUpdating,
		}
		err = store.UpdateVolumeReplicationStates(context.Background(), updateVolumeRep)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		updatedVolumeRep, err1 := store.GetVolumeReplication(context.Background(), volumeRep.UUID)
		assert.NoError(tt, err1, "Expected no error, got %v", err1)
		assert.Equal(tt, models.LifeCycleStateUpdating, updatedVolumeRep.State, "Expected volume state %v, got %v", models.LifeCycleStateUpdating, updatedVolumeRep.State)
	})
	t.Run("WhenVolumeReplicationIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		updateVolumeRep := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "dummy"},
			Name:      "test_volume_rep",
			State:     models.LifeCycleStateUpdating,
		}
		err = store.UpdateVolumeReplicationStates(context.Background(), updateVolumeRep)
		assert.EqualError(tt, err, "volume replication not found", "Expected no error, got %v", err)
	})
}

func TestUpdateVolumeReplicationTransferStats(t *testing.T) {
	t.Run("WhenVolumeReplicationStateIsUpdatedSuccessfully", func(tt *testing.T) {
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

		volumeRep := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:      "test_volume_rep",
			Account:   account,
			Volume:    volume,
		}
		err = store.db.Create(volumeRep).Error()
		assert.NoError(tt, err, "Failed to create volume replication")

		updateVolumeRep := &datamodel.VolumeReplication{
			BaseModel:        datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:             "test_volume_rep",
			LastTransferSize: 100,
		}
		err = store.UpdateVolumeReplicationTransferStats(context.Background(), updateVolumeRep)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		updatedVolumeRep, err1 := store.GetVolumeReplication(context.Background(), volumeRep.UUID)
		assert.NoError(tt, err1, "Expected no error, got %v", err1)
		assert.Equal(tt, int64(100), updatedVolumeRep.LastTransferSize, "Expected volume state %v, got %v", models.LifeCycleStateUpdating, updatedVolumeRep.State)
	})
	t.Run("WhenVolumeReplicationIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		updateVolumeRep := &datamodel.VolumeReplication{
			BaseModel:        datamodel.BaseModel{UUID: "dummy"},
			Name:             "test_volume_rep",
			LastTransferSize: 100,
		}
		err = store.UpdateVolumeReplicationTransferStats(context.Background(), updateVolumeRep)
		assert.EqualError(tt, err, "volume replication not found", "Expected no error, got %v", err)
	})
}

func TestGetVolumeReplicationCount(t *testing.T) {
	t.Run("WhenAccountExists", func(tt *testing.T) {
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
		assert.NoError(tt, err, "Failed to create account")

		volumeRep := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:      "test_volume_rep",
			AccountID: account.ID,
		}
		err = store.db.Create(volumeRep).Error()
		assert.NoError(tt, err, "Failed to create volume replication")

		count, err := store.GetVolumeReplicationCount(context.Background(), account.Name)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, int64(1), count, "Expected count %v, got %v", 1, count)
	})

	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		count, err := store.GetVolumeReplicationCount(context.Background(), "nonexistent_account")
		assert.EqualError(tt, err, "[0] undefined error: account not found")
		assert.Equal(tt, int64(0), count, "Expected count %v, got %v", 0, count)
	})
}
