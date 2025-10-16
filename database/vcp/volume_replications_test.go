package database

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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
		assert.Equal(tt, createdVolumeRep.ReplicationAttributes.DestinationReplicationUUID, createdVolumeRep.UUID)
	})
	t.Run("WhenVolumeReplicationIsCreatedSuccessfullyFromSrc", func(tt *testing.T) {
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
				EndpointType:               "src",
			},
		}

		createdVolumeRep, err := store.CreateVolumeReplication(context.Background(), volumeRep)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volumeRep.Name, createdVolumeRep.Name, "Expected volume name %v, got %v", volumeRep.Name, createdVolumeRep.Name)
		assert.Equal(tt, createdVolumeRep.State, models.LifeCycleStateCreating, "Expected volume state %v, got %v", models.LifeCycleStateCreating, createdVolumeRep.State)
		assert.Equal(tt, createdVolumeRep.StateDetails, models.LifeCycleStateCreatingDetails, "Expected volume state %v, got %v", models.LifeCycleStateCreatingDetails, createdVolumeRep.State)
		assert.Equal(tt, createdVolumeRep.ReplicationAttributes.SourceReplicationUUID, createdVolumeRep.UUID)
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
			BaseModel:    datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:         "test_volume_rep",
			Account:      account,
			Volume:       volume,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}
		err = store.db.Create(volumeRep).Error()
		if err != nil {
			tt.Fatalf("Failed to create volume replication: %v", err)
		}

		deletedVolumeRep, err := store.DeleteVolumeReplication(context.Background(), volumeRep)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volumeRep.Name, deletedVolumeRep.Name, "Expected volume name %v, got %v", volumeRep.Name, deletedVolumeRep.Name)
		assert.NotNil(tt, deletedVolumeRep.DeletedAt, "Expected volume to be deleted, got %v", deletedVolumeRep.DeletedAt)
		assert.Equal(tt, models.LifeCycleStateDeleted, deletedVolumeRep.State, "Expected volume state %v, got %v", models.LifeCycleStateDeleted, deletedVolumeRep.State)
		assert.Equal(tt, models.LifeCycleStateDeletedDetails, deletedVolumeRep.StateDetails, "Expected volume state details %v, got %v", models.LifeCycleStateDeletedDetails, deletedVolumeRep.StateDetails)

		_, err = store.GetVolumeReplication(context.Background(), volumeRep.UUID)
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

		mirrorState := models.OntapUninitialized
		mirrorStateSnapmirrored := models.OntapSnapmirrored
		relationshipStatus := models.SnapmirrorRelationshipIdle
		volumeRep := &datamodel.VolumeReplication{
			BaseModel:          datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:               "test_volume_rep",
			Account:            account,
			Volume:             volume,
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
			Healthy:            false,
		}
		err = store.db.Create(volumeRep).Error()
		assert.NoError(tt, err, "Failed to create volume replication")

		updateVolumeRep := &datamodel.VolumeReplication{
			BaseModel:        datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
			Name:             "test_volume_rep",
			LastTransferSize: 100,
			MirrorState:      &mirrorStateSnapmirrored,
			Healthy:          true,
		}
		err = store.UpdateVolumeReplicationTransferStats(context.Background(), updateVolumeRep)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		updatedVolumeRep, err1 := store.GetVolumeReplication(context.Background(), volumeRep.UUID)
		assert.NoError(tt, err1, "Expected no error, got %v", err1)
		assert.Equal(tt, int64(100), updatedVolumeRep.LastTransferSize, "Expected volume last transfer size %v, got %v", 100, updatedVolumeRep.LastTransferSize)
		assert.Equal(tt, models.OntapSnapmirrored, *updatedVolumeRep.MirrorState, "Expected volume mirror state %v, got %v", models.OntapSnapmirrored, *updatedVolumeRep.MirrorState)
		assert.True(tt, updatedVolumeRep.Healthy, "Expected volume healthy status %v, got %v", true, updatedVolumeRep.Healthy)
		assert.Equal(tt, models.SnapmirrorRelationshipIdle, *updatedVolumeRep.RelationshipStatus, "Expected volume relationship status %v, got %v", models.SnapmirrorRelationshipIdle, *updatedVolumeRep.RelationshipStatus)
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
		var customErr *errors2.CustomError
		assert.True(tt, errors2.As(err, &customErr), "Expected a CustomError")
		assert.ErrorContains(tt, customErr.OriginalErr, "account not found")
		assert.Equal(tt, int64(0), count, "Expected count %v, got %v", 0, count)
	})
}

func CreateTestData(store *DataStoreRepository) (*datamodel.Account, *datamodel.Pool, *datamodel.Volume, error) {
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-account-uuid",
		},
		Name: "test_account",
	}
	err := store.db.Create(account).Error()
	if err != nil {
		return nil, nil, nil, err
	}

	pool := &datamodel.Pool{
		Name:    "test_pool",
		Account: account,
		ClusterDetails: datamodel.ClusterDetails{
			ExternalName: "external-cluster",
		},
	}
	err = store.db.Create(pool).Error()
	if err != nil {
		return nil, nil, nil, err
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
		return nil, nil, nil, err
	}

	return account, pool, volume, nil
}

func TestListVolumeReplications(t *testing.T) {
	t.Run("HappyPath", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		if err != nil {
			t.Fatalf("Failed to create test data: %v", err)
		}

		replication1 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-1"},
			Name:      "replication_1",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
		}
		err = store.db.Create(replication1).Error()
		if err != nil {
			t.Fatalf("Failed to create replication 1: %v", err)
		}

		replication2 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-2"},
			Name:      "replication_2",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
		}
		err = store.db.Create(replication2).Error()
		if err != nil {
			t.Fatalf("Failed to create replication 2: %v", err)
		}

		replicationUUIDs := []string{replication1.UUID, replication2.UUID}
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", account.ID),
			utils.NewFilterCondition("uuid", "in", replicationUUIDs))

		replications, err := store.ListVolumeReplications(context.Background(), *filter)
		assert.NoError(t, err, "Expected no error, got %v", err)
		assert.Len(t, replications, 2, "Expected 2 volume replications, got %d", len(replications))
		assert.Equal(t, replication1.UUID, replications[0].UUID, "Expected replication 1 UUID %v, got %v", replication1.UUID, replications[0].UUID)
		assert.Equal(t, replication2.UUID, replications[1].UUID, "Expected replication 2 UUID %v, got %v", replication2.UUID, replications[1].UUID)
		assert.Equal(t, "external-cluster", replications[0].Volume.Pool.ClusterDetails.ExternalName, "Expected cluster name %v, got %v", "external-cluster", replications[0].Volume.Pool.ClusterDetails.ExternalName)
	})
	t.Run("WhenNoReplicationsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err, "Failed to clean up test database")

		account, _, _, err := CreateTestData(store)
		if err != nil {
			t.Fatalf("Failed to create test data: %v", err)
		}

		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", account.ID))

		replications, err := store.ListVolumeReplications(context.Background(), *filter)
		assert.NoError(t, err, "Expected no error, got %v", err)
		assert.Empty(t, replications, "Expected no volume replications, got %d", len(replications))
	})
	t.Run("WhenFilterIsNil", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err, "Failed to clean up test database")

		_, _, _, err = CreateTestData(store)
		if err != nil {
			t.Fatalf("Failed to create test data: %v", err)
		}

		expectedError := customerrors.NewUserInputValidationErr("no filter conditions provided for listing volume replications")

		replications, err := store.ListVolumeReplications(context.Background(), utils.Filter{})
		assert.EqualError(t, expectedError, "no filter conditions provided for listing volume replications", "Expected error %v, got %v", expectedError, err)
		assert.Empty(t, replications, "Expected no volume replications, got %d", len(replications))
	})
	t.Run("HappyPathNewFilter", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		if err != nil {
			t.Fatalf("Failed to create test data: %v", err)
		}

		replication1 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-1"},
			Name:      "replication_1",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
			Uri:       "projects/45110233509/locations/australia-southeast1/volume/godpvolume4/replications/replication-name-6",
		}
		err = store.db.Create(replication1).Error()
		if err != nil {
			t.Fatalf("Failed to create replication 1: %v", err)
		}

		replication2 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-2"},
			Name:      "replication_2",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
			RemoteUri: "projects/45110233509/locations/us-east4/volume/gosrcvolume1/replications/replication-name-6",
		}
		err = store.db.Create(replication2).Error()
		if err != nil {
			t.Fatalf("Failed to create replication 2: %v", err)
		}

		// replicationUUIDs := []string{replication1.UUID, replication2.UUID}
		uris := []string{replication1.Uri, replication2.RemoteUri}
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", account.ID),
			utils.NewFilterCondition("uri", "in", uris))

		replications, err := store.ListVolumeReplications(context.Background(), *filter)
		assert.NoError(t, err, "Expected no error, got %v", err)
		assert.Len(t, replications, 1, "Expected 2 volume replications, got %d", len(replications))
		assert.Equal(t, replication1.UUID, replications[0].UUID, "Expected replication 1 UUID %v, got %v", replication1.UUID, replications[0].UUID)
		assert.Equal(t, "external-cluster", replications[0].Volume.Pool.ClusterDetails.ExternalName, "Expected cluster name %v, got %v", "external-cluster", replications[0].Volume.Pool.ClusterDetails.ExternalName)
	})
}

// TestUpdateVolumeReplicationFields tests the UpdateVolumeReplicationFields method
func TestUpdateVolumeReplicationFields(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	account, _, volume, err := CreateTestData(store)
	assert.NoError(t, err, "Failed to create test data")

	volumeRep := &datamodel.VolumeReplication{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-rep-uuid"},
		Name:      "test_volume_rep",
		AccountID: account.ID,
		VolumeID:  volume.ID,
	}
	err = store.db.Create(volumeRep).Error()
	assert.NoError(t, err, "Failed to create volume replication")

	t.Run("UpdateSingleField", func(tt *testing.T) {
		updates := map[string]interface{}{
			"name": "updated_volume_rep",
		}
		err := store.UpdateVolumeReplicationFields(context.Background(), volumeRep.UUID, updates)
		assert.NoError(tt, err, "Expected no error updating field")

		updated, err := store.GetVolumeReplication(context.Background(), volumeRep.UUID)
		assert.NoError(tt, err)
		assert.Equal(tt, "updated_volume_rep", updated.Name)
	})

	t.Run("UpdateMultipleFields", func(tt *testing.T) {
		updates := map[string]interface{}{
			"state":         models.LifeCycleStateUpdating,
			"state_details": "updating details",
		}
		err := store.UpdateVolumeReplicationFields(context.Background(), volumeRep.UUID, updates)
		assert.NoError(tt, err, "Expected no error updating fields")

		updated, err := store.GetVolumeReplication(context.Background(), volumeRep.UUID)
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateUpdating, updated.State)
		assert.Equal(tt, "updating details", updated.StateDetails)
	})

	t.Run("UpdateNonExistentReplication", func(tt *testing.T) {
		updates := map[string]interface{}{
			"name": "should_not_exist",
		}
		err := store.UpdateVolumeReplicationFields(context.Background(), "non-existent-uuid", updates)
		assert.Error(tt, err, "Expected error for non-existent replication")
	})
}

func TestListVolumeReplicationsWithPagination(t *testing.T) {
	t.Run("WhenNoVolumeReplicationsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		conditions := [][]interface{}{
			{"account_id", "=", 999}, // Non-existent account ID
		}
		pagination := &utils.Pagination{Limit: 10, Offset: 0}
		replications, err := store.ListVolumeReplicationsWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 0, len(replications), "Expected %v replications, got %v", 0, len(replications))
	})

	t.Run("WhenVolumeReplicationsExistWithPagination", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create pool with deployment name
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Create 5 volume replications for pagination testing
		replications := make([]*datamodel.VolumeReplication, 5)
		for i := 0; i < 5; i++ {
			replications[i] = &datamodel.VolumeReplication{
				BaseModel: datamodel.BaseModel{UUID: fmt.Sprintf("test-replication-uuid-%d", i+1)},
				Name:      fmt.Sprintf("test_replication_%d", i+1),
				AccountID: account.ID,
				Account:   account,
				VolumeID:  volume.ID,
				Volume:    volume,
			}
			err = store.db.Create(replications[i]).Error()
			assert.NoError(tt, err, "Failed to create replication %d", i+1)
		}

		conditions := [][]interface{}{
			{"account_id = ?", account.ID},
		}

		// Test first page with limit 2
		pagination := &utils.Pagination{Limit: 2, Offset: 0}
		resultReplications, err := store.ListVolumeReplicationsWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 2, len(resultReplications), "Expected 2 replications on first page, got %v", len(resultReplications))

		// Verify that Account is preloaded
		assert.NotNil(tt, resultReplications[0].Account, "Account should be preloaded")
		assert.Equal(tt, account.Name, resultReplications[0].Account.Name, "Account name should match")

		// Verify that Volume is preloaded with only id and pool_id
		assert.NotNil(tt, resultReplications[0].Volume, "Volume should be preloaded")
		assert.Equal(tt, volume.ID, resultReplications[0].Volume.ID, "Volume ID should match")
		assert.Equal(tt, pool.ID, resultReplications[0].Volume.PoolID, "Volume PoolID should match")

		// Verify that Volume.Pool is preloaded with only id and deployment_name
		assert.NotNil(tt, resultReplications[0].Volume.Pool, "Volume.Pool should be preloaded")
		assert.Equal(tt, pool.ID, resultReplications[0].Volume.Pool.ID, "Pool ID should match")
		assert.Equal(tt, pool.DeploymentName, resultReplications[0].Volume.Pool.DeploymentName, "Pool DeploymentName should match")

		// Test second page with limit 2
		pagination = &utils.Pagination{Limit: 2, Offset: 2}
		resultReplications, err = store.ListVolumeReplicationsWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 2, len(resultReplications), "Expected 2 replications on second page, got %v", len(resultReplications))

		// Test third page with limit 2
		pagination = &utils.Pagination{Limit: 2, Offset: 4}
		resultReplications, err = store.ListVolumeReplicationsWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 1, len(resultReplications), "Expected 1 replication on third page, got %v", len(resultReplications))

		// Test with limit larger than total replications
		pagination = &utils.Pagination{Limit: 10, Offset: 0}
		resultReplications, err = store.ListVolumeReplicationsWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 5, len(resultReplications), "Expected 5 replications with large limit, got %v", len(resultReplications))
	})

	t.Run("WhenPaginationIsNil", func(tt *testing.T) {
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

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test_replication",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
			Volume:    volume,
		}
		err = store.db.Create(replication).Error()
		assert.NoError(tt, err, "Failed to create replication")

		conditions := [][]interface{}{
			{"account_id", "=", account.ID},
		}

		// Test with nil pagination - should use default limit
		resultReplications, err := store.ListVolumeReplicationsWithPagination(context.Background(), conditions, nil)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 1, len(resultReplications), "Expected 1 replication with nil pagination, got %v", len(resultReplications))
	})

	t.Run("WhenPaginationHasZeroLimit", func(tt *testing.T) {
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

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test_replication",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
			Volume:    volume,
		}
		err = store.db.Create(replication).Error()
		assert.NoError(tt, err, "Failed to create replication")

		conditions := [][]interface{}{
			{"account_id", "=", account.ID},
		}

		// Test with zero limit - should use default limit
		pagination := &utils.Pagination{Limit: 0, Offset: 0}
		resultReplications, err := store.ListVolumeReplicationsWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 1, len(resultReplications), "Expected 1 replication with zero limit (default), got %v", len(resultReplications))
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate an error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		if err != nil {
			return
		}

		conditions := [][]interface{}{
			{"account_id", "=", 1},
		}
		pagination := &utils.Pagination{Limit: 10, Offset: 0}
		replications, err := store.ListVolumeReplicationsWithPagination(context.Background(), conditions, pagination)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, replications, "Expected nil replications when error occurs")
	})

	t.Run("WhenOffsetExceedsTotalReplications", func(tt *testing.T) {
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

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test_replication",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
			Volume:    volume,
		}
		err = store.db.Create(replication).Error()
		assert.NoError(tt, err, "Failed to create replication")

		conditions := [][]interface{}{
			{"account_id", "=", account.ID},
		}

		// Test with offset beyond total replications
		pagination := &utils.Pagination{Limit: 10, Offset: 100}
		resultReplications, err := store.ListVolumeReplicationsWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 0, len(resultReplications), "Expected 0 replications when offset exceeds total, got %v", len(resultReplications))
	})

	t.Run("WhenVolumeReplicationsWithDifferentStates", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		accountID := int64(1)
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   accountID,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Create replications with different states
		replication1 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid-1"},
			Name:      "test_replication_1",
			State:     "active",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
			Volume:    volume,
		}
		err = store.db.Create(replication1).Error()
		assert.NoError(tt, err, "Failed to create replication 1")

		replication2 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid-2"},
			Name:      "test_replication_2",
			State:     "paused",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
			Volume:    volume,
		}
		err = store.db.Create(replication2).Error()
		assert.NoError(tt, err, "Failed to create replication 2")

		replication3 := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid-3"},
			Name:      "test_replication_3",
			State:     "broken",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
			Volume:    volume,
		}
		err = store.db.Create(replication3).Error()
		assert.NoError(tt, err, "Failed to create replication 3")

		conditions := [][]interface{}{
			{"account_id = ?", accountID},
			{"state = ?", "active"},
		}

		pagination := &utils.Pagination{Limit: 10, Offset: 0}
		replications, err := store.ListVolumeReplicationsWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 1, len(replications), "Expected 1 active replication, got %v", len(replications))
		assert.Equal(tt, "test-replication-uuid-1", replications[0].UUID, "Expected replication 1 UUID, got %v", replications[0].UUID)
	})

	t.Run("WhenOptimizedPreloadsWorkCorrectly", func(tt *testing.T) {
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

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
			// Add other fields that should NOT be loaded
			Description: "pool description",
			State:       "active",
			SizeInBytes: 1000000,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
			// Add other fields that should NOT be loaded
			Description: "volume description",
			State:       "active",
			SizeInBytes: 500000,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "test-replication-uuid"},
			Name:      "test_replication",
			AccountID: account.ID,
			Account:   account,
			VolumeID:  volume.ID,
			Volume:    volume,
		}
		err = store.db.Create(replication).Error()
		assert.NoError(tt, err, "Failed to create replication")

		conditions := [][]interface{}{
			{"account_id", "=", account.ID},
		}

		pagination := &utils.Pagination{Limit: 10, Offset: 0}
		resultReplications, err := store.ListVolumeReplicationsWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 1, len(resultReplications), "Expected 1 replication, got %v", len(resultReplications))

		// Verify Account is fully loaded
		assert.NotNil(tt, resultReplications[0].Account, "Account should be preloaded")
		assert.Equal(tt, account.Name, resultReplications[0].Account.Name, "Account name should match")

		// Verify Volume is partially loaded (only id and pool_id)
		assert.NotNil(tt, resultReplications[0].Volume, "Volume should be preloaded")
		assert.Equal(tt, volume.ID, resultReplications[0].Volume.ID, "Volume ID should match")
		assert.Equal(tt, pool.ID, resultReplications[0].Volume.PoolID, "Volume PoolID should match")
		// These fields should be empty/zero due to selective loading
		assert.Empty(tt, resultReplications[0].Volume.Name, "Volume Name should be empty due to selective loading")
		assert.Empty(tt, resultReplications[0].Volume.Description, "Volume Description should be empty due to selective loading")
		assert.Zero(tt, resultReplications[0].Volume.SizeInBytes, "Volume SizeInBytes should be zero due to selective loading")

		// Verify Volume.Pool is partially loaded (only id and deployment_name)
		assert.NotNil(tt, resultReplications[0].Volume.Pool, "Volume.Pool should be preloaded")
		assert.Equal(tt, pool.ID, resultReplications[0].Volume.Pool.ID, "Pool ID should match")
		assert.Equal(tt, pool.DeploymentName, resultReplications[0].Volume.Pool.DeploymentName, "Pool DeploymentName should match")
		// These fields should be empty/zero due to selective loading
		assert.Empty(tt, resultReplications[0].Volume.Pool.Name, "Pool Name should be empty due to selective loading")
		assert.Empty(tt, resultReplications[0].Volume.Pool.Description, "Pool Description should be empty due to selective loading")
		assert.Zero(tt, resultReplications[0].Volume.Pool.SizeInBytes, "Pool SizeInBytes should be zero due to selective loading")
	})
}
