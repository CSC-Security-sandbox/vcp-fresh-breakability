package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

func TestGetVolume(t *testing.T) {
	t.Run("WhenVolumeExists", func(tt *testing.T) {
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

		result, err := store.GetVolume(context.Background(), "test-volume-uuid")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volume.Name, result.Name, "Expected volume name %v, got %v", volume.Name, result.Name)
		assert.Equal(tt, account.Name, result.Account.Name, "Expected account name %v, got %v", account.Name, result.Account.Name)
	})
	t.Run("WhenVolumeDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		volume, err := store.GetVolume(context.Background(), "test-volume-uuid")
		assert.Nil(tt, volume, "Expected nil volume, got %v", volume)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}

func TestCreateVolume(t *testing.T) {
	// t.Run("WhenVolumeIsCreatedSuccessfully", func(tt *testing.T) {
	//	db, err := SetupTestDB()
	//	assert.NoError(tt, err, "Failed to set up test database")
	//	wrapper := gormwrapper.New(db)
	//	store := NewDataStoreRepository(wrapper)
	//
	//	err = ClearInMemoryDB(store.db.GORM())
	//	assert.NoError(tt, err, "Failed to clean up test database")
	//
	//	account := &datamodel.Account{
	//		BaseModel: datamodel.BaseModel{
	//			ID:   1,
	//			UUID: "test-account-uuid",
	//		},
	//		Name: "test_account",
	//	}
	//	err = store.db.Create(account).Error()
	//	if err != nil {
	//		tt.Fatalf("Failed to create account: %v", err)
	//	}
	//
	//	pool := &datamodel.Pool{
	//		Name:    "test_pool",
	//		Account: account,
	//	}
	//
	//	err = store.db.Create(pool).Error()
	//	if err != nil {
	//		tt.Fatalf("Failed to create pool: %v", err)
	//	}
	//
	//	volume := &datamodel.Volume{
	//		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
	//		Name:      "test_volume",
	//		AccountID: account.ID,
	//		Account:   account,
	//		Pool:      pool,
	//		PoolID:    pool.ID,
	//	}
	//
	//	createdVolume, err := store.CreateVolume(context.Background(), volume)
	//	assert.NoError(tt, err, "Expected no error, got %v", err)
	//	assert.Equal(tt, volume.Name, createdVolume.Name, "Expected volume name %v, got %v", volume.Name, createdVolume.Name)
	//	assert.Equal(tt, createdVolume.State, models.LifeCycleStateCreating, "Expected volume state %v, got %v", models.LifeCycleStateCreating, createdVolume.State)
	//	assert.Equal(tt, createdVolume.StateDetails, models.LifeCycleStateCreatingDetails, "Expected volume state %v, got %v", models.LifeCycleStateCreatingDetails, createdVolume.State)
	// })
	t.Run("WhenVolumeAlreadyExists", func(tt *testing.T) {
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

		createdVolume, err := store.CreateVolume(context.Background(), volume)
		assert.EqualError(tt, err, "volume already exists", "Expected error 'volume already exists', got %v", err)
		assert.Nil(tt, createdVolume, "Expected nil volume, got %v", createdVolume)
	})
}

func TestDeleteVolume(t *testing.T) {
	t.Run("WhenVolumeIsDeletedSuccessfully", func(tt *testing.T) {
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

		deletedVolume, err := store.DeleteVolume(context.Background(), volume.UUID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volume.Name, deletedVolume.Name, "Expected volume name %v, got %v", volume.Name, deletedVolume.Name)
		assert.NotNil(tt, deletedVolume.DeletedAt, "Expected volume to be deleted, got %v", deletedVolume.DeletedAt)
		assert.Equal(tt, models.LifeCycleStateDeleted, deletedVolume.State, "Expected volume state %v, got %v", models.LifeCycleStateDeleted, deletedVolume.State)
		assert.Equal(tt, "", deletedVolume.StateDetails, "Expected volume state details %v, got %v", "", deletedVolume.StateDetails)

		_, err = store.GetVolume(context.Background(), volume.UUID)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
	t.Run("WhenVolumeIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		deletedVolume, err := store.DeleteVolume(context.Background(), "dummy")
		assert.Nil(tt, deletedVolume, "Expected nil volume, got %v", deletedVolume)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}

func TestUpdateVolumeState(t *testing.T) {
	t.Run("WhenVolumeStateIsUpdatedSuccessfully", func(tt *testing.T) {
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

		updatedVolume, err := store.UpdateVolumeState(context.Background(), volume.UUID, models.LifeCycleStateDeleted, "")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volume.Name, updatedVolume.Name, "Expected volume name %v, got %v", volume.Name, updatedVolume.Name)
		assert.Equal(tt, models.LifeCycleStateDeleted, updatedVolume.State, "Expected volume state %v, got %v", models.LifeCycleStateDeleted, updatedVolume.State)
		assert.Equal(tt, "", updatedVolume.StateDetails, "Expected volume state details %v, got %v", "", updatedVolume.StateDetails)
	})
	t.Run("WhenVolumeIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		updatedVolume, err := store.UpdateVolumeState(context.Background(), "dummy", models.LifeCycleStateDeleted, "")
		assert.Nil(tt, updatedVolume, "Expected nil volume, got %v", updatedVolume)
		assert.ErrorContains(tt, err, "not found", "Expected no error, got %v", err)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}
