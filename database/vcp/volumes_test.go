package database

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
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
		assert.EqualError(tt, err.(*vsaerrors.CustomError).OriginalErr, "volume already exists", "Expected error 'volume already exists', got %v", err)
		assert.EqualError(tt, err, "Invalid input parameters provided", "Expected error 'Invalid input parameters provided', got %v", err)
		assert.Nil(tt, createdVolume, "Expected nil volume, got %v", createdVolume)
	})
	t.Run("CreatesVolumeSuccessfullyWhenParamsAreProvided", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		pool := &datamodel.Pool{
			Name:    "test_pool",
			Account: account,
		}
		assert.NoError(tt, store.db.Create(pool).Error())

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
			VolumeAttributes: &datamodel.VolumeAttributes{
				RestoredBackupID:   "test-backup-uuid",
				RestoredBackupPath: "test-backup-path",
			},
		}

		createdVolume, err := store.CreateVolume(context.Background(), volume)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volume.Name, createdVolume.Name, "Expected volume name %v, got %v", volume.Name, createdVolume.Name)
		assert.Equal(tt, models.LifeCycleStateRestoring, createdVolume.State, "Expected volume state %v, got %v", models.LifeCycleStateRestoring, createdVolume.State)
		assert.Equal(tt, models.LifeCycleStateRestoringDetails, createdVolume.StateDetails, "Expected volume state details %v, got %v", models.LifeCycleStateRestoringDetails, createdVolume.StateDetails)
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

func TestVerifyVolumeOwnership(t *testing.T) {
	t.Run("WhenAccountAndVolumeExist_ThenReturnVolume", func(tt *testing.T) {
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		result, err := store.VerifyVolumeOwnership(context.Background(), "test-volume-uuid", "test_account")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, result, "Expected volume, got nil")
		assert.Equal(tt, volume.UUID, result.UUID, "Expected volume UUID %v, got %v", volume.UUID, result.UUID)
	})

	t.Run("WhenAccountDoesNotExist_ThenReturnError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		result, err := store.VerifyVolumeOwnership(context.Background(), "test-volume-uuid", "nonexistent_account")
		assert.Nil(tt, result, "Expected nil volume, got %v", result)
		assert.Error(tt, err, "Expected error for missing account")
	})

	t.Run("WhenVolumeDoesNotExistForAccount_ThenReturnError", func(tt *testing.T) {
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

		result, err := store.VerifyVolumeOwnership(context.Background(), "nonexistent-volume-uuid", "test_account")
		assert.Nil(tt, result, "Expected nil volume, got %v", result)
		assert.Error(tt, err, "Expected error for missing volume")
	})

	t.Run("WhenVolumeIsNotFoundForAccount_ReturnsNotFoundErr", func(tt *testing.T) {
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

		// Do NOT create a volume for this account

		result, err := store.VerifyVolumeOwnership(context.Background(), "nonexistent-volume-uuid", "test_account")
		assert.Nil(tt, result, "Expected nil volume, got %v", result)
		assert.Error(tt, err, "Expected error for missing volume")
		assert.True(tt, customerrors.IsNotFoundErr(err), "Expected NotFoundErr, got %v", err)
	})
}

func TestUpdateVolumeFields(t *testing.T) {
	t.Run("WhenFieldsAreUpdatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		pool := &datamodel.Pool{
			Name:    "test_pool",
			Account: account,
		}
		assert.NoError(tt, store.db.Create(pool).Error())

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
			State:     "old_state",
		}
		assert.NoError(tt, store.db.Create(volume).Error())

		updates := map[string]interface{}{
			"State":        "new_state",
			"StateDetails": "updated details",
		}
		err = store.UpdateVolumeFields(context.Background(), volume.UUID, updates)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		updated, err := store.GetVolume(context.Background(), volume.UUID)
		assert.NoError(tt, err)
		assert.Equal(tt, "new_state", updated.State)
		assert.Equal(tt, "updated details", updated.StateDetails)
	})

	t.Run("WhenVolumeIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		updates := map[string]interface{}{
			"State": "new_state",
		}
		err = store.UpdateVolumeFields(context.Background(), "nonexistent-uuid", updates)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
	})

	t.Run("WhenTransactionStartFails", func(tt *testing.T) {
		origStartTransaction := startTransaction
		startTransaction = func(db *gorm.DB) (*gorm.DB, error) {
			return nil, errors.New("transaction start failed")
		}
		defer func() { startTransaction = origStartTransaction }()

		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		updates := map[string]interface{}{
			"State": "new_state",
		}
		err = store.UpdateVolumeFields(context.Background(), "any-uuid", updates)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "transaction start failed")
	})

	t.Run("WhenUpdateFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.db.Create(account).Error())

		pool := &datamodel.Pool{
			Name:    "test_pool",
			Account: account,
		}
		assert.NoError(tt, store.db.Create(pool).Error())

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		assert.NoError(tt, store.db.Create(volume).Error())

		// Pass an invalid field to cause update error
		updates := map[string]interface{}{
			"NonExistentField": "value",
		}
		err = store.UpdateVolumeFields(context.Background(), volume.UUID, updates)
		assert.Error(tt, err)
	})
}

func TestGetVolumeCount(t *testing.T) {
	t.Run("WhenAccountExistsWithVolumes", func(tt *testing.T) {
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

		volume1 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid-1"},
			Name:      "test_volume_1",
			AccountID: account.ID,
			Account:   account,
		}
		volume2 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid-2"},
			Name:      "test_volume_2",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(volume1).Error()
		assert.NoError(tt, err, "Failed to create volume 1")
		err = store.db.Create(volume2).Error()
		assert.NoError(tt, err, "Failed to create volume 2")

		count, err := store.GetVolumeCount(context.Background(), "test_account")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, int64(2), count, "Expected volume count %v, got %v", 2, count)
	})

	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		count, err := store.GetVolumeCount(context.Background(), "nonexistent_account")
		assert.Equal(tt, int64(0), count, "Expected volume count %v, got %v", 0, count)
		assert.Error(tt, err, "Expected error for missing account")
	})
}

func TestListVolumesWithDetails(t *testing.T) {
	t.Run("WhenVolumesExist", func(tt *testing.T) {
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
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		volume1 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid-1"},
			Name:      "test_volume_1",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		volume2 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid-2"},
			Name:      "test_volume_2",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.db.Create(volume1).Error()
		assert.NoError(tt, err, "Failed to create volume 1")
		err = store.db.Create(volume2).Error()
		assert.NoError(tt, err, "Failed to create volume 2")

		volumes, err := _listVolumesWithDetails(store.db.GORM())
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 2, len(volumes), "Expected %v volumes, got %v", 2, len(volumes))
		assert.Equal(tt, volume1.UUID, volumes[0].UUID, "Expected volume UUID %v, got %v", volume1.UUID, volumes[0].UUID)
		assert.Equal(tt, volume2.UUID, volumes[1].UUID, "Expected volume UUID %v, got %v", volume2.UUID, volumes[1].UUID)
	})

	t.Run("WhenNoVolumesExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		volumes, err := _listVolumesWithDetails(store.db.GORM())
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 0, len(volumes), "Expected %v volumes, got %v", 0, len(volumes))
	})
}

func TestListVolumes(t *testing.T) {
	t.Run("WhenNoVolumesExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		conditions := [][]interface{}{
			{"account_id", "=", 999}, // Non-existent account ID
		}
		volumes, err := store.ListVolumes(context.Background(), conditions)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 0, len(volumes), "Expected %v volumes, got %v", 0, len(volumes))
	})
	t.Run("ListVolumesWhenBackupPolicyAttached", func(tt *testing.T) {
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

		policyEnabled := true
		policyDisabled := false
		backupPolicyUUID := "test-backup-policy-uuid"

		volume1 := &datamodel.Volume{
			BaseModel:      datamodel.BaseModel{UUID: "test-volume-uuid-1"},
			Name:           "test_volume_1",
			AccountID:      account.ID,
			Account:        account,
			DataProtection: &datamodel.DataProtection{BackupPolicyID: backupPolicyUUID, ScheduledBackupEnabled: &policyEnabled},
		}
		err = store.db.Create(volume1).Error()
		assert.NoError(tt, err, "Failed to create volume 1")

		volume2 := volume1
		volume2.ID = 2
		volume2.UUID = "test-volume-uuid-2"
		volume2.DataProtection.ScheduledBackupEnabled = &policyDisabled
		err = store.db.Create(volume2).Error()
		assert.NoError(tt, err, "Failed to create volume 2")

		volume3 := volume1
		volume3.ID = 3
		volume3.UUID = "test-volume-uuid-3"
		volume3.DataProtection = nil
		err = store.db.Create(volume3).Error()
		assert.NoError(tt, err, "Failed to create volume 3")

		conditions := [][]interface{}{
			{"account_id = ?", accountID},
			{"data_protection->>'backup_policy_id' = ?", backupPolicyUUID},
			{"data_protection->>'scheduled_backup_enabled' = true"},
		}

		volumes, err := store.ListVolumes(context.Background(), conditions)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 1, len(volumes), "Expected %v volumes, got %v", 0, len(volumes))
	})
}

func TestRevertedVolume(t *testing.T) {
	t.Run("WhenVolumeIsRevertedSuccessfully", func(tt *testing.T) {
		// Save the original function
		originalHydrateBatchSnapshotstoCCFE := hydrationActivities.HydrateBatchSnapshotstoCCFE
		defer func() {
			// Restore the original function after the test
			hydrationActivities.HydrateBatchSnapshotstoCCFE = originalHydrateBatchSnapshotstoCCFE
		}()

		// Override the function to always return nil
		hydrationActivities.HydrateBatchSnapshotstoCCFE = func(ctx context.Context, createdSnapshots []*datamodel.Snapshot, deletedSnapshots []*datamodel.Snapshot) error {
			return nil
		}

		// Test setup
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

		// Create pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create volume
		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:        "test_volume",
			AccountID:   account.ID,
			Account:     account,
			Pool:        pool,
			PoolID:      pool.ID,
			SizeInBytes: 1000000,
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 10,
			},
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Create snapshots
		snapshot1 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid-1"},
			Name:      "test_snapshot_1",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Account:   account,
			Volume:    volume,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "ext-uuid-1",
			},
		}
		snapshot2 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid-2"},
			Name:      "test_snapshot_2",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			Account:   account,
			Volume:    volume,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "ext-uuid-2",
			},
		}
		err = store.db.Create(snapshot1).Error()
		assert.NoError(tt, err, "Failed to create snapshot 1")
		err = store.db.Create(snapshot2).Error()
		assert.NoError(tt, err, "Failed to create snapshot 2")

		// Call RevertedVolume
		snapshots, err := store.RevertedVolume(context.Background(), volume, snapshot1)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, snapshots, "Expected snapshots to be returned")
	})
	t.Run("WhenTransactionStartFails", func(tt *testing.T) {
		origStartTransaction := startTransaction
		startTransaction = func(db *gorm.DB) (*gorm.DB, error) {
			return nil, errors.New("transaction start failed")
		}
		defer func() { startTransaction = origStartTransaction }()

		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		}
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
		}

		_, err = store.RevertedVolume(context.Background(), volume, snapshot)
		assert.Error(tt, err, "Expected error for transaction failure")
		assert.Contains(tt, err.Error(), "transaction start failed")
	})
}

func TestRevertDeleteSnapshots(t *testing.T) {
	t.Run("WhenRevertDeleteSnapshotsSucceeds", func(t *testing.T) {
		ctx := context.Background()
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(t, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(t, err)

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
		}
		err = store.db.Create(snapshot).Error()
		assert.NoError(t, err)

		snapshots, err := revertDeleteSnapshots(ctx, store.db.GORM(), volume.ID, snapshot.UUID)
		assert.NoError(t, err)
		assert.NotNil(t, snapshots)
	})

	t.Run("WhenRevertDeleteSnapshotsFailsOnDatabaseReadError", func(t *testing.T) {
		ctx := context.Background()
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		// Use an invalid volume ID to trigger database read error
		// The function handles the error gracefully and returns empty slice instead of error
		snapshots, err := revertDeleteSnapshots(ctx, store.db.GORM(), 99999, "invalid-snapshot-uuid")
		assert.NoError(t, err) // The function handles the error gracefully
		assert.NotNil(t, snapshots)
		assert.Empty(t, snapshots) // Should return empty slice for invalid data
	})

	t.Run("WhenRevertDeleteSnapshotsFailsOnDatabaseUpdateError", func(t *testing.T) {
		ctx := context.Background()
		db, err := SetupTestDB()
		assert.NoError(t, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(t, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(t, err)

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
		}
		err = store.db.Create(snapshot).Error()
		assert.NoError(t, err)

		// Close the database connection to simulate a database error
		sqlDB, err := store.db.GORM().DB()
		assert.NoError(t, err, "Failed to get underlying sql.DB")
		err = sqlDB.Close()
		if err != nil {
			return
		}

		// Now try to call revertDeleteSnapshots - this should fail due to closed connection
		snapshots, err := revertDeleteSnapshots(ctx, store.db.GORM(), volume.ID, snapshot.UUID)
		assert.Error(t, err, "Expected error when database connection is closed")
		assert.Nil(t, snapshots, "Expected nil snapshots when database query fails")
	})
}

func TestGetVolumesByPoolID_ErrorHandling(t *testing.T) {
	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
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

		volumes, err := store.GetVolumesByPoolID(context.Background(), 1)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, volumes, "Expected nil volumes when error occurs")
	})
}

func TestGetVolumeCountByPoolID_ErrorHandling(t *testing.T) {
	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
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

		count, err := store.GetVolumeCountByPoolID(context.Background(), 1)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Equal(tt, int64(0), count, "Expected count to be 0 when error occurs")
	})
}

func TestGetMultipleVolumes_ErrorHandling(t *testing.T) {
	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
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
			{"name", "test-volume"},
		}
		volumes, err := store.GetMultipleVolumes(context.Background(), conditions)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, volumes, "Expected nil volumes when error occurs")
	})
}

func TestGetAllVolumesForHG_ErrorHandling(t *testing.T) {
	t.Run("WhenBlockDevicesQueryErrorOccurs", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test data
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
			VolumeAttributes: &datamodel.VolumeAttributes{
				BlockDevices: &[]datamodel.BlockDevice{
					{
						Name: "test-device",
						HostGroupDetails: []datamodel.HostGroupDetail{
							{
								HostGroupUUID: "test-hostgroup-uuid",
							},
						},
					},
				},
			},
		}

		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Close the database to simulate an error during query
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		if err != nil {
			return
		}

		volumes, err := store.GetAllVolumesForHG(context.Background(), "test-hostgroup-uuid", account.ID)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, volumes, "Expected nil volumes when error occurs")
	})

	t.Run("WhenBlockPropertiesQueryErrorOccurs", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test data
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
			VolumeAttributes: &datamodel.VolumeAttributes{
				BlockProperties: &datamodel.BlockProperties{
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "test-hostgroup-uuid",
						},
					},
				},
			},
		}

		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Close the database to simulate an error during query
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		if err != nil {
			return
		}

		volumes, err := store.GetAllVolumesForHG(context.Background(), "test-hostgroup-uuid", account.ID)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, volumes, "Expected nil volumes when error occurs")
	})
}

// Note: Success tests for GetAllVolumesForHG are skipped because SQLite doesn't support
// PostgreSQL's JSONB syntax used in the queries. These tests would need to be run against
// a PostgreSQL database to work correctly.
func TestGetAllVolumesForHG_Success(t *testing.T) {
	t.Skip("Skipped because SQLite doesn't support PostgreSQL JSONB syntax")
}
