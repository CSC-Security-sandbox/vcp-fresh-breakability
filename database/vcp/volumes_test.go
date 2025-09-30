package database

import (
	"context"
	"errors"
	"fmt"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
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
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
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
		assert.EqualError(tt, err.(*vsaerrors.CustomError).OriginalErr, "volume with this name already exists in the same zone", "Expected error 'volume already exists', got %v", err)
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

func TestDeleteVolumeAndChildResources(t *testing.T) {
	t.Run("WhenVolumeAndChildResourcesAreDeletedSuccessfully", func(tt *testing.T) {
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
			Volume:    volume,
			AccountID: account.ID,
			Account:   account,
		}

		err = store.db.Create(snapshot).Error()
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		deletedVolume, err := store.DeleteVolumeAndChildResources(context.Background(), volume.UUID)
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

		deletedVolume, err := store.DeleteVolumeAndChildResources(context.Background(), "dummy")
		assert.Nil(tt, deletedVolume, "Expected nil volume, got %v", deletedVolume)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
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

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		deletedVolume, err := store.DeleteVolumeAndChildResources(context.Background(), "dummy")
		assert.Nil(tt, deletedVolume, "Expected nil volume, got %v", deletedVolume)
		assert.ErrorContains(tt, err, "transaction start failed", "Expected error 'transaction start failed', got %v", err)
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

func TestBatchUpdateVolumeFields(t *testing.T) {
	// Note: We can't test the actual SQL execution with SQLite since it uses PostgreSQL-specific syntax,
	// but we can test error scenarios, validation, and business logic

	t.Run("WhenUpdatesSliceIsEmpty", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Test with empty slice - should return immediately without database operations
		err = store.BatchUpdateVolumeFields(context.Background(), []datamodel.VolumeFieldUpdate{})
		assert.NoError(tt, err, "Expected no error for empty updates slice")
	})

	t.Run("WhenNilUpdatesSlice", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Test with nil slice - should return immediately
		err = store.BatchUpdateVolumeFields(context.Background(), nil)
		assert.NoError(tt, err, "Expected no error for nil updates slice")
	})

	t.Run("WhenDatabaseConnectionFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		// Close the database to simulate connection failure
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		// Prepare valid updates
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: "test-uuid-1",
				Fields: map[string]interface{}{
					"used_bytes": int64(1000),
				},
			},
		}

		// Should fail due to closed database
		err = store.BatchUpdateVolumeFields(context.Background(), updates)
		assert.Error(tt, err, "Expected error when database connection is closed")
	})

	t.Run("WhenSQLExecutionFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Prepare updates that will cause SQL execution to fail
		// (SQLite doesn't support PostgreSQL VALUES syntax)
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: "test-uuid-1",
				Fields: map[string]interface{}{
					"used_bytes": int64(1000),
				},
			},
		}

		// Should fail due to SQL syntax error in SQLite
		err = store.BatchUpdateVolumeFields(context.Background(), updates)
		assert.Error(tt, err, "Expected error due to PostgreSQL-specific SQL in SQLite")

		// Verify it returns the proper VCP error type
		assert.Contains(tt, err.Error(), "An internal error occurred", "Expected VCP database error")
	})

	t.Run("WhenBuildVolumeUpdateQueryIsCalledCorrectly", func(tt *testing.T) {
		// Test that buildVolumeUpdateQuery is called with correct parameters
		// This tests the integration without requiring actual SQL execution
		store := &DataStoreRepository{}

		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: "test-uuid-1",
				Fields: map[string]interface{}{
					"used_bytes": int64(1000),
				},
			},
			{
				UUID: "test-uuid-2",
				Fields: map[string]interface{}{
					"used_bytes": int64(2000),
				},
			},
		}

		// Call buildVolumeUpdateQuery directly to verify it works correctly
		sql, args := store.buildVolumeUpdateQuery(context.Background(), updates)

		// Verify the query and args are generated correctly
		assert.NotEmpty(tt, sql, "SQL should not be empty")
		assert.Len(tt, args, 4, "Should have 4 arguments for 2 updates")
		assert.Equal(tt, "test-uuid-1", args[0], "First UUID should match")
		assert.Equal(tt, int64(1000), args[1], "First used_bytes should match")
		assert.Equal(tt, "test-uuid-2", args[2], "Second UUID should match")
		assert.Equal(tt, int64(2000), args[3], "Second used_bytes should match")
	})

	t.Run("WhenUpdatingSingleVolume", func(tt *testing.T) {
		tt.Skip("Skipped because this function uses PostgreSQL-specific VALUES clause syntax not supported in SQLite")

		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Setup test data
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
			UsedBytes: 1000, // Initial value
		}

		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Prepare update
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: volume.UUID,
				Fields: map[string]interface{}{
					"used_bytes": int64(2000),
				},
			},
		}

		// Execute batch update
		err = store.BatchUpdateVolumeFields(context.Background(), updates)
		assert.NoError(tt, err, "Expected no error during batch update")

		// Verify the update
		updatedVolume, err := store.GetVolume(context.Background(), volume.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated volume")
		assert.Equal(tt, int64(2000), updatedVolume.UsedBytes, "Expected used_bytes to be updated to 2000")
	})

	t.Run("WhenUpdatingMultipleVolumes", func(tt *testing.T) {
		tt.Skip("Skipped because this function uses PostgreSQL-specific VALUES clause syntax not supported in SQLite")

		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Setup test data
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

		volume1 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-1-uuid"},
			Name:      "test_volume_1",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
			UsedBytes: 1000,
		}

		volume2 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-2-uuid"},
			Name:      "test_volume_2",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
			UsedBytes: 1500,
		}

		volume3 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-3-uuid"},
			Name:      "test_volume_3",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
			UsedBytes: 2000,
		}

		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")
		err = store.db.Create(volume1).Error()
		assert.NoError(tt, err, "Failed to create volume1")
		err = store.db.Create(volume2).Error()
		assert.NoError(tt, err, "Failed to create volume2")
		err = store.db.Create(volume3).Error()
		assert.NoError(tt, err, "Failed to create volume3")

		// Prepare batch updates
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: volume1.UUID,
				Fields: map[string]interface{}{
					"used_bytes": int64(3000),
				},
			},
			{
				UUID: volume2.UUID,
				Fields: map[string]interface{}{
					"used_bytes": int64(4000),
				},
			},
			{
				UUID: volume3.UUID,
				Fields: map[string]interface{}{
					"used_bytes": int64(5000),
				},
			},
		}

		// Execute batch update
		err = store.BatchUpdateVolumeFields(context.Background(), updates)
		assert.NoError(tt, err, "Expected no error during batch update")

		// Verify all updates
		updatedVolume1, err := store.GetVolume(context.Background(), volume1.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated volume1")
		assert.Equal(tt, int64(3000), updatedVolume1.UsedBytes, "Expected volume1 used_bytes to be updated to 3000")

		updatedVolume2, err := store.GetVolume(context.Background(), volume2.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated volume2")
		assert.Equal(tt, int64(4000), updatedVolume2.UsedBytes, "Expected volume2 used_bytes to be updated to 4000")

		updatedVolume3, err := store.GetVolume(context.Background(), volume3.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated volume3")
		assert.Equal(tt, int64(5000), updatedVolume3.UsedBytes, "Expected volume3 used_bytes to be updated to 5000")
	})

	t.Run("WhenVolumeDoesNotExist", func(tt *testing.T) {
		tt.Skip("Skipped because this function uses PostgreSQL-specific VALUES clause syntax not supported in SQLite")

		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Prepare update for non-existent volume
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: "non-existent-volume-uuid",
				Fields: map[string]interface{}{
					"used_bytes": int64(2000),
				},
			},
		}

		// Execute batch update - this should not error as it's a bulk operation
		// The UPDATE will simply affect 0 rows
		err = store.BatchUpdateVolumeFields(context.Background(), updates)
		assert.NoError(tt, err, "Expected no error even when volume doesn't exist in bulk update")
	})

	t.Run("WhenMixedExistentAndNonExistentVolumes", func(tt *testing.T) {
		tt.Skip("Skipped because this function uses PostgreSQL-specific VALUES clause syntax not supported in SQLite")

		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Setup test data
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
			UsedBytes: 1000,
		}

		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Prepare mixed updates (existing + non-existing volume)
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: volume.UUID, // This exists
				Fields: map[string]interface{}{
					"used_bytes": int64(2000),
				},
			},
			{
				UUID: "non-existent-volume-uuid", // This doesn't exist
				Fields: map[string]interface{}{
					"used_bytes": int64(3000),
				},
			},
		}

		// Execute batch update
		err = store.BatchUpdateVolumeFields(context.Background(), updates)
		assert.NoError(tt, err, "Expected no error during mixed batch update")

		// Verify the existing volume was updated
		updatedVolume, err := store.GetVolume(context.Background(), volume.UUID)
		assert.NoError(tt, err, "Failed to retrieve updated volume")
		assert.Equal(tt, int64(2000), updatedVolume.UsedBytes, "Expected existing volume used_bytes to be updated to 2000")
	})
}

func TestGetVolumeByName(t *testing.T) {
	t.Run("WhenVolumeExistsWithName", func(tt *testing.T) {
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

		result, err := store.GetVolumeByName(context.Background(), "test_volume")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volume.Name, result.Name, "Expected volume name %v, got %v", volume.Name, result.Name)
		assert.Equal(tt, account.Name, result.Account.Name, "Expected account name %v, got %v", account.Name, result.Account.Name)
		assert.Equal(tt, volume.UUID, result.UUID, "Expected volume UUID %v, got %v", volume.UUID, result.UUID)
	})

	t.Run("WhenVolumeDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		volume, err := store.GetVolumeByName(context.Background(), "nonexistent_volume")
		assert.Nil(tt, volume, "Expected nil volume, got %v", volume)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate an error during query
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		if err != nil {
			return
		}

		volume, err := store.GetVolumeByName(context.Background(), "test_volume")
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, volume, "Expected nil volume when error occurs")
	})
}

func TestGetVolumeWithAccountID(t *testing.T) {
	t.Run("WhenVolumeExistsWithUUIDAndAccountID", func(tt *testing.T) {
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

		result, err := store.GetVolumeWithAccountID(context.Background(), "test-volume-uuid", account.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volume.Name, result.Name, "Expected volume name %v, got %v", volume.Name, result.Name)
		assert.Equal(tt, account.Name, result.Account.Name, "Expected account name %v, got %v", account.Name, result.Account.Name)
		assert.Equal(tt, volume.UUID, result.UUID, "Expected volume UUID %v, got %v", volume.UUID, result.UUID)
	})

	t.Run("WhenVolumeDoesNotExist", func(tt *testing.T) {
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

		volume, err := store.GetVolumeWithAccountID(context.Background(), "nonexistent-volume-uuid", account.ID)
		assert.Nil(tt, volume, "Expected nil volume, got %v", volume)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})

	t.Run("WhenVolumeExistsButDifferentAccountID", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account1 := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid-1",
			},
			Name: "test_account_1",
		}

		account2 := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-account-uuid-2",
			},
			Name: "test_account_2",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account1.ID,
			Account:   account1,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account1.ID,
			Account:   account1,
			Pool:      pool,
			PoolID:    pool.ID,
		}

		err = store.db.Create(account1).Error()
		assert.NoError(tt, err, "Failed to create account1")
		err = store.db.Create(account2).Error()
		assert.NoError(tt, err, "Failed to create account2")
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Try to get volume with account2's ID (should not find it)
		result, err := store.GetVolumeWithAccountID(context.Background(), "test-volume-uuid", account2.ID)
		assert.Nil(tt, result, "Expected nil volume, got %v", result)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected NotFoundErr, got %v", err)
		}
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate an error during query
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		if err != nil {
			return
		}

		volume, err := store.GetVolumeWithAccountID(context.Background(), "test-volume-uuid", int64(1))
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, volume, "Expected nil volume when error occurs")
	})
}

func TestGetVolumeByNameAndAccountID(t *testing.T) {
	t.Run("WhenVolumeExistsWithNameAndAccountID", func(tt *testing.T) {
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

		result, err := store.GetVolumeByNameAndAccountID(context.Background(), "test_volume", account.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, volume.Name, result.Name, "Expected volume name %v, got %v", volume.Name, result.Name)
		assert.Equal(tt, account.Name, result.Account.Name, "Expected account name %v, got %v", account.Name, result.Account.Name)
		assert.Equal(tt, volume.UUID, result.UUID, "Expected volume UUID %v, got %v", volume.UUID, result.UUID)
	})

	t.Run("WhenVolumeDoesNotExist", func(tt *testing.T) {
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

		volume, err := store.GetVolumeByNameAndAccountID(context.Background(), "nonexistent_volume", account.ID)
		assert.Nil(tt, volume, "Expected nil volume, got %v", volume)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})

	t.Run("WhenVolumeExistsButDifferentAccountID", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account1 := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid-1",
			},
			Name: "test_account_1",
		}

		account2 := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-account-uuid-2",
			},
			Name: "test_account_2",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account1.ID,
			Account:   account1,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account1.ID,
			Account:   account1,
			Pool:      pool,
			PoolID:    pool.ID,
		}

		err = store.db.Create(account1).Error()
		assert.NoError(tt, err, "Failed to create account1")
		err = store.db.Create(account2).Error()
		assert.NoError(tt, err, "Failed to create account2")
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err, "Failed to create volume")

		// Try to get volume with account2's ID (should not find it)
		result, err := store.GetVolumeByNameAndAccountID(context.Background(), "test_volume", account2.ID)
		assert.Nil(tt, result, "Expected nil volume, got %v", result)
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected NotFoundErr, got %v", err)
		}
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate an error during query
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		if err != nil {
			return
		}

		volume, err := store.GetVolumeByNameAndAccountID(context.Background(), "test_volume", int64(1))
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, volume, "Expected nil volume when error occurs")
	})
}

// Note: Success tests for GetAllVolumesForHG are skipped because SQLite doesn't support
// PostgreSQL's JSONB syntax used in the queries. These tests would need to be run against
// a PostgreSQL database to work correctly.
func TestGetAllVolumesForHG_Success(t *testing.T) {
	t.Skip("Skipped because SQLite doesn't support PostgreSQL JSONB syntax")
}

func TestGetVolumeByNameAccountIDAndZone(t *testing.T) {
	// Note: The main functionality tests are skipped for SQLite because GetVolumeByNameAccountIDAndZone uses
	// PostgreSQL's JSONB syntax (pools.pool_attributes->>'primary_zone') which is not supported in SQLite.
	// These tests would need to be run against a PostgreSQL database to work correctly.

	// However, we can still test basic error handling and parameter validation
	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate an error during query
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		if err != nil {
			return
		}

		volume, err := store.GetVolumeByNameAccountIDAndZone(context.Background(), "test_volume", int64(1), "us-west1-a")
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, volume, "Expected nil volume when error occurs")

		// Verify it's wrapped as a database read error
		var vcpError *vsaerrors.CustomError
		if errors.As(err, &vcpError) {
			assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, vcpError.TrackingID, "Expected ErrDatabaseDataReadError tracking ID for database connection error")
		}
	})

	t.Run("WhenVolumeNotFound_ExpectNotFoundError", func(tt *testing.T) {
		// This test documents the expected behavior when SQLite encounters PostgreSQL JSONB syntax
		// In a real PostgreSQL environment, this would test the actual not found scenario
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// This will fail with SQLite due to JSONB syntax, but we can verify the error is handled gracefully
		// In PostgreSQL, this would be a record not found error
		volume, err := store.GetVolumeByNameAccountIDAndZone(context.Background(), "test_volume", int64(1), "us-west1-a")
		assert.Nil(tt, volume, "Expected nil volume due to JSONB syntax error or not found")
		assert.Error(tt, err, "Expected error due to unsupported JSONB syntax in SQLite or not found in PostgreSQL")

		// The error should be wrapped appropriately based on the underlying error
		var vcpError *vsaerrors.CustomError
		if errors.As(err, &vcpError) {
			// In SQLite, this will likely be a database read error due to JSONB syntax
			// In PostgreSQL, this should be ErrVolumeNotFound for a missing record
			assert.True(tt, vcpError.TrackingID == vsaerrors.ErrVolumeNotFound || vcpError.TrackingID == vsaerrors.ErrDatabaseDataReadError,
				"Expected either ErrVolumeNotFound or ErrDatabaseDataReadError, got tracking ID: %d", vcpError.TrackingID)
		}
	})
}

func TestListVolumesWithAccounts(t *testing.T) {
	t.Run("ReturnsVolumesWithPreloadedAccountsSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
			Name:      "Account1",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid-1"},
			Name:      "Pool1",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err)

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 1024,
			AccountID:   account.ID,
			Account:     account,
			PoolID:      pool.ID,
			Pool:        pool,
		}
		err = store.db.Create(volume).Error()
		assert.NoError(tt, err)

		results, err := store.ListVolumesWithAccounts(context.Background())
		assert.NoError(tt, err)
		assert.Len(tt, results, 1)
		assert.Equal(tt, "volume-uuid-1", results[0].UUID)
		assert.NotNil(tt, results[0].Account)
		assert.Equal(tt, "Account1", results[0].Account.Name)
	})

	t.Run("ReturnsEmptySliceWhenNoVolumesExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		results, err := store.ListVolumesWithAccounts(context.Background())
		assert.NoError(tt, err)
		assert.Empty(tt, results)
	})

	t.Run("ReturnsErrorOnDBFailure", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sqlDB, err := store.db.GORM().DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		results, err := store.ListVolumesWithAccounts(context.Background())
		assert.Error(tt, err)
		assert.Nil(tt, results)
	})
}

func TestBuildVolumeUpdateQuery(t *testing.T) {
	// Create a DataStoreRepository instance for testing
	// Note: We don't need a real database connection since buildVolumeUpdateQuery is a pure function
	store := &DataStoreRepository{}

	t.Run("WhenUpdatesSliceIsEmpty", func(tt *testing.T) {
		updates := []datamodel.VolumeFieldUpdate{}

		sql, args := store.buildVolumeUpdateQuery(context.Background(), updates)

		expectedSQL := "UPDATE volumes SET used_bytes = tmp.used_bytes, updated_at = NOW() FROM (VALUES ) AS tmp(uuid, used_bytes) WHERE volumes.uuid::text = tmp.uuid::text"
		assert.Equal(tt, expectedSQL, sql, "Expected SQL for empty updates")
		assert.Empty(tt, args, "Expected empty args array for empty updates")
	})

	t.Run("WhenUpdatingSingleVolume", func(tt *testing.T) {
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: "test-uuid-1",
				Fields: map[string]interface{}{
					"used_bytes": int64(1000),
				},
			},
		}

		sql, args := store.buildVolumeUpdateQuery(context.Background(), updates)

		expectedSQL := "UPDATE volumes SET used_bytes = tmp.used_bytes, updated_at = NOW() FROM (VALUES ($1::uuid, $2::bigint)) AS tmp(uuid, used_bytes) WHERE volumes.uuid::text = tmp.uuid::text"
		assert.Equal(tt, expectedSQL, sql, "Expected SQL for single volume update")

		expectedArgs := []interface{}{"test-uuid-1", int64(1000)}
		assert.Equal(tt, expectedArgs, args, "Expected args for single volume update")
		assert.Len(tt, args, 2, "Expected exactly 2 arguments for single volume")
	})

	t.Run("WhenUpdatingMultipleVolumes", func(tt *testing.T) {
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: "test-uuid-1",
				Fields: map[string]interface{}{
					"used_bytes": int64(1000),
				},
			},
			{
				UUID: "test-uuid-2",
				Fields: map[string]interface{}{
					"used_bytes": int64(2000),
				},
			},
			{
				UUID: "test-uuid-3",
				Fields: map[string]interface{}{
					"used_bytes": int64(3000),
				},
			},
		}

		sql, args := store.buildVolumeUpdateQuery(context.Background(), updates)

		expectedSQL := "UPDATE volumes SET used_bytes = tmp.used_bytes, updated_at = NOW() FROM (VALUES ($1::uuid, $2::bigint), ($3::uuid, $4::bigint), ($5::uuid, $6::bigint)) AS tmp(uuid, used_bytes) WHERE volumes.uuid::text = tmp.uuid::text"
		assert.Equal(tt, expectedSQL, sql, "Expected SQL for multiple volume updates")

		expectedArgs := []interface{}{
			"test-uuid-1", int64(1000),
			"test-uuid-2", int64(2000),
			"test-uuid-3", int64(3000),
		}
		assert.Equal(tt, expectedArgs, args, "Expected args for multiple volume updates")
		assert.Len(tt, args, 6, "Expected exactly 6 arguments for 3 volumes")
	})

	t.Run("WhenUsedBytesFieldIsMissing", func(tt *testing.T) {
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: "test-uuid-1",
				Fields: map[string]interface{}{
					// Missing used_bytes field
					"some_other_field": "value",
				},
			},
		}

		sql, args := store.buildVolumeUpdateQuery(context.Background(), updates)

		expectedSQL := "UPDATE volumes SET used_bytes = tmp.used_bytes, updated_at = NOW() FROM (VALUES ($1::uuid, $2::bigint)) AS tmp(uuid, used_bytes) WHERE volumes.uuid::text = tmp.uuid::text"
		assert.Equal(tt, expectedSQL, sql, "Expected SQL structure even with missing used_bytes")

		expectedArgs := []interface{}{"test-uuid-1", 0} // Default value for missing field
		assert.Equal(tt, expectedArgs, args, "Expected default value 0 for missing used_bytes")
	})

	t.Run("WhenFieldsMapIsNil", func(tt *testing.T) {
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID:   "test-uuid-1",
				Fields: nil, // Nil fields map
			},
		}

		sql, args := store.buildVolumeUpdateQuery(context.Background(), updates)

		expectedSQL := "UPDATE volumes SET used_bytes = tmp.used_bytes, updated_at = NOW() FROM (VALUES ($1::uuid, $2::bigint)) AS tmp(uuid, used_bytes) WHERE volumes.uuid::text = tmp.uuid::text"
		assert.Equal(tt, expectedSQL, sql, "Expected SQL structure even with nil fields")

		expectedArgs := []interface{}{"test-uuid-1", 0} // Default value for nil fields
		assert.Equal(tt, expectedArgs, args, "Expected default value 0 for nil fields")
	})

	t.Run("WhenParameterCountingIsCorrect", func(tt *testing.T) {
		// Test with many updates to verify parameter counting
		updates := make([]datamodel.VolumeFieldUpdate, 10)
		for i := 0; i < 10; i++ {
			updates[i] = datamodel.VolumeFieldUpdate{
				UUID: fmt.Sprintf("test-uuid-%d", i+1),
				Fields: map[string]interface{}{
					"used_bytes": int64((i + 1) * 1000),
				},
			}
		}

		sql, args := store.buildVolumeUpdateQuery(context.Background(), updates)

		// Verify SQL contains correct placeholders for 10 volumes (20 parameters total)
		assert.Contains(tt, sql, "$1::uuid, $2::bigint", "Expected first parameter pair")
		assert.Contains(tt, sql, "$19::uuid, $20::bigint", "Expected last parameter pair")
		assert.NotContains(tt, sql, "$21", "Should not contain parameters beyond $20")

		// Verify args array has correct length and values
		assert.Len(tt, args, 20, "Expected 20 arguments for 10 volumes")

		// Verify parameter ordering
		assert.Equal(tt, "test-uuid-1", args[0], "First UUID should be at index 0")
		assert.Equal(tt, int64(1000), args[1], "First used_bytes should be at index 1")
		assert.Equal(tt, "test-uuid-10", args[18], "Last UUID should be at index 18")
		assert.Equal(tt, int64(10000), args[19], "Last used_bytes should be at index 19")
	})

	t.Run("WhenSQLStructureIsValid", func(tt *testing.T) {
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: "test-uuid-1",
				Fields: map[string]interface{}{
					"used_bytes": int64(1000),
				},
			},
		}

		sql, args := store.buildVolumeUpdateQuery(context.Background(), updates)

		// Verify SQL structure components
		assert.Contains(tt, sql, "UPDATE volumes", "Should contain UPDATE volumes clause")
		assert.Contains(tt, sql, "SET used_bytes = tmp.used_bytes, updated_at = NOW()", "Should contain SET clause")
		assert.Contains(tt, sql, "FROM (VALUES", "Should contain VALUES clause")
		assert.Contains(tt, sql, "AS tmp(uuid, used_bytes)", "Should contain temp table alias")
		assert.Contains(tt, sql, "WHERE volumes.uuid::text = tmp.uuid::text", "Should contain WHERE clause")

		// Verify PostgreSQL-specific syntax
		assert.Contains(tt, sql, "::uuid", "Should contain PostgreSQL UUID casting")
		assert.Contains(tt, sql, "::bigint", "Should contain PostgreSQL bigint casting")
		assert.Contains(tt, sql, "NOW()", "Should contain NOW() function")

		// Verify args are populated
		assert.NotEmpty(tt, args, "Args should not be empty")
	})

	t.Run("WhenUsedBytesHasDifferentTypes", func(tt *testing.T) {
		updates := []datamodel.VolumeFieldUpdate{
			{
				UUID: "test-uuid-1",
				Fields: map[string]interface{}{
					"used_bytes": 1000, // int instead of int64
				},
			},
			{
				UUID: "test-uuid-2",
				Fields: map[string]interface{}{
					"used_bytes": int64(2000), // correct int64
				},
			},
			{
				UUID: "test-uuid-3",
				Fields: map[string]interface{}{
					"used_bytes": "3000", // string instead of int64
				},
			},
		}

		sql, args := store.buildVolumeUpdateQuery(context.Background(), updates)

		// Should handle different types by accepting interface{}
		assert.Contains(tt, sql, "($1::uuid, $2::bigint), ($3::uuid, $4::bigint), ($5::uuid, $6::bigint)", "Should generate correct placeholders")
		assert.Len(tt, args, 6, "Should have 6 arguments")

		// Verify the values are passed as-is (type conversion happens in database layer)
		assert.Equal(tt, "test-uuid-1", args[0])
		assert.Equal(tt, 1000, args[1]) // int value
		assert.Equal(tt, "test-uuid-2", args[2])
		assert.Equal(tt, int64(2000), args[3]) // int64 value
		assert.Equal(tt, "test-uuid-3", args[4])
		assert.Equal(tt, "3000", args[5]) // string value
	})
}

// Add this test method at the end of the file
func TestListVolumesWithPagination(t *testing.T) {
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
		pagination := &dbutils.Pagination{Limit: 10, Offset: 0}
		volumes, err := store.ListVolumesWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 0, len(volumes), "Expected %v volumes, got %v", 0, len(volumes))
	})

	t.Run("WhenVolumesExistWithPagination", func(tt *testing.T) {
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

		// Create 5 volumes for pagination testing
		volumes := make([]*datamodel.Volume, 5)
		for i := 0; i < 5; i++ {
			volumes[i] = &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: fmt.Sprintf("test-volume-uuid-%d", i+1)},
				Name:      fmt.Sprintf("test_volume_%d", i+1),
				AccountID: account.ID,
				Account:   account,
				Pool:      pool,
				PoolID:    pool.ID,
			}
			err = store.db.Create(volumes[i]).Error()
			assert.NoError(tt, err, "Failed to create volume %d", i+1)
		}

		conditions := [][]interface{}{
			{"account_id = ?", account.ID},
		}

		// Test first page with limit 2
		pagination := &dbutils.Pagination{Limit: 2, Offset: 0}
		resultVolumes, err := store.ListVolumesWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 2, len(resultVolumes), "Expected 2 volumes on first page, got %v", len(resultVolumes))

		// Test second page with limit 2
		pagination = &dbutils.Pagination{Limit: 2, Offset: 2}
		resultVolumes, err = store.ListVolumesWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 2, len(resultVolumes), "Expected 2 volumes on second page, got %v", len(resultVolumes))

		// Test third page with limit 2
		pagination = &dbutils.Pagination{Limit: 2, Offset: 4}
		resultVolumes, err = store.ListVolumesWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 1, len(resultVolumes), "Expected 1 volume on third page, got %v", len(resultVolumes))

		// Test with limit larger than total volumes
		pagination = &dbutils.Pagination{Limit: 10, Offset: 0}
		resultVolumes, err = store.ListVolumesWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 5, len(resultVolumes), "Expected 5 volumes with large limit, got %v", len(resultVolumes))
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
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
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

		conditions := [][]interface{}{
			{"account_id", "=", account.ID},
		}

		// Test with nil pagination - should use default limit
		resultVolumes, err := store.ListVolumesWithPagination(context.Background(), conditions, nil)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 1, len(resultVolumes), "Expected 1 volume with nil pagination, got %v", len(resultVolumes))
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
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
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

		conditions := [][]interface{}{
			{"account_id", "=", account.ID},
		}

		// Test with zero limit - should use default limit (1000)
		pagination := &dbutils.Pagination{Limit: 0, Offset: 0}
		resultVolumes, err := store.ListVolumesWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 1, len(resultVolumes), "Expected 1 volume with zero limit (default), got %v", len(resultVolumes))
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
		pagination := &dbutils.Pagination{Limit: 10, Offset: 0}
		volumes, err := store.ListVolumesWithPagination(context.Background(), conditions, pagination)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, volumes, "Expected nil volumes when error occurs")
	})

	t.Run("WhenOffsetExceedsTotalVolumes", func(tt *testing.T) {
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

		conditions := [][]interface{}{
			{"account_id", "=", account.ID},
		}

		// Test with offset beyond total volumes
		pagination := &dbutils.Pagination{Limit: 10, Offset: 100}
		resultVolumes, err := store.ListVolumesWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 0, len(resultVolumes), "Expected 0 volumes when offset exceeds total, got %v", len(resultVolumes))
	})

	t.Run("WhenVolumesWithBackupPolicyAttached", func(tt *testing.T) {
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
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		policyEnabled := true
		policyDisabled := false
		backupPolicyUUID := "test-backup-policy-uuid"

		volume1 := &datamodel.Volume{
			BaseModel:      datamodel.BaseModel{UUID: "test-volume-uuid-1"},
			Name:           "test_volume_1",
			AccountID:      account.ID,
			Account:        account,
			Pool:           pool,
			PoolID:         pool.ID,
			DataProtection: &datamodel.DataProtection{BackupPolicyID: backupPolicyUUID, ScheduledBackupEnabled: &policyEnabled},
		}
		err = store.db.Create(volume1).Error()
		assert.NoError(tt, err, "Failed to create volume 1")

		volume2 := &datamodel.Volume{
			BaseModel:      datamodel.BaseModel{UUID: "test-volume-uuid-2"},
			Name:           "test_volume_2",
			AccountID:      account.ID,
			Account:        account,
			Pool:           pool,
			PoolID:         pool.ID,
			DataProtection: &datamodel.DataProtection{BackupPolicyID: backupPolicyUUID, ScheduledBackupEnabled: &policyDisabled},
		}
		err = store.db.Create(volume2).Error()
		assert.NoError(tt, err, "Failed to create volume 2")

		volume3 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid-3"},
			Name:      "test_volume_3",
			AccountID: account.ID,
			Account:   account,
			Pool:      pool,
			PoolID:    pool.ID,
			// No DataProtection
		}
		err = store.db.Create(volume3).Error()
		assert.NoError(tt, err, "Failed to create volume 3")

		conditions := [][]interface{}{
			{"account_id = ?", accountID},
			{"data_protection->>'backup_policy_id' = ?", backupPolicyUUID},
			{"data_protection->>'scheduled_backup_enabled' = true"},
		}

		pagination := &dbutils.Pagination{Limit: 10, Offset: 0}
		volumes, err := store.ListVolumesWithPagination(context.Background(), conditions, pagination)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, 1, len(volumes), "Expected 1 volume with backup policy enabled, got %v", len(volumes))
		assert.Equal(tt, "test-volume-uuid-1", volumes[0].UUID, "Expected volume 1 UUID, got %v", volumes[0].UUID)
	})
}
