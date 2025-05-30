package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"gorm.io/gorm"
)

func TestGetSvmsByPoolID(t *testing.T) {
	t.Run("WhenSvmExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:   "test_svm",
			PoolID: 1234,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		result, err := store.GetSvmsByPoolID(context.Background(), svm.PoolID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, svm.Name, result[0].Name, "Expected svm name %v, got %v", svm.Name, result[0].Name)
	})
	t.Run("WhenSvmDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		result, err := store.GetSvmsByPoolID(context.Background(), 12)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, result, "Expected result to be empty, but got %v", result)
	})
}

func TestCreateSVM(t *testing.T) {
	t.Run("WhenSvmIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
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
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "test-svm-uuid",
			},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}

		createdSvm, err := store.CreateSVM(context.Background(), svm)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, svm.Name, createdSvm.Name, "Expected svm name %v, got %v", svm.Name, createdSvm.Name)
	})
	t.Run("WhenSvmAlreadyExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
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
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "test-svm-uuid",
			},
			Name:   "test_svm",
			PoolID: 1234,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		_, err = store.CreateSVM(context.Background(), svm)
		var customErr *vsaerrors.CustomError
		if errors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "svm already exists")
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
	})
}

func TestDeleteSVM(t *testing.T) {
	t.Run("WhenSvmIsDeletedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
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
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "test-svm-uuid",
			},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		err = store.DeleteSVM(context.Background(), svm)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		deletedSvm := &datamodel.Svm{}
		err = store.db.GORM().First(deletedSvm, "uuid = ?", svm.UUID).Error
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			tt.Errorf("Expected record not found error, got %v", err)
		}
	})
}

func TestDeletingSVM(t *testing.T) {
	t.Run("UpdatesSvmStateToDeletingSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
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
			BaseModel: datamodel.BaseModel{ID: 1234},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "test-svm-uuid",
			},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		err = store.DeletingSVM(context.Background(), svm)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		updatedSvm := &datamodel.Svm{}
		err = store.db.GORM().First(updatedSvm, "uuid = ?", svm.UUID).Error
		if err != nil {
			tt.Fatalf("Failed to fetch updated svm: %v", err)
		}
		if updatedSvm.State != models.LifeCycleStateDeleting {
			tt.Errorf("Expected state %v, got %v", models.LifeCycleStateDeleting, updatedSvm.State)
		}
		if updatedSvm.StateDetails != models.LifeCycleStateDeletingDetails {
			tt.Errorf("Expected state details %v, got %v", models.LifeCycleStateDeletingDetails, updatedSvm.StateDetails)
		}
	})
}

func TestGetSvmForPoolID(t *testing.T) {
	t.Run("WhenSvmExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:   "test_svm",
			PoolID: 1234,
		}
		err = store.db.Create(svm).Error()
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		result, err := store.GetSvmForPoolID(context.Background(), svm.PoolID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, svm.Name, result.Name, "Expected svm name %v, got %v", svm.Name, result.Name)
		assert.Equal(tt, svm.PoolID, result.PoolID, "Expected svm pool id %v, got %v", svm.PoolID, result.PoolID)
	})
}
