package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
)

func TestErroredResource(t *testing.T) {
	t.Run("UpdatesPoolStateToErrorSuccessfully", func(tt *testing.T) {
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

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
			State:     models.LifeCycleStateCreating,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		errorMsg := "custom error message"
		res, err := store.ErroredResource(context.Background(), pool, errorMsg)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		updatedPool := res.(*datamodel.Pool)
		if updatedPool.State != models.LifeCycleStateError {
			tt.Errorf("Expected state %v, got %v", models.LifeCycleStateError, updatedPool.State)
		}
		if updatedPool.StateDetails != errorMsg {
			tt.Errorf("Expected state details %v, got %v", errorMsg, updatedPool.StateDetails)
		}
	})

	t.Run("UpdatesVolumeStateToErrorSuccessfully", func(tt *testing.T) {
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

		pool := &datamodel.Pool{
			BaseModel:    datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:         "test_pool",
			AccountID:    account.ID,
			Account:      account,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateReadyDetails,
		}
		err = store.db.Create(pool).Error()
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:         "test_volume",
			AccountID:    account.ID,
			Account:      account,
			PoolID:       pool.ID,
			Pool:         pool,
			State:        models.LifeCycleStateCreating,
			StateDetails: models.LifeCycleStateCreatingDetails,
		}
		err = store.db.Create(volume).Error()
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		errorMsg := "custom error message"
		res, err := store.ErroredResource(context.Background(), volume, errorMsg)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		updatedVolume := res.(*datamodel.Volume)
		if updatedVolume.State != models.LifeCycleStateError {
			tt.Errorf("Expected state %v, got %v", models.LifeCycleStateError, updatedVolume.State)
		}
		if updatedVolume.StateDetails != errorMsg {
			tt.Errorf("Expected state details %v, got %v", errorMsg, updatedVolume.StateDetails)
		}
	})

	t.Run("ErrorsWhenNonPointerIsPassed", func(tt *testing.T) {
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

		errorMsg := "custom error message"
		res, err := store.ErroredResource(context.Background(), "non-struct-value", errorMsg)
		assert.Error(tt, err)
		assert.Equal(tt, err.Error(), "resource must be a pointer to a struct")
		assert.Nil(t, res)
	})

	t.Run("ErrorsWhenNonStructIsPassed", func(tt *testing.T) {
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

		errorMsg := "custom error message"
		resource := "not-a-struct"
		res, err := store.ErroredResource(context.Background(), &resource, errorMsg)
		assert.Error(tt, err)
		assert.Equal(tt, err.Error(), "invalid resource type for ErroredResource method, expected a struct")
		assert.Nil(t, res)
	})

	t.Run("ErrorsWhenStructWithNoStateIsPassed", func(tt *testing.T) {
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

		errorMsg := "custom error message"
		resource := struct{ name string }{"John Doe"}
		res, err := store.ErroredResource(context.Background(), &resource, errorMsg)
		assert.Error(tt, err)
		assert.Equal(tt, err.Error(), "State field not found in the errored resource")
		assert.Nil(t, res)
	})

	t.Run("ErrorsWhenStructWithNoStateDetailsIsPassed", func(tt *testing.T) {
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

		errorMsg := "custom error message"
		resource := struct{ State string }{models.LifeCycleStateError}
		res, err := store.ErroredResource(context.Background(), &resource, errorMsg)
		assert.Error(tt, err)
		assert.Equal(tt, err.Error(), "StateDetails field not found in the errored resource")
		assert.Nil(t, res)
	})
}
