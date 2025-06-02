package repository

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

func TestGetKmsConfig(t *testing.T) {
	t.Run("WhenKmsConfigExists", func(tt *testing.T) {
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

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
			Name:      "test",
			AccountID: account.ID,
		}
		err = store.db.Create(account).Error()
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}
		err = store.db.Create(kmsConfig).Error()
		if err != nil {
			tt.Fatalf("Failed to create kms config: %v", err)
		}

		result, err := store.GetKmsConfig(context.Background(), "test-uuid")
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}
		if result.Name != kmsConfig.Name {
			tt.Errorf("Expected kms config name %v, got %v", kmsConfig.Name, result.Name)
		}
		if result.AccountID != account.ID {
			tt.Errorf("Expected account name %v, got %v", account.ID, result.AccountID)
		}
	})

	t.Run("WhenKmsConfigDoesNotExist", func(tt *testing.T) {
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

		_, err = store.GetKmsConfig(context.Background(), "test-uuid")
		if err == nil {
			tt.Errorf("Expected error, got nil")
		}
		if !customerrors.IsNotFoundErr(err) {
			tt.Errorf("Expected error %v, got %v", gorm.ErrRecordNotFound, err)
		}
	})
}

func TestUpdateUpdateKmsConfigState(t *testing.T) {
	t.Run("WhenUpdateKmsConfigStateIsUpdatedSuccessfully", func(tt *testing.T) {
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

		kmsConfig := &datamodel.KmsConfig{
			Name:      "test_kms_config",
			AccountID: account.ID,
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
		}

		err = store.db.Create(kmsConfig).Error()
		if err != nil {
			tt.Fatalf("Failed to create kms config: %v", err)
		}

		_, err = store.UpdateKmsConfigState(context.Background(), "test-uuid", models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		updatedkms, err1 := store.GetKmsConfig(context.Background(), "test-uuid")
		assert.NoError(tt, err1, "Expected no error, got %v", err1)
		assert.Equal(tt, models.LifeCycleStateUpdating, updatedkms.State, "Expected volume state %v, got %v", models.LifeCycleStateUpdating, updatedkms.State)
	})
	t.Run("WhenUpdateKmsConfigIsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		kms := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "dummy"},
			Name:      "test_volume_rep",
			State:     models.LifeCycleStateUpdating,
		}
		_, err = store.UpdateKmsConfigState(context.Background(), kms.UUID, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)
		assert.EqualError(tt, err, "KMS Configuration not found", "Expected no error, got %v", err)
	})
}
