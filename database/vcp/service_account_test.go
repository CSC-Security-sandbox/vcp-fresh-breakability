package database

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestUpdateServiceAccountEmailAndKey(t *testing.T) {
	t.Run("UpdateServiceAccountEmailAndKeyUpdatesFieldsOnSuccess", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err)

		sa := &datamodel.ServiceAccount{
			BaseModel:                      datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail:            "old@email.com",
			ServiceAccountPasswordLocation: "old-location",
			State:                          models.AccountStateDisabled,
			StateDetails:                   models.LifeCycleStateCreatingDetails,
		}
		err = store.db.Create(sa).Error()
		assert.NoError(t, err)

		result, err := store.UpdateServiceAccountEmailAndKey(context.Background(), "sa-uuid", "new@email.com", "new-key")
		assert.NoError(t, err)
		assert.Equal(t, "new@email.com", result.ServiceAccountEmail)
		assert.NotEqual(t, "old-location", result.ServiceAccountPasswordLocation)
		assert.Equal(t, models.AccountStateDisabled, result.State)
		assert.Equal(t, models.LifeCycleStateCreatingDetails, result.StateDetails)
	})
	t.Run("UpdateServiceAccountEmailAndKeyReturnsErrorIfAccountNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err)

		_, err = store.UpdateServiceAccountEmailAndKey(context.Background(), "nonexistent-uuid", "email@email.com", "key")
		assert.ErrorContains(t, err, "record not found")
	})
	t.Run("UpdateServiceAccountEmailAndKeyReturnsErrorOnEncryptFailure", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(t, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(t, err)

		sa := &datamodel.ServiceAccount{
			BaseModel: datamodel.BaseModel{UUID: "sa-uuid"},
		}
		err = store.db.Create(sa).Error()
		assert.NoError(t, err)

		origEncryptPassword := utils.EncryptPassword
		utils.EncryptPassword = func(_ log.Secret) (*string, error) {
			return nil, fmt.Errorf("encryption error")
		}
		defer func() { utils.EncryptPassword = origEncryptPassword }()

		_, err = store.UpdateServiceAccountEmailAndKey(context.Background(), "sa-uuid", "email@email.com", "key")
		assert.ErrorContains(t, err, "encryption error")
	})
}

func TestGetGcpKmsServiceAccountFromEmail(t *testing.T) {
	t.Run("ReturnsServiceAccountOnSuccess", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sa := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail: "test@email.com",
		}
		err = store.db.Create(sa).Error()
		assert.NoError(tt, err)

		result, err := store.GetServiceAccountFromEmail(context.Background(), "test@email.com")
		assert.NoError(tt, err)
		assert.Equal(tt, "test@email.com", result.ServiceAccountEmail)
	})

	t.Run("ReturnsNotFoundErrorIfNoAccount", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		_, err = store.GetServiceAccountFromEmail(context.Background(), "notfound@email.com")
		assert.Error(tt, err)
		assert.Contains(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "not found")
	})
}

func TestDeleteServiceAccount(t *testing.T) {
	t.Run("DeletesServiceAccountAndUpdatesState", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sa := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail: "test@email.com",
			State:               models.AccountStateEnabled,
			StateDetails:        "active",
		}
		err = store.db.Create(sa).Error()
		assert.NoError(tt, err)

		err = store.DeleteServiceAccount(context.Background(), sa)
		assert.NoError(tt, err)

		var updated datamodel.ServiceAccount
		err = store.db.Where("uuid = ?", "sa-uuid").First(&updated).Error()
		assert.NoError(tt, err)
		assert.Equal(tt, models.LifeCycleStateDisabled, updated.State)
		assert.Equal(tt, models.LifeCycleStateDisabledDetails, updated.StateDetails)
	})
}

func TestAddKeyToServiceAccount(t *testing.T) {
	t.Run("AddKeyToServiceAccount_Success", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sa := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail: "test@email.com",
		}
		err = store.db.Create(sa).Error()
		assert.NoError(tt, err)

		newKey := datamodel.ServiceAccountKey{
			KeyID:     "new-key-id-123",
			KeyData:   "encrypted-key-data",
			IsPrimary: false,
			IsActive:  true,
		}

		err = store.AddKeyToServiceAccount(context.Background(), "sa-uuid", newKey)
		assert.NoError(tt, err)

		var updated datamodel.ServiceAccount
		err = store.db.Where("uuid = ?", "sa-uuid").First(&updated).Error()
		assert.NoError(tt, err)
		assert.NotNil(tt, updated.ServiceAccountAttributes)
		assert.Len(tt, updated.ServiceAccountAttributes.Keys, 1)
		assert.Equal(tt, "new-key-id-123", updated.ServiceAccountAttributes.Keys[0].KeyID)
	})

	t.Run("AddKeyToServiceAccount_InitializesNilAttributes", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sa := &datamodel.ServiceAccount{
			BaseModel:                datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail:      "test@email.com",
			ServiceAccountAttributes: nil, // Explicitly nil
		}
		err = store.db.Create(sa).Error()
		assert.NoError(tt, err)

		newKey := datamodel.ServiceAccountKey{
			KeyID:     "new-key-id-456",
			KeyData:   "encrypted-key-data",
			IsPrimary: false,
			IsActive:  true,
		}

		err = store.AddKeyToServiceAccount(context.Background(), "sa-uuid", newKey)
		assert.NoError(tt, err)

		var updated datamodel.ServiceAccount
		err = store.db.Where("uuid = ?", "sa-uuid").First(&updated).Error()
		assert.NoError(tt, err)
		assert.NotNil(tt, updated.ServiceAccountAttributes)
		assert.Len(tt, updated.ServiceAccountAttributes.Keys, 1)
		assert.Equal(tt, "new-key-id-456", updated.ServiceAccountAttributes.Keys[0].KeyID)
	})

	t.Run("AddKeyToServiceAccount_ServiceAccountNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		newKey := datamodel.ServiceAccountKey{
			KeyID:   "new-key-id-123",
			KeyData: "encrypted-key-data",
		}

		err = store.AddKeyToServiceAccount(context.Background(), "nonexistent-uuid", newKey)
		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, customErr.TrackingID)
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
	})
}

func TestRemoveKeyFromServiceAccount(t *testing.T) {
	t.Run("RemoveKeyFromServiceAccount_Success", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sa := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail: "test@email.com",
			ServiceAccountAttributes: &datamodel.ServiceAccountAttributes{
				Keys: []datamodel.ServiceAccountKey{
					{KeyID: "key-1", KeyData: "data-1", IsActive: true},
					{KeyID: "key-2", KeyData: "data-2", IsActive: true},
				},
			},
		}
		err = store.db.Create(sa).Error()
		assert.NoError(tt, err)

		err = store.RemoveKeyFromServiceAccount(context.Background(), "sa-uuid", "key-1")
		assert.NoError(tt, err)

		var updated datamodel.ServiceAccount
		err = store.db.Where("uuid = ?", "sa-uuid").First(&updated).Error()
		assert.NoError(tt, err)
		assert.Len(tt, updated.ServiceAccountAttributes.Keys, 1)
		assert.Equal(tt, "key-2", updated.ServiceAccountAttributes.Keys[0].KeyID)
	})

	t.Run("RemoveKeyFromServiceAccount_ServiceAccountNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		err = store.RemoveKeyFromServiceAccount(context.Background(), "nonexistent-uuid", "key-1")
		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, customErr.TrackingID)
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
	})

	t.Run("RemoveKeyFromServiceAccount_KeyNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sa := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail: "test@email.com",
			ServiceAccountAttributes: &datamodel.ServiceAccountAttributes{
				Keys: []datamodel.ServiceAccountKey{
					{KeyID: "key-1", KeyData: "data-1", IsActive: true},
				},
			},
		}
		err = store.db.Create(sa).Error()
		assert.NoError(tt, err)

		err = store.RemoveKeyFromServiceAccount(context.Background(), "sa-uuid", "nonexistent-key")
		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, vsaerrors.ErrResourceNotFound, customErr.TrackingID)
			assert.Contains(tt, customErr.OriginalErr.Error(), "key not found")
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
	})
}

func TestSetPrimaryKeyForServiceAccount(t *testing.T) {
	t.Run("SetPrimaryKeyForServiceAccount_Success", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sa := &datamodel.ServiceAccount{
			BaseModel:                      datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail:            "test@email.com",
			ServiceAccountPasswordLocation: "old-primary-key-data",
			ServiceAccountAttributes: &datamodel.ServiceAccountAttributes{
				Keys: []datamodel.ServiceAccountKey{
					{KeyID: "key-1", KeyData: "data-1", IsPrimary: true, IsActive: true},
					{KeyID: "key-2", KeyData: "new-primary-key-data", IsPrimary: false, IsActive: true},
				},
			},
		}
		err = store.db.Create(sa).Error()
		assert.NoError(tt, err)

		err = store.SetPrimaryKeyForServiceAccount(context.Background(), "sa-uuid", "key-2")
		assert.NoError(tt, err)

		var updated datamodel.ServiceAccount
		err = store.db.Where("uuid = ?", "sa-uuid").First(&updated).Error()
		assert.NoError(tt, err)
		assert.Equal(tt, "new-primary-key-data", updated.ServiceAccountPasswordLocation)
		assert.False(tt, updated.ServiceAccountAttributes.Keys[0].IsPrimary)
		assert.True(tt, updated.ServiceAccountAttributes.Keys[1].IsPrimary)
	})

	t.Run("SetPrimaryKeyForServiceAccount_ServiceAccountNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		err = store.SetPrimaryKeyForServiceAccount(context.Background(), "nonexistent-uuid", "key-1")
		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, customErr.TrackingID)
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
	})

	t.Run("SetPrimaryKeyForServiceAccount_KeyNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sa := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail: "test@email.com",
			ServiceAccountAttributes: &datamodel.ServiceAccountAttributes{
				Keys: []datamodel.ServiceAccountKey{
					{KeyID: "key-1", KeyData: "data-1", IsPrimary: true, IsActive: true},
				},
			},
		}
		err = store.db.Create(sa).Error()
		assert.NoError(tt, err)

		err = store.SetPrimaryKeyForServiceAccount(context.Background(), "sa-uuid", "nonexistent-key")
		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, vsaerrors.ErrResourceNotFound, customErr.TrackingID)
			assert.Contains(tt, customErr.OriginalErr.Error(), "key not found")
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
	})
}

func TestGetServiceAccountWithKeys(t *testing.T) {
	t.Run("GetServiceAccountWithKeys_Success", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sa := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail: "test@email.com",
			ServiceAccountAttributes: &datamodel.ServiceAccountAttributes{
				Keys: []datamodel.ServiceAccountKey{
					{KeyID: "key-1", KeyData: "data-1", IsPrimary: true, IsActive: true},
					{KeyID: "key-2", KeyData: "data-2", IsPrimary: false, IsActive: true},
				},
			},
		}
		err = store.db.Create(sa).Error()
		assert.NoError(tt, err)

		result, err := store.GetServiceAccountWithKeys(context.Background(), "sa-uuid")
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "sa-uuid", result.UUID)
		assert.NotNil(tt, result.ServiceAccountAttributes)
		assert.Len(tt, result.ServiceAccountAttributes.Keys, 2)
	})

	t.Run("GetServiceAccountWithKeys_ServiceAccountNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		result, err := store.GetServiceAccountWithKeys(context.Background(), "nonexistent-uuid")
		assert.Error(tt, err)
		assert.Nil(tt, result)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, customErr.TrackingID)
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
	})
}

func TestUpdateServiceAccountPasswordLocation(t *testing.T) {
	t.Run("UpdateServiceAccountPasswordLocation_Success", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		sa := &datamodel.ServiceAccount{
			BaseModel:                      datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail:            "test@email.com",
			ServiceAccountPasswordLocation: "old-encrypted-key",
		}
		err = store.db.Create(sa).Error()
		assert.NoError(tt, err)

		err = store.UpdateServiceAccountPasswordLocation(context.Background(), "sa-uuid", "new-encrypted-key")
		assert.NoError(tt, err)

		var updated datamodel.ServiceAccount
		err = store.db.Where("uuid = ?", "sa-uuid").First(&updated).Error()
		assert.NoError(tt, err)
		assert.Equal(tt, "new-encrypted-key", updated.ServiceAccountPasswordLocation)
	})

	t.Run("UpdateServiceAccountPasswordLocation_NotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err)

		err = store.UpdateServiceAccountPasswordLocation(context.Background(), "missing-uuid", "new-encrypted-key")
		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, vsaerrors.ErrDatabaseDataReadError, customErr.TrackingID)
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
	})
}
