package repository

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
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
		assert.Contains(tt, err.Error(), "not found")
	})
}
