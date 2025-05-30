package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
)

func TestGetMultipleKMSConfigs(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	serviceAccounts := []*datamodel.ServiceAccount{
		{BaseModel: datamodel.BaseModel{ID: int64(111), UUID: "uuid10"}, Name: "ServiceAccount1"},
		{BaseModel: datamodel.BaseModel{ID: int64(222), UUID: "uuid20"}, Name: "ServiceAccount2"},
	}
	kmsConfigs := []*datamodel.KmsConfig{
		{BaseModel: datamodel.BaseModel{UUID: "uuid1", DeletedAt: nil}, Name: "kmsConfig1", ServiceAccountID: serviceAccounts[0].ID},
		{BaseModel: datamodel.BaseModel{UUID: "uuid2", DeletedAt: nil}, Name: "kmsConfig2", ServiceAccountID: serviceAccounts[1].ID},
	}

	err = store.db.Create(serviceAccounts).Error()
	assert.NoError(t, err, "Failed to create Service account table")
	err = store.db.Create(kmsConfigs).Error()
	assert.NoError(t, err, "Failed to create KMS Config table")

	t.Run("RetrievesKMSConfigsSuccessfully", func(tt *testing.T) {
		kmsConfigUUIDList := []string{"uuid1", "uuid2"}
		conditions := [][]interface{}{{"uuid in ?", kmsConfigUUIDList}}
		result, err := store.GetMultipleKmsConfigs(context.Background(), conditions)

		assert.NoError(tt, err)
		assert.Equal(tt, "kmsConfig1", result[0].Name)
		assert.Equal(tt, "kmsConfig2", result[1].Name)
		assert.Equal(tt, "ServiceAccount1", result[0].ServiceAccount.Name)
		assert.Equal(tt, "ServiceAccount2", result[1].ServiceAccount.Name)
	})
	t.Run("ReturnsEmptyWhenRecordsAreNotFound", func(tt *testing.T) {
		kmsConfigUUIDList := []string{"nonexistent-uuid"}
		conditions := [][]interface{}{{"uuid in ?", kmsConfigUUIDList}}
		result, err := store.GetMultipleKmsConfigs(context.Background(), conditions)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})
	t.Run("HandlesEmptyUUIDListGracefully", func(tt *testing.T) {
		kmsConfigUUIDList := []string{}
		conditions := [][]interface{}{{"uuid in ?", kmsConfigUUIDList}}
		result, err := store.GetMultipleKmsConfigs(context.Background(), conditions)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})
}

func TestGetMultipleKMSConfigsDBErrorCondition(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err, "Failed to set up test database")
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err, "Failed to clean up test database")

	t.Run("RetrievesKMSConfigsSuccessfully", func(tt *testing.T) {
		dbErr := db.Migrator().DropTable(&datamodel.KmsConfig{})
		if dbErr != nil {
			assert.Fail(tt, "Dropping table KmsConfig from in-memory DB failed; aborting test")
		}
		kmsConfigUUIDList := []string{"uuid1", "uuid2"}
		conditions := [][]interface{}{{"uuid in ?", kmsConfigUUIDList}}
		result, kmsErr := store.GetMultipleKmsConfigs(context.Background(), conditions)

		assert.Error(tt, kmsErr)
		assert.Nil(tt, result)
	})
}
