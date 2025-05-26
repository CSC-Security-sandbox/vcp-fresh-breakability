package repository

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
)

func TestCreateHostGroup(t *testing.T) {
	t.Run("WhenValidParams", func(tt *testing.T) {
		ctx := context.Background()
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		hostGroup := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "test-hostgroup-uuid"},
			Name:      "test_hostgroup",
			AccountID: account.ID,
		}

		_, err = store.CreateHostGroup(ctx, hostGroup)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		result, _ := store.GetHostGroup(ctx, hostGroup.UUID, account.ID)
		assert.Equal(tt, hostGroup.Name, result.Name)
		assert.NotEmpty(tt, result.UUID)
	})
	t.Run("WithDuplicateName", func(tt *testing.T) {
		ctx := context.Background()
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		hostGroup := &datamodel.HostGroup{
			Name:      "duplicate_hostgroup",
			AccountID: account.ID,
		}
		_, err = store.CreateHostGroup(ctx, hostGroup)
		assert.NoError(tt, err, "Failed to create host group")

		_, err = store.CreateHostGroup(ctx, hostGroup)
		assert.EqualError(tt, err, "hostgroup already exists")
	})
}

func TestGetHostGroupWithDetails(t *testing.T) {
	t.Run("WhenHostGroupExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hostGroup := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-hostgroup-uuid",
			},
			AccountID: 1,
		}
		err = wrapper.Create(hostGroup).Error()
		assert.NoError(tt, err, "Failed to create host group")

		result, err := getHostGroupWithDetails(wrapper.GORM(), hostGroup)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, hostGroup.UUID, result.UUID, "Expected UUID %v, got %v", hostGroup.UUID, result.UUID)
	})
	t.Run("WhenHostGroupDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hostGroup := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				UUID: "non-existent-hostgroup-uuid",
			},
			AccountID: 1,
		}

		_, err = getHostGroupWithDetails(wrapper.GORM(), hostGroup)
		assert.EqualError(tt, err, fmt.Sprintf("host group '%s' not found", hostGroup.UUID))
	})
}

func TestGetMultipleHostGroups(t *testing.T) {
	t.Run("WithValidUUIDs", func(tt *testing.T) {
		ctx := context.Background()
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		hostGroup1 := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "test-hostgroup-uuid1"},
			Name:      "hostgroup1",
			AccountID: account.ID,
		}
		hostGroup2 := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "test-hostgroup-uuid2"},
			Name:      "hostgroup2",
			AccountID: account.ID,
		}
		err = store.db.Create(hostGroup1).Error()
		assert.NoError(tt, err, "Failed to create host group 1")
		err = store.db.Create(hostGroup2).Error()
		assert.NoError(tt, err, "Failed to create host group 2")

		result, err := store.GetMultipleHostGroups(ctx, []string{"test-hostgroup-uuid1", "test-hostgroup-uuid2"}, account.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, result, 2)
		assert.Equal(tt, hostGroup1.UUID, result[0].UUID)
		assert.Equal(tt, hostGroup2.UUID, result[1].UUID)
	})
	t.Run("WithNonExistentUUIDs", func(tt *testing.T) {
		ctx := context.Background()
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		result, err := store.GetMultipleHostGroups(ctx, []string{"non-existent-uuid1", "non-existent-uuid2"}, account.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, result)
	})
	t.Run("WithInvalidAccountID", func(tt *testing.T) {
		ctx := context.Background()
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		result, _ := store.GetMultipleHostGroups(ctx, []string{"test-hostgroup-uuid1"}, 999) // Invalid account ID
		assert.Empty(tt, result)
	})
	t.Run("WithFindError", func(tt *testing.T) {
		ctx := context.Background()
		db, err := SetupTestDB()
		raiseErrorFlag := db.Statement.RaiseErrorOnNotFound
		db.Statement.RaiseErrorOnNotFound = true
		defer func() {
			db.Statement.RaiseErrorOnNotFound = raiseErrorFlag
		}()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, err = store.GetMultipleHostGroups(ctx, []string{"test-hostgroup-uuid1"}, 999) // Invalid account ID
		assert.Error(tt, err, "record not found")
	})
}
