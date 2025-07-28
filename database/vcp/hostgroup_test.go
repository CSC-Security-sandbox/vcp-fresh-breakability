package database

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

func TestGetHostGroup(t *testing.T) {
	t.Run("WhenHostGroupNotExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		result, err := store.GetHostGroup(context.Background(), "hg", 1)
		assert.EqualError(tt, err, "host group not found")
		assert.Nil(tt, result, "Expected result to be nil")
	})
	t.Run("WhenHostGroupExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-hg",
			},
			Name:      "test_hg",
			AccountID: 1,
		}
		err = store.db.Create(hg).Error()
		if err != nil {
			tt.Fatalf("Failed to create hg: %v", err)
		}

		result, err := store.GetHostGroup(context.Background(), "test-hg", 1)
		assert.NoError(tt, err, "Failed to get host group")
		assert.NotNil(tt, result, "Expected result to be not nil")
	})
	t.Run("WhenHostGroupExistsWithDifferentAccount", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-hg",
			},
			Name:      "test_hg",
			AccountID: 2,
		}
		err = store.db.Create(hg).Error()
		if err != nil {
			tt.Fatalf("Failed to create hg: %v", err)
		}

		result, err := store.GetHostGroup(context.Background(), "test-hg", 1)
		assert.EqualError(tt, err, "host group not found")
		assert.Nil(tt, result, "Expected result to be nil")
	})
}

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
		result, err := store.GetHostGroup(ctx, hostGroup.UUID, account.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
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
		err = store.db.Create(hostGroup).Error()
		assert.NoError(tt, err, "Failed to create hostGroup")

		_, err = store.CreateHostGroup(ctx, hostGroup)
		assert.EqualError(tt, err, "hostgroup already exists")
	})
	t.Run("WithHostGroupReturnsError", func(tt *testing.T) {
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
		err = store.db.Create(hostGroup).Error()
		assert.NoError(tt, err, "Failed to create hostGroup")

		defer func() {
			getHostGroupWithDetails = _getHostGroupWithDetails
		}()

		getHostGroupWithDetails = func(db *gorm.DB, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error) {
			return nil, customerrors.New("some error occurred")
		}

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
		assert.EqualError(tt, err, "host group not found")
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

func TestDeleteHostGroup(t *testing.T) {
	t.Run("WhenHostGroupNotExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		store := NewDataStoreRepository(gormwrapper.New(db))

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		getHostGroupWithDetails = func(db *gorm.DB, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error) {
			return nil, customerrors.NewNotFoundErr("host group", nil)
		}

		defer func() { getHostGroupWithDetails = _getHostGroupWithDetails }()
		result, err := store.DeleteHostGroup(context.Background(), "hg1", 1)
		assert.EqualError(tt, err, "host group not found")
		assert.Nil(tt, result, "Expected result to be nil")
	})
	t.Run("WhenHostGroupExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-hg1",
			},
			Name:      "test_hg",
			AccountID: 1,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(hg).Error()
		if err != nil {
			tt.Fatalf("Failed to create hg: %v", err)
		}

		getHostGroupWithDetails = func(db *gorm.DB, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error) {
			return hg, nil
		}
		isHostGroupInUse = func(db *gorm.DB, hostGroupUUID string, accountID int64) (bool, error) {
			return false, nil
		}

		defer func() {
			getHostGroupWithDetails = _getHostGroupWithDetails
			isHostGroupInUse = _isHostGroupInUse
		}()

		result, err := store.DeleteHostGroup(context.Background(), "test-hg1", 1)
		assert.NoError(tt, err, "Failed to get host group")
		assert.Equal(tt, result.State, models.LifeCycleStateDeleted, "Expected result to be nil")
	})
	t.Run("WhenHostGroupExistsAndInUse", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-hg1",
			},
			Name:      "test_hg",
			AccountID: 1,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(hg).Error()
		if err != nil {
			tt.Fatalf("Failed to create hg: %v", err)
		}

		getHostGroupWithDetails = func(db *gorm.DB, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error) {
			return hg, nil
		}
		isHostGroupInUse = func(db *gorm.DB, hostGroupUUID string, accountID int64) (bool, error) {
			return true, nil
		}

		defer func() {
			getHostGroupWithDetails = _getHostGroupWithDetails
			isHostGroupInUse = _isHostGroupInUse
		}()

		result, err := store.DeleteHostGroup(context.Background(), "test-hg1", 1)
		assert.EqualError(tt, err, "host group is in use by one or more volumes")
		assert.Nil(tt, result)
	})
	t.Run("WhenHostGroupExistsAndInUseReturnsError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-hg1",
			},
			Name:      "test_hg",
			AccountID: 1,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(hg).Error()
		if err != nil {
			tt.Fatalf("Failed to create hg: %v", err)
		}

		getHostGroupWithDetails = func(db *gorm.DB, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error) {
			return hg, nil
		}
		isHostGroupInUse = func(db *gorm.DB, hostGroupUUID string, accountID int64) (bool, error) {
			return true, errors.New("some error occurred")
		}

		defer func() {
			getHostGroupWithDetails = _getHostGroupWithDetails
			isHostGroupInUse = _isHostGroupInUse
		}()

		result, err := store.DeleteHostGroup(context.Background(), "test-hg1", 1)
		assert.EqualError(tt, err, "some error occurred")
		assert.Nil(tt, result)
	})
}

func TestUpdateHostGroupsState(t *testing.T) {
	t.Run("WhenHostGroupNotExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		getMultipleHostGroups = func(db *gorm.DB, hostGroupUUID []string, accountID int64) ([]*datamodel.HostGroup, error) {
			return nil, customerrors.NewNotFoundErr("host group", nil)
		}

		defer func() { getMultipleHostGroups = _getMultipleHostGroups }()

		err = store.UpdateHostGroupsState(context.Background(), []string{"hg1"}, 1, models.LifeCycleStateREADY, "")
		assert.EqualError(tt, err, "host group not found")
	})
	t.Run("WhenHostGroupsExistsAndUpdateSucceeds", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hg1 := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-hg1",
			},
			Name:      "test_hg",
			AccountID: 1,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(hg1).Error()
		if err != nil {
			tt.Fatalf("Failed to create hg: %v", err)
		}

		hg2 := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-hg2",
			},
			Name:      "test_hg",
			AccountID: 1,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(hg2).Error()
		if err != nil {
			tt.Fatalf("Failed to create hg: %v", err)
		}

		startTransaction = func(db1 *gorm.DB) (*gorm.DB, error) {
			return db, nil
		}
		commitOrRollbackOnError = func(log slogger.Logger, tx *gorm.DB, err *error) {}
		defer func() {
			startTransaction = _startTransaction
			commitOrRollbackOnError = _commitOrRollbackOnError
			getMultipleHostGroups = _getMultipleHostGroups
		}()

		getMultipleHostGroups = func(db *gorm.DB, hostGroupUUID []string, accountID int64) ([]*datamodel.HostGroup, error) {
			return []*datamodel.HostGroup{hg1, hg2}, nil
		}

		err = store.UpdateHostGroupsState(context.Background(), []string{"hg1"}, 1, models.LifeCycleStateDeleted, "")
		assert.NoError(tt, err, "Failed to get host group")
	})
}

func TestIsHostGroupInUse(t *testing.T) {
	t.Run("WhenIsHostGroupInUseFalse", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hg1 := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-hg1",
			},
			Name:      "test_hg",
			AccountID: 1,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(hg1).Error()
		if err != nil {
			tt.Fatalf("Failed to create hg: %v", err)
		}

		volumesWithHG = func(db *gorm.DB, hostGroupUUID string, accountID int64) ([]*datamodel.Volume, error) {
			return []*datamodel.Volume{}, nil
		}

		defer func() { volumesWithHG = _volumesWithHG }()
		inUse, err := isHostGroupInUse(store.db.GORM(), hg1.UUID, hg1.AccountID)
		assert.NoError(tt, err, "Failed to get host group")
		assert.False(tt, inUse, "Expected host group to not be in use")
	})
	t.Run("WhenIsHostGroupInUseTrue", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hg1 := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-hg1",
			},
			Name:      "test_hg",
			AccountID: 1,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(hg1).Error()
		if err != nil {
			tt.Fatalf("Failed to create hg: %v", err)
		}

		volumesWithHG = func(db *gorm.DB, hostGroupUUID string, accountID int64) ([]*datamodel.Volume, error) {
			return []*datamodel.Volume{{Name: "123"}}, nil
		}

		defer func() { volumesWithHG = _volumesWithHG }()
		inUse, err := isHostGroupInUse(store.db.GORM(), hg1.UUID, hg1.AccountID)
		assert.NoError(tt, err, "Failed to get host group")
		assert.True(tt, inUse, "Expected host group to not be in use")
	})
	t.Run("WhenIsHostGroupInUseReturnsError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hg1 := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-hg1",
			},
			Name:      "test_hg",
			AccountID: 1,
			State:     models.LifeCycleStateREADY,
		}
		err = store.db.Create(hg1).Error()
		if err != nil {
			tt.Fatalf("Failed to create hg: %v", err)
		}

		volumesWithHG = func(db *gorm.DB, hostGroupUUID string, accountID int64) ([]*datamodel.Volume, error) {
			return nil, errors.New("some error occurred")
		}

		defer func() { volumesWithHG = _volumesWithHG }()
		inUse, err := isHostGroupInUse(store.db.GORM(), hg1.UUID, hg1.AccountID)
		assert.EqualError(tt, err, "some error occurred")
		assert.True(tt, inUse, "Expected host group to not be in use")
	})
}

func TestUpdateHostGroup(t *testing.T) {
	t.Run("WhenUpdateHostGroupSucceeds", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		hg1 := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-hg1",
			},
			Name:        "test_hg",
			AccountID:   1,
			Description: "Old description",
			Hosts:       datamodel.Hosts{Hosts: []string{"host1"}},
			State:       models.LifeCycleStateREADY,
		}
		err = store.db.Create(hg1).Error()
		assert.NoError(tt, err, "Failed to create host group")

		newDescription := "Updated description"
		newHosts := []string{"host2", "host3"}

		startTransaction = func(db1 *gorm.DB) (*gorm.DB, error) {
			return db, nil
		}
		commitOrRollbackOnError = func(log slogger.Logger, tx *gorm.DB, err *error) {}
		defer func() {
			startTransaction = _startTransaction
			commitOrRollbackOnError = _commitOrRollbackOnError
			getHostGroupWithDetails = _getHostGroupWithDetails
		}()

		getHostGroupWithDetails = func(db *gorm.DB, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error) {
			return hg1, nil
		}

		updatedHostGroup, err := store.UpdateHostGroup(context.Background(), hg1.UUID, hg1.AccountID, &newDescription, &newHosts)
		assert.NoError(tt, err, "Failed to update host group")
		assert.Equal(tt, newDescription, updatedHostGroup.Description, "Description did not update correctly")
		assert.Equal(tt, newHosts, updatedHostGroup.Hosts.Hosts, "Hosts did not update correctly")
	})
	t.Run("WhenUpdateHostGroupFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		startTransaction = func(db1 *gorm.DB) (*gorm.DB, error) {
			return nil, errors.New("transaction error")
		}
		defer func() { startTransaction = _startTransaction }()

		newDescription := "Updated description"
		newHosts := []string{"host2", "host3"}

		_, err = store.UpdateHostGroup(context.Background(), "non-existent-uuid", 1, &newDescription, &newHosts)
		assert.EqualError(tt, err, "transaction error", "Expected transaction error")
	})
}
