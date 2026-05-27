package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
)

func TestCreateActiveDirectory(t *testing.T) {
	t.Run("WhenActiveDirectoryIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName:    "test-active-directory",
			AccountId: 123,
		}

		createdAd, err := store.CreateActiveDirectory(context.Background(), ad)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, createdAd, "Expected created active directory to not be nil")
		assert.Equal(tt, ad.AdName, createdAd.AdName, "Expected AD name %v, got %v", ad.AdName, createdAd.AdName)
		assert.Equal(tt, ad.AccountId, createdAd.AccountId, "Expected account ID %v, got %v", ad.AccountId, createdAd.AccountId)
		assert.Equal(tt, ad.UUID, createdAd.UUID, "Expected UUID %v, got %v", ad.UUID, createdAd.UUID)
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate a database error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName:    "test-active-directory",
			AccountId: 123,
		}

		_, err = store.CreateActiveDirectory(context.Background(), ad)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Contains(tt, err.Error(), "sql: database is closed", "Expected database closed error")
	})

	t.Run("WhenActiveDirectoryHasMinimalFields", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		ad := &datamodel.ActiveDirectory{
			AdName:    "minimal-ad",
			AccountId: 456,
		}

		createdAd, err := store.CreateActiveDirectory(context.Background(), ad)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, createdAd, "Expected created active directory to not be nil")
		assert.Equal(tt, ad.AdName, createdAd.AdName, "Expected AD name %v, got %v", ad.AdName, createdAd.AdName)
		assert.Equal(tt, ad.AccountId, createdAd.AccountId, "Expected account ID %v, got %v", ad.AccountId, createdAd.AccountId)
	})
}

func TestGetActiveDirectoryByNameAndAccountID(t *testing.T) {
	t.Run("WhenActiveDirectoryExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory first
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName:    "test-active-directory",
			AccountId: 123,
		}
		err = store.db.Create(ad).Error()
		assert.NoError(tt, err, "Failed to create active directory")

		// Retrieve the active directory
		result, err := store.GetActiveDirectoryByNameAndAccountID(context.Background(), ad.AdName, ad.AccountId)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Equal(tt, ad.AdName, result.AdName, "Expected AD name %v, got %v", ad.AdName, result.AdName)
		assert.Equal(tt, ad.AccountId, result.AccountId, "Expected account ID %v, got %v", ad.AccountId, result.AccountId)
		assert.Equal(tt, ad.UUID, result.UUID, "Expected UUID %v, got %v", ad.UUID, result.UUID)
	})

	t.Run("WhenActiveDirectoryDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to retrieve a non-existent active directory
		result, err := store.GetActiveDirectoryByNameAndAccountID(context.Background(), "non-existent-ad", 999)
		assert.NoError(tt, err, "Expected no error for non-existent record")
		assert.Nil(tt, result, "Expected result to be nil for non-existent record")
	})

	t.Run("WhenActiveDirectoryWithSameNameButDifferentAccountExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory with specific account ID
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName:    "shared-ad-name",
			AccountId: 123,
		}
		err = store.db.Create(ad).Error()
		assert.NoError(tt, err, "Failed to create active directory")

		// Try to retrieve with same name but different account ID
		result, err := store.GetActiveDirectoryByNameAndAccountID(context.Background(), "shared-ad-name", 456)
		assert.NoError(tt, err, "Expected no error")
		assert.Nil(tt, result, "Expected result to be nil for different account ID")

		// Verify we can still retrieve with correct account ID
		result, err = store.GetActiveDirectoryByNameAndAccountID(context.Background(), "shared-ad-name", 123)
		assert.NoError(tt, err, "Expected no error")
		assert.NotNil(tt, result, "Expected result to not be nil for correct account ID")
		assert.Equal(tt, ad.AdName, result.AdName, "Expected correct AD name")
		assert.Equal(tt, ad.AccountId, result.AccountId, "Expected correct account ID")
	})

	t.Run("WhenSoftDeletedActiveDirectoryExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName:    "soft-deleted-ad",
			AccountId: 123,
		}
		err = store.db.Create(ad).Error()
		assert.NoError(tt, err, "Failed to create active directory")

		// Soft delete the active directory
		err = store.db.GORM().Delete(&datamodel.ActiveDirectory{}, "uuid = ?", ad.UUID).Error
		assert.NoError(tt, err, "Failed to soft delete active directory")

		// Try to retrieve the soft-deleted active directory
		result, err := store.GetActiveDirectoryByNameAndAccountID(context.Background(), ad.AdName, ad.AccountId)
		assert.NoError(tt, err, "Expected no error")
		assert.Nil(tt, result, "Expected result to be nil for soft-deleted record")
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate a database error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		_, err = store.GetActiveDirectoryByNameAndAccountID(context.Background(), "test-ad", 123)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Contains(tt, err.Error(), "sql: database is closed", "Expected database closed error")
	})
}

func TestCreateActiveDirectoryFunction(t *testing.T) {
	t.Run("WhenDirectFunctionCallSucceeds", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		err = ClearInMemoryDB(db)
		assert.NoError(tt, err, "Failed to clean up test database")

		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "direct-test-uuid",
			},
			AdName:    "direct-test-ad",
			AccountId: 789,
		}

		result, err := createActiveDirectory(db, ad)
		assert.NoError(tt, err, "Expected no error from direct function call")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Equal(tt, ad.AdName, result.AdName, "Expected AD name to match")
		assert.Equal(tt, ad.AccountId, result.AccountId, "Expected account ID to match")
	})

	t.Run("WhenDuplicateActiveDirectoryExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		err = ClearInMemoryDB(db)
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create first active directory
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "duplicate-test-uuid-1",
			},
			AdName:    "duplicate-test-ad",
			AccountId: 789,
		}

		result, err := createActiveDirectory(db, ad)
		assert.NoError(tt, err, "Expected no error creating first AD")
		assert.NotNil(tt, result, "Expected first result to not be nil")

		// Try to create duplicate active directory
		duplicateAd := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "duplicate-test-uuid-2",
			},
			AdName:    "duplicate-test-ad",
			AccountId: 789,
		}

		result, err = createActiveDirectory(db, duplicateAd)
		assert.Error(tt, err, "Expected error when creating duplicate AD")
		assert.Nil(tt, result, "Expected result to be nil on duplicate")
		assert.Contains(tt, err.Error(), "Active Directory with the given name already exists", "Expected duplicate AD error message")
	})

	t.Run("WhenDuplicateActiveDirectoryExistsViaRepositoryMethod", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create first active directory
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "duplicate-test-uuid-3",
			},
			AdName:    "duplicate-repo-test-ad",
			AccountId: 789,
		}

		result, err := store.CreateActiveDirectory(context.Background(), ad)
		assert.NoError(tt, err, "Expected no error creating first AD")
		assert.NotNil(tt, result, "Expected first result to not be nil")

		// Try to create duplicate active directory
		duplicateAd := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "duplicate-test-uuid-4",
			},
			AdName:    "duplicate-repo-test-ad",
			AccountId: 789,
		}

		result, err = store.CreateActiveDirectory(context.Background(), duplicateAd)
		assert.Error(tt, err, "Expected error when creating duplicate AD")
		assert.Nil(tt, result, "Expected result to be nil on duplicate")
		assert.Contains(tt, err.Error(), "Active Directory with the given name already exists", "Expected duplicate AD error message")
	})

	t.Run("WhenDirectFunctionCallFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		// Close the database to simulate error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		ad := &datamodel.ActiveDirectory{
			AdName:    "fail-test-ad",
			AccountId: 789,
		}

		result, err := createActiveDirectory(db, ad)
		assert.Error(tt, err, "Expected error from direct function call")
		assert.Nil(tt, result, "Expected result to be nil on error")
	})
}

func TestGetActiveDirectoryWithDetailsFunction(t *testing.T) {
	t.Run("WhenRecordExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		err = ClearInMemoryDB(db)
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "details-test-uuid",
			},
			AdName:    "details-test-ad",
			AccountId: 123,
		}
		err = db.Create(ad).Error
		assert.NoError(tt, err, "Failed to create active directory")

		// Query using the function
		query := &datamodel.ActiveDirectory{
			AdName:    "details-test-ad",
			AccountId: 123,
			BaseModel: datamodel.BaseModel{DeletedAt: nil},
		}
		result, err := getActiveDirectoryWithDetails(db, query)
		assert.NoError(tt, err, "Expected no error")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Equal(tt, ad.AdName, result.AdName, "Expected AD name to match")
		assert.Equal(tt, ad.AccountId, result.AccountId, "Expected account ID to match")
	})

	t.Run("WhenRecordNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		err = ClearInMemoryDB(db)
		assert.NoError(tt, err, "Failed to clean up test database")

		query := &datamodel.ActiveDirectory{
			AdName:    "non-existent-ad",
			AccountId: 999,
			BaseModel: datamodel.BaseModel{DeletedAt: nil},
		}
		result, err := getActiveDirectoryWithDetails(db, query)
		assert.NoError(tt, err, "Expected no error for record not found")
		assert.Nil(tt, result, "Expected result to be nil when record not found")
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		// Close the database to simulate error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		query := &datamodel.ActiveDirectory{
			AdName:    "error-test-ad",
			AccountId: 123,
		}
		result, err := getActiveDirectoryWithDetails(db, query)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, result, "Expected result to be nil on error")
	})
}

// TestGetActiveDirectoryByUUID was removed because GetActiveDirectoryByUUID method doesn't exist
// Use GetActiveDirectoryByUuidAndAccountId instead

func TestListActiveDirectories(t *testing.T) {
	t.Run("WhenActiveDirectoriesExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create multiple active directories for the same account
		ads := []*datamodel.ActiveDirectory{
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-1"},
				AdName:    "ad-name-1",
				AccountId: 123,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-2"},
				AdName:    "ad-name-2",
				AccountId: 123,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-3"},
				AdName:    "ad-name-3",
				AccountId: 456, // Different account
			},
		}

		for _, ad := range ads {
			err = store.db.Create(ad).Error()
			assert.NoError(tt, err, "Failed to create active directory")
		}

		// List active directories for account 123
		result, err := store.ListActiveDirectories(context.Background(), 123)
		assert.NoError(tt, err, "Expected no error")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Len(tt, result, 2, "Expected 2 active directories for account 123")
		assert.Equal(tt, "ad-1", result[0].UUID)
		assert.Equal(tt, "ad-2", result[1].UUID)
	})

	t.Run("WhenNoActiveDirectoriesExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// List active directories for an account with no ADs
		result, err := store.ListActiveDirectories(context.Background(), 999)
		assert.NoError(tt, err, "Expected no error")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Len(tt, result, 0, "Expected 0 active directories")
	})

	t.Run("WhenSoftDeletedActiveDirectoriesExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create active directories
		ads := []*datamodel.ActiveDirectory{
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-1"},
				AdName:    "ad-name-1",
				AccountId: 123,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-2"},
				AdName:    "ad-name-2",
				AccountId: 123,
			},
		}

		for _, ad := range ads {
			err = store.db.Create(ad).Error()
			assert.NoError(tt, err, "Failed to create active directory")
		}

		// Soft delete one AD
		err = store.db.GORM().Delete(&datamodel.ActiveDirectory{}, "uuid = ?", "ad-1").Error
		assert.NoError(tt, err, "Failed to soft delete active directory")

		// List should only return non-deleted ADs
		result, err := store.ListActiveDirectories(context.Background(), 123)
		assert.NoError(tt, err, "Expected no error")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Len(tt, result, 1, "Expected 1 active directory (excluding deleted)")
		assert.Equal(tt, "ad-2", result[0].UUID)
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		_, err = store.ListActiveDirectories(context.Background(), 123)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Contains(tt, err.Error(), "sql: database is closed", "Expected database closed error")
	})
}

func TestGetMultipleActiveDirectoriesByUUIDs(t *testing.T) {
	t.Run("WhenAllUUIDsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create multiple active directories
		ads := []*datamodel.ActiveDirectory{
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-uuid-1"},
				AdName:    "ad-name-1",
				AccountId: 123,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-uuid-2"},
				AdName:    "ad-name-2",
				AccountId: 123,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-uuid-3"},
				AdName:    "ad-name-3",
				AccountId: 456,
			},
		}

		for _, ad := range ads {
			err = store.db.Create(ad).Error()
			assert.NoError(tt, err, "Failed to create active directory")
		}

		// Get multiple ADs by UUIDs
		uuids := []string{"ad-uuid-1", "ad-uuid-2"}
		result, err := store.GetMultipleActiveDirectoriesByUUIDs(context.Background(), uuids)
		assert.NoError(tt, err, "Expected no error")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Len(tt, result, 2, "Expected 2 active directories")

		// Verify both UUIDs are in the result
		resultUUIDs := make(map[string]bool)
		for _, ad := range result {
			resultUUIDs[ad.UUID] = true
		}
		assert.True(tt, resultUUIDs["ad-uuid-1"], "Expected ad-uuid-1 in result")
		assert.True(tt, resultUUIDs["ad-uuid-2"], "Expected ad-uuid-2 in result")
	})

	t.Run("WhenSomeUUIDsDoNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create only one AD
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{UUID: "ad-uuid-1"},
			AdName:    "ad-name-1",
			AccountId: 123,
		}
		err = store.db.Create(ad).Error()
		assert.NoError(tt, err, "Failed to create active directory")

		// Request multiple UUIDs including non-existent ones
		uuids := []string{"ad-uuid-1", "non-existent-uuid"}
		result, err := store.GetMultipleActiveDirectoriesByUUIDs(context.Background(), uuids)
		assert.NoError(tt, err, "Expected no error")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Len(tt, result, 1, "Expected 1 active directory")
		assert.Equal(tt, "ad-uuid-1", result[0].UUID)
	})

	t.Run("WhenNoUUIDsProvided", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Empty UUID list
		uuids := []string{}
		result, err := store.GetMultipleActiveDirectoriesByUUIDs(context.Background(), uuids)
		assert.NoError(tt, err, "Expected no error")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Len(tt, result, 0, "Expected 0 active directories")
	})

	t.Run("WhenIncludesSoftDeletedADs", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create multiple ADs
		ads := []*datamodel.ActiveDirectory{
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-uuid-1"},
				AdName:    "ad-name-1",
				AccountId: 123,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-uuid-2"},
				AdName:    "ad-name-2",
				AccountId: 123,
			},
		}

		for _, ad := range ads {
			err = store.db.Create(ad).Error()
			assert.NoError(tt, err, "Failed to create active directory")
		}

		// Soft delete one AD
		err = store.db.GORM().Delete(&datamodel.ActiveDirectory{}, "uuid = ?", "ad-uuid-1").Error
		assert.NoError(tt, err, "Failed to soft delete active directory")

		// Get multiple should include soft-deleted ADs (as per the implementation)
		uuids := []string{"ad-uuid-1", "ad-uuid-2"}
		result, err := store.GetMultipleActiveDirectoriesByUUIDs(context.Background(), uuids)
		assert.NoError(tt, err, "Expected no error")
		assert.NotNil(tt, result, "Expected result to not be nil")
		// Note: The function doesn't filter deleted_at, so it returns both
		assert.Len(tt, result, 2, "Expected 2 active directories including soft-deleted")
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		uuids := []string{"ad-uuid-1"}
		_, err = store.GetMultipleActiveDirectoriesByUUIDs(context.Background(), uuids)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Contains(tt, err.Error(), "sql: database is closed", "Expected database closed error")
	})
}

func TestListActiveDirectoriesFunction(t *testing.T) {
	t.Run("WhenDirectFunctionCallSucceeds", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		err = ClearInMemoryDB(db)
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create multiple ADs
		ads := []*datamodel.ActiveDirectory{
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-1"},
				AdName:    "ad-name-1",
				AccountId: 100,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "ad-2"},
				AdName:    "ad-name-2",
				AccountId: 100,
			},
		}

		for _, ad := range ads {
			err = db.Create(ad).Error
			assert.NoError(tt, err, "Failed to create active directory")
		}

		result, err := listActiveDirectories(db, 100)
		assert.NoError(tt, err, "Expected no error from direct function call")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Len(tt, result, 2, "Expected 2 active directories")
	})
}

func TestGetMultipleActiveDirectoriesByUUIDsFunction(t *testing.T) {
	t.Run("WhenDirectFunctionCallSucceeds", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		err = ClearInMemoryDB(db)
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create multiple ADs
		ads := []*datamodel.ActiveDirectory{
			{
				BaseModel: datamodel.BaseModel{UUID: "uuid-1"},
				AdName:    "ad-1",
				AccountId: 100,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "uuid-2"},
				AdName:    "ad-2",
				AccountId: 100,
			},
		}

		for _, ad := range ads {
			err = db.Create(ad).Error
			assert.NoError(tt, err, "Failed to create active directory")
		}

		uuids := []string{"uuid-1", "uuid-2"}
		result, err := getMultipleActiveDirectoriesByUUIDs(db, uuids)
		assert.NoError(tt, err, "Expected no error from direct function call")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Len(tt, result, 2, "Expected 2 active directories")
	})
}

func TestGetActiveDirectoryByUuidAndAccountId(t *testing.T) {
	t.Run("WhenActiveDirectoryExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory first
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "550e8400-e29b-41d4-a716-446655440000",
			},
			AdName:    "test-active-directory",
			AccountId: 123,
		}

		err = db.Create(ad).Error
		assert.NoError(tt, err, "Failed to create active directory")

		retrievedAd, err := store.GetActiveDirectoryByUuidAndAccountId(context.Background(), "550e8400-e29b-41d4-a716-446655440000", 123)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, retrievedAd, "Expected active directory to not be nil")
		assert.Equal(tt, ad.AdName, retrievedAd.AdName, "Expected AD name %v, got %v", ad.AdName, retrievedAd.AdName)
		assert.Equal(tt, ad.AccountId, retrievedAd.AccountId, "Expected account ID %v, got %v", ad.AccountId, retrievedAd.AccountId)
		assert.Equal(tt, ad.UUID, retrievedAd.UUID, "Expected UUID %v, got %v", ad.UUID, retrievedAd.UUID)
	})

	t.Run("WhenActiveDirectoryDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(db)
		assert.NoError(tt, err, "Failed to clean up test database")

		retrievedAd, err := store.GetActiveDirectoryByUuidAndAccountId(context.Background(), "550e8400-e29b-41d4-a716-446655440000", 123)
		assert.Error(tt, err, "Expected not found error for non-existent AD")
		assert.ErrorContains(tt, err, "Active Directory not found")
		assert.Nil(tt, retrievedAd, "Expected active directory to be nil")
	})

	t.Run("WhenActiveDirectoryExistsButWrongAccount", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(db)
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory for account 123
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "550e8400-e29b-41d4-a716-446655440000",
			},
			AdName:    "test-active-directory",
			AccountId: 123,
		}

		err = db.Create(ad).Error
		assert.NoError(tt, err, "Failed to create active directory")

		// Try to get it for account 456
		retrievedAd, err := store.GetActiveDirectoryByUuidAndAccountId(context.Background(), "550e8400-e29b-41d4-a716-446655440000", 456)
		assert.Error(tt, err, "Expected not found error for wrong account")
		assert.ErrorContains(tt, err, "Active Directory not found")
		assert.Nil(tt, retrievedAd, "Expected active directory to be nil for wrong account")
	})

	t.Run("WhenActiveDirectoryIsNil", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(db)
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory for account 123
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "550e8400-e29b-41d4-a716-446655440000",
			},
			AdName:    "test-active-directory",
			AccountId: 123,
		}

		err = db.Create(ad).Error
		assert.NoError(tt, err, "Failed to create active directory")

		// Try to get it for account 456
		retrievedAd, err := store.GetActiveDirectoryByUuidAndAccountId(context.Background(), "550e8400-e29b-41d4-a716-446655440000", 456)
		assert.Error(tt, err, "Expected not found error for wrong account")
		assert.ErrorContains(tt, err, "Active Directory not found")
		assert.Nil(tt, retrievedAd, "Expected active directory to be nil for wrong account")
		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		updatedAd, err := store.UpdateActiveDirectory(context.Background(), nil)
		assert.Error(tt, err, "Expected error when active directory is nil")
		assert.Nil(tt, updatedAd, "Expected result to be nil")
		assert.Contains(tt, err.Error(), "Active Directory is nil", "Expected nil AD error message")
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate a database error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		_, err = store.GetActiveDirectoryByUuidAndAccountId(context.Background(), "550e8400-e29b-41d4-a716-446655440000", 123)
		assert.Error(tt, err, "Expected error when database is closed")
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			AdName:    "test-ad",
			AccountId: 123,
		}

		_, err = store.UpdateActiveDirectory(context.Background(), ad)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Contains(tt, err.Error(), "sql: database is closed", "Expected database closed error")
	})

	t.Run("WhenUpdatingMultipleFields", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "multi-update-uuid",
			},
			AdName:    "original-name",
			AccountId: 100,
		}
		createdAd, err := store.CreateActiveDirectory(context.Background(), ad)
		assert.NoError(tt, err, "Failed to create active directory")

		// Update multiple fields
		originalUpdatedAt := createdAd.UpdatedAt
		createdAd.AdName = "new-name"
		// Note: AccountId typically shouldn't change, but testing field updates

		updatedAd, err := store.UpdateActiveDirectory(context.Background(), createdAd)
		assert.NoError(tt, err, "Expected no error")
		assert.NotNil(tt, updatedAd, "Expected updated active directory to not be nil")
		assert.Equal(tt, "new-name", updatedAd.AdName, "Expected updated AD name")
		assert.True(tt, updatedAd.UpdatedAt.After(originalUpdatedAt), "Expected UpdatedAt to be updated")
	})
}

func TestUpdateActiveDirectory(t *testing.T) {
	t.Run("WhenActiveDirectoryIsUpdatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory first
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "update-test-uuid",
			},
			AdName:    "original-ad-name",
			AccountId: 123,
		}
		createdAd, err := store.CreateActiveDirectory(context.Background(), ad)
		assert.NoError(tt, err, "Failed to create active directory")

		// Update the active directory
		createdAd.AdName = "updated-ad-name"
		updatedAd, err := store.UpdateActiveDirectory(context.Background(), createdAd)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, updatedAd, "Expected updated active directory to not be nil")
		assert.Equal(tt, "updated-ad-name", updatedAd.AdName, "Expected updated AD name")
		assert.Equal(tt, createdAd.ID, updatedAd.ID, "Expected ID to remain the same")
	})

	t.Run("WhenActiveDirectoryDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to update non-existent active directory
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				ID: 99999,
			},
			AdName:    "non-existent-ad",
			AccountId: 123,
		}

		updatedAd, err := store.UpdateActiveDirectory(context.Background(), ad)
		assert.Error(tt, err, "Expected error for non-existent record")
		assert.Nil(tt, updatedAd, "Expected result to be nil")
		assert.Contains(tt, err.Error(), "not found", "Expected not found error message")
	})

	t.Run("WhenActiveDirectoryIsNil", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		updatedAd, err := store.UpdateActiveDirectory(context.Background(), nil)
		assert.Error(tt, err, "Expected error when active directory is nil")
		assert.Nil(tt, updatedAd, "Expected result to be nil")
		assert.Contains(tt, err.Error(), "Active Directory is nil", "Expected nil AD error message")
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate a database error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			AdName:    "test-ad",
			AccountId: 123,
		}

		_, err = store.UpdateActiveDirectory(context.Background(), ad)
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Contains(tt, err.Error(), "sql: database is closed", "Expected database closed error")
	})

	t.Run("WhenUpdatingMultipleFields", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "multi-update-uuid",
			},
			AdName:    "original-name",
			AccountId: 100,
		}
		createdAd, err := store.CreateActiveDirectory(context.Background(), ad)
		assert.NoError(tt, err, "Failed to create active directory")

		// Update multiple fields
		originalUpdatedAt := createdAd.UpdatedAt
		createdAd.AdName = "new-name"
		// Note: AccountId typically shouldn't change, but testing field updates

		updatedAd, err := store.UpdateActiveDirectory(context.Background(), createdAd)
		assert.NoError(tt, err, "Expected no error")
		assert.NotNil(tt, updatedAd, "Expected updated active directory to not be nil")
		assert.Equal(tt, "new-name", updatedAd.AdName, "Expected updated AD name")
		assert.True(tt, updatedAd.UpdatedAt.After(originalUpdatedAt), "Expected UpdatedAt to be updated")
	})
}

func TestUpdateActiveDirectoryFunction(t *testing.T) {
	t.Run("WhenDirectFunctionCallSucceeds", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		err = ClearInMemoryDB(db)
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "direct-update-uuid",
			},
			AdName:    "direct-original",
			AccountId: 456,
		}
		err = db.Create(ad).Error
		assert.NoError(tt, err, "Failed to create active directory")

		// Update using direct function
		ad.AdName = "direct-updated"
		result, err := updateActiveDirectory(db, ad)
		assert.NoError(tt, err, "Expected no error from direct function call")
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Equal(tt, "direct-updated", result.AdName, "Expected updated AD name")
	})

	t.Run("WhenNilActiveDirectory", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		result, err := updateActiveDirectory(db, nil)
		assert.Error(tt, err, "Expected error for nil active directory")
		assert.Nil(tt, result, "Expected result to be nil")
		assert.Contains(tt, err.Error(), "Active Directory is nil", "Expected nil AD error message")
	})

	t.Run("WhenNoRowsAffected", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		err = ClearInMemoryDB(db)
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to update with non-existent ID
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				ID: 99999,
			},
			AdName:    "non-existent",
			AccountId: 789,
		}

		result, err := updateActiveDirectory(db, ad)
		assert.Error(tt, err, "Expected error when no rows affected")
		assert.Nil(tt, result, "Expected result to be nil")
		assert.Contains(tt, err.Error(), "not found", "Expected not found error")
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")

		// Close the database to simulate error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			AdName:    "error-test",
			AccountId: 123,
		}

		result, err := updateActiveDirectory(db, ad)
		assert.Error(tt, err, "Expected error from direct function call")
		assert.Nil(tt, result, "Expected result to be nil on error")
	})
}

// TestGetActiveDirectoryByUUIDAndAccountID was removed - method consolidated to GetActiveDirectoryByUuidAndAccountId
// All tests now use the consistent camelCase naming: GetActiveDirectoryByUuidAndAccountId

// TestDeleteActiveDirectory tests the DeleteActiveDirectory method
func TestDeleteActiveDirectory(t *testing.T) {
	t.Run("WhenActiveDirectoryExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "delete-test-uuid",
			},
			AdName:    "delete-test-ad",
			AccountId: 123,
		}
		err = store.db.Create(ad).Error()
		assert.NoError(tt, err, "Failed to create active directory")

		// Delete the active directory
		err = store.DeleteActiveDirectory(context.Background(), "delete-test-uuid")
		assert.NoError(tt, err, "Expected no error when deleting")

		// Verify it's soft deleted
		var count int64
		err = store.db.GORM().Model(&datamodel.ActiveDirectory{}).Where("uuid = ?", "delete-test-uuid").Count(&count).Error
		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), count, "Expected AD to be soft deleted")
	})

	t.Run("WhenActiveDirectoryNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Try to delete non-existent AD (should succeed as idempotent)
		err = store.DeleteActiveDirectory(context.Background(), "non-existent-uuid")
		assert.NoError(tt, err, "Expected no error for non-existent record")
	})

	t.Run("WhenDatabaseUpdateError", func(tt *testing.T) {
		// Test to cover line 78: error return path in deleteActiveDirectory
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "error-delete-uuid",
			},
			AdName:    "error-delete-ad",
			AccountId: 123,
		}
		err = store.db.Create(ad).Error()
		assert.NoError(tt, err, "Failed to create active directory")

		// Close the database to simulate error during update
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		// Try to delete - should fail
		err = store.DeleteActiveDirectory(context.Background(), "error-delete-uuid")
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Contains(tt, err.Error(), "sql: database is closed", "Expected database closed error")
	})
}

// TestGetSVMsUsingActiveDirectory tests the GetSVMsUsingActiveDirectory method
func TestGetSVMsUsingActiveDirectory(t *testing.T) {
	t.Run("WhenNoSVMsUseTheAD", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an active directory to get a valid ID
		ad := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName:    "test-ad",
			AccountId: 123,
		}
		createdAd, err := store.CreateActiveDirectory(context.Background(), ad)
		assert.NoError(tt, err, "Failed to create active directory")

		// Get SVMs using the AD ID (should return empty list)
		svms, err := store.GetSVMsUsingActiveDirectory(context.Background(), createdAd.ID)
		assert.NoError(tt, err, "Expected no error")
		assert.Empty(tt, svms, "Expected empty SVM list")
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		// Test to cover line 93: error return path in getSVMsUsingActiveDirectory
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Close the database to simulate error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		err = sqlDB.Close()
		assert.NoError(tt, err)

		// Use a dummy ID since the database is closed anyway
		svms, err := store.GetSVMsUsingActiveDirectory(context.Background(), int64(999))
		assert.Error(tt, err, "Expected error when database is closed")
		assert.Nil(tt, svms, "Expected nil result on error")
		assert.Contains(tt, err.Error(), "sql: database is closed", "Expected database closed error")
	})
}
