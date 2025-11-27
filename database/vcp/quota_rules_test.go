package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"gorm.io/gorm"
)

func TestGetQuotaRulesByVolumeID(t *testing.T) {
	t.Run("WhenQuotaRulesExistForVolume", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		quotaRule1 := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid-1",
			},
			Name:           "quota-rule-1",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
			State:          models.LifeCycleStateAvailable,
		}
		err = store.db.Create(quotaRule1).Error()
		assert.NoError(tt, err, "Failed to create quota rule 1")

		quotaRule2 := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid-2",
			},
			Name:           "quota-rule-2",
			QuotaType:      "DEFAULT_USER_QUOTA",
			QuotaTarget:    "",
			DiskLimitInKib: 2097152,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
			State:          models.LifeCycleStateAvailable,
		}
		err = store.db.Create(quotaRule2).Error()
		assert.NoError(tt, err, "Failed to create quota rule 2")

		quotaRules, err := store.GetQuotaRulesByVolumeID(context.Background(), volume.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, quotaRules, 2, "Expected 2 quota rules, got %d", len(quotaRules))
		assert.Equal(tt, quotaRule1.UUID, quotaRules[0].UUID, "Expected quota rule 1 UUID")
		assert.Equal(tt, quotaRule2.UUID, quotaRules[1].UUID, "Expected quota rule 2 UUID")
	})

	t.Run("WhenNoQuotaRulesExistForVolume", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		quotaRules, err := store.GetQuotaRulesByVolumeID(context.Background(), volume.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, quotaRules, "Expected no quota rules, got %d", len(quotaRules))
	})

	t.Run("WhenVolumeDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		quotaRules, err := store.GetQuotaRulesByVolumeID(context.Background(), 999)
		assert.NoError(tt, err, "Expected no error for non-existent volume")
		assert.Empty(tt, quotaRules, "Expected no quota rules for non-existent volume")
	})
}

func TestCreatingQuotaRule(t *testing.T) {
	t.Run("WhenQuotaRuleIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		quotaRule := &datamodel.QuotaRule{
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
			State:          models.LifeCycleStateCreating,
		}

		createdQuotaRule, err := store.CreatingQuotaRule(context.Background(), quotaRule)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotEmpty(tt, createdQuotaRule.UUID, "Expected UUID to be generated")
		assert.Equal(tt, quotaRule.Name, createdQuotaRule.Name, "Expected quota rule name to match")
		assert.Equal(tt, quotaRule.QuotaType, createdQuotaRule.QuotaType, "Expected quota type to match")
		assert.Equal(tt, quotaRule.QuotaTarget, createdQuotaRule.QuotaTarget, "Expected quota target to match")
		assert.NotNil(tt, createdQuotaRule.Volume, "Expected volume to be preloaded")
		assert.Equal(tt, volume.UUID, createdQuotaRule.Volume.UUID, "Expected volume UUID to match")
	})

	t.Run("WhenQuotaRuleWithUUIDIsCreated", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		quotaRuleUUID := "custom-quota-rule-uuid"
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: quotaRuleUUID,
			},
			Name:           "test-quota-rule-with-uuid",
			QuotaType:      "DEFAULT_USER_QUOTA",
			QuotaTarget:    "",
			DiskLimitInKib: 2097152,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
			State:          models.LifeCycleStateCreating,
		}

		createdQuotaRule, err := store.CreatingQuotaRule(context.Background(), quotaRule)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, quotaRuleUUID, createdQuotaRule.UUID, "Expected custom UUID to be preserved")
	})

	t.Run("WhenDuplicateQuotaRuleExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		quotaRule1 := &datamodel.QuotaRule{
			Name:           "quota-rule-1",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
			State:          models.LifeCycleStateCreating,
		}
		_, err = store.CreatingQuotaRule(context.Background(), quotaRule1)
		assert.NoError(tt, err, "Failed to create first quota rule")

		quotaRule2 := &datamodel.QuotaRule{
			Name:           "quota-rule-2",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000", // Same type and target
			DiskLimitInKib: 2097152,
			AccountID:      account.ID,
			VolumeID:       volume.ID, // Same volume
			State:          models.LifeCycleStateCreating,
		}
		_, err = store.CreatingQuotaRule(context.Background(), quotaRule2)
		assert.Error(tt, err, "Expected error for duplicate quota rule")
		assert.Contains(tt, err.Error(), "quota rule with same type and target already exists", "Expected conflict error")
	})

	t.Run("WhenDifferentQuotaTargetsAreAllowed", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		quotaRule1 := &datamodel.QuotaRule{
			Name:           "quota-rule-1",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
			State:          models.LifeCycleStateCreating,
		}
		_, err = store.CreatingQuotaRule(context.Background(), quotaRule1)
		assert.NoError(tt, err, "Failed to create first quota rule")

		quotaRule2 := &datamodel.QuotaRule{
			Name:           "quota-rule-2",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "2000", // Different target
			DiskLimitInKib: 2097152,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
			State:          models.LifeCycleStateCreating,
		}
		_, err = store.CreatingQuotaRule(context.Background(), quotaRule2)
		assert.NoError(tt, err, "Expected no error for different quota target")
	})
}

func TestUpdateQuotaRule(t *testing.T) {
	t.Run("WhenQuotaRuleIsUpdatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
			State:          models.LifeCycleStateCreating,
		}
		err = store.db.Create(quotaRule).Error()
		assert.NoError(tt, err, "Failed to create quota rule")

		quotaRule.DiskLimitInKib = 2097152
		quotaRule.Description = "Updated description"

		updatedQuotaRule, err := store.UpdateQuotaRule(context.Background(), quotaRule)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, models.LifeCycleStateREADY, updatedQuotaRule.State, "Expected state to be READY")
		assert.Equal(tt, models.LifeCycleStateReadyDetails, updatedQuotaRule.StateDetails, "Expected state details to match")
		assert.Equal(tt, int64(2097152), updatedQuotaRule.DiskLimitInKib, "Expected disk limit to be updated")
		assert.Equal(tt, "Updated description", updatedQuotaRule.Description, "Expected description to be updated")
	})

	t.Run("WhenQuotaRuleNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "non-existent-uuid",
			},
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
		}

		_, err = store.UpdateQuotaRule(context.Background(), quotaRule)
		assert.Error(tt, err, "Expected error for non-existent quota rule")
		assert.Contains(tt, err.Error(), "quota rule", "Expected quota rule not found error")
	})
}

func TestGetQuotaRuleByUUID(t *testing.T) {
	t.Run("WhenQuotaRuleExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
		}
		err = store.db.Create(quotaRule).Error()
		assert.NoError(tt, err, "Failed to create quota rule")

		result, err := store.GetQuotaRuleByUUID(context.Background(), "quota-rule-uuid", account.ID, volume.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, quotaRule.UUID, result.UUID, "Expected quota rule UUID to match")
		assert.Equal(tt, quotaRule.Name, result.Name, "Expected quota rule name to match")
		assert.NotNil(tt, result.Volume, "Expected volume to be preloaded")
		assert.NotNil(tt, result.Volume.Pool, "Expected pool to be preloaded")
	})

	t.Run("WhenQuotaRuleDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, _, _, err = CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		result, err := store.GetQuotaRuleByUUID(context.Background(), "non-existent-uuid", 1, 1)
		assert.Error(tt, err, "Expected error for non-existent quota rule")
		assert.Nil(tt, result, "Expected nil result")
		assert.Contains(tt, err.Error(), "quota rule", "Expected quota rule not found error")
	})

	t.Run("WhenAccountIDFilterIsApplied", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account1, _, volume1, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		account2 := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-account-2-uuid",
			},
			Name: "test_account_2",
		}
		err = store.db.Create(account2).Error()
		assert.NoError(tt, err, "Failed to create account 2")

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account1.ID,
			VolumeID:       volume1.ID,
		}
		err = store.db.Create(quotaRule).Error()
		assert.NoError(tt, err, "Failed to create quota rule")

		// Try to get with wrong account ID
		result, err := store.GetQuotaRuleByUUID(context.Background(), "quota-rule-uuid", account2.ID, volume1.ID)
		assert.Error(tt, err, "Expected error for wrong account ID")
		assert.Nil(tt, result, "Expected nil result")
	})

	t.Run("WhenVolumeIDFilterIsApplied", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume1, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		volume2 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-2-uuid"},
			Name:      "test_volume_2",
			AccountID: account.ID,
			PoolID:    volume1.PoolID,
			SvmID:     volume1.SvmID,
		}
		err = store.db.Create(volume2).Error()
		assert.NoError(tt, err, "Failed to create volume 2")

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume1.ID,
		}
		err = store.db.Create(quotaRule).Error()
		assert.NoError(tt, err, "Failed to create quota rule")

		// Try to get with wrong volume ID
		result, err := store.GetQuotaRuleByUUID(context.Background(), "quota-rule-uuid", account.ID, volume2.ID)
		assert.Error(tt, err, "Expected error for wrong volume ID")
		assert.Nil(tt, result, "Expected nil result")
	})

	t.Run("WhenNoFiltersAreApplied", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
		}
		err = store.db.Create(quotaRule).Error()
		assert.NoError(tt, err, "Failed to create quota rule")

		// Get with zero account ID and volume ID (no filters)
		result, err := store.GetQuotaRuleByUUID(context.Background(), "quota-rule-uuid", 0, 0)
		assert.NoError(tt, err, "Expected no error when no filters applied")
		assert.NotNil(tt, result, "Expected quota rule to be found")
		assert.Equal(tt, quotaRule.UUID, result.UUID, "Expected quota rule UUID to match")
	})
}

func TestDeleteQuotaRule(t *testing.T) {
	t.Run("WhenQuotaRuleIsDeletedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
			State:          models.LifeCycleStateAvailable,
		}
		err = store.db.Create(quotaRule).Error()
		assert.NoError(tt, err, "Failed to create quota rule")

		deletedQuotaRule, err := store.DeleteQuotaRule(context.Background(), "quota-rule-uuid")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Nil(tt, deletedQuotaRule, "Expected nil return value")

		// Verify soft delete
		var softDeletedQuotaRule datamodel.QuotaRule
		err = store.db.Unscoped().Where("uuid = ?", "quota-rule-uuid").First(&softDeletedQuotaRule).Error()
		assert.NoError(tt, err, "Expected to find soft deleted quota rule")
		assert.NotNil(tt, softDeletedQuotaRule.DeletedAt, "Expected DeletedAt to be set")
		assert.True(tt, softDeletedQuotaRule.DeletedAt.Valid, "Expected DeletedAt to be valid")
		assert.Equal(tt, models.LifeCycleStateDeleted, softDeletedQuotaRule.State, "Expected state to be DELETED")
		assert.Equal(tt, models.LifeCycleStateDeletedDetails, softDeletedQuotaRule.StateDetails, "Expected state details to match")
	})

	t.Run("WhenQuotaRuleDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, err = store.DeleteQuotaRule(context.Background(), "non-existent-uuid")
		assert.Error(tt, err, "Expected error for non-existent quota rule")
		assert.Contains(tt, err.Error(), "quota rule", "Expected quota rule not found error")
	})
}

func TestGetQuotaRuleCountBySvmID(t *testing.T) {
	t.Run("WhenQuotaRulesExistForNFSVolumes", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		// Update volume to have NFS protocols
		volume.VolumeAttributes = &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFSv3, utils.ProtocolNFSv4},
		}
		err = store.db.Save(volume).Error()
		assert.NoError(tt, err, "Failed to update volume with NFS protocols")

		quotaRule1 := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid-1",
			},
			Name:           "quota-rule-1",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
		}
		err = store.db.Create(quotaRule1).Error()
		assert.NoError(tt, err, "Failed to create quota rule 1")

		quotaRule2 := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid-2",
			},
			Name:           "quota-rule-2",
			QuotaType:      "DEFAULT_USER_QUOTA",
			QuotaTarget:    "",
			DiskLimitInKib: 2097152,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
		}
		err = store.db.Create(quotaRule2).Error()
		assert.NoError(tt, err, "Failed to create quota rule 2")

		count, err := store.GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, int64(2), count, "Expected 2 quota rules, got %d", count)
	})

	t.Run("WhenNoQuotaRulesExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		// Update volume to have NFS protocols
		volume.VolumeAttributes = &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFSv3},
		}
		err = store.db.Save(volume).Error()
		assert.NoError(tt, err, "Failed to update volume with NFS protocols")

		count, err := store.GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, int64(0), count, "Expected 0 quota rules, got %d", count)
	})

	t.Run("WhenVolumesHaveNonNFSProtocols", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		// Update volume to have only SMB protocol (not NFS)
		volume.VolumeAttributes = &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolSMB},
		}
		err = store.db.Save(volume).Error()
		assert.NoError(tt, err, "Failed to update volume with SMB protocol")

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
		}
		err = store.db.Create(quotaRule).Error()
		assert.NoError(tt, err, "Failed to create quota rule")

		count, err := store.GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, int64(0), count, "Expected 0 quota rules for non-NFS volume, got %d", count)
	})

	t.Run("WhenMultipleVolumesWithMixedProtocols", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume1, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		// Update volume1 to have NFS protocols
		volume1.VolumeAttributes = &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFSv3},
		}
		err = store.db.Save(volume1).Error()
		assert.NoError(tt, err, "Failed to update volume1 with NFS protocols")

		// Create volume2 with SMB protocol
		volume2 := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-2-uuid"},
			Name:      "test_volume_2",
			AccountID: account.ID,
			PoolID:    volume1.PoolID,
			SvmID:     volume1.SvmID,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{utils.ProtocolSMB},
			},
		}
		err = store.db.Create(volume2).Error()
		assert.NoError(tt, err, "Failed to create volume2")

		// Create quota rule for NFS volume
		quotaRule1 := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid-1",
			},
			Name:           "quota-rule-1",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume1.ID,
		}
		err = store.db.Create(quotaRule1).Error()
		assert.NoError(tt, err, "Failed to create quota rule 1")

		// Create quota rule for SMB volume (should not be counted)
		quotaRule2 := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid-2",
			},
			Name:           "quota-rule-2",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "2000",
			DiskLimitInKib: 2097152,
			AccountID:      account.ID,
			VolumeID:       volume2.ID,
		}
		err = store.db.Create(quotaRule2).Error()
		assert.NoError(tt, err, "Failed to create quota rule 2")

		count, err := store.GetQuotaRuleCountBySvmID(context.Background(), volume1.SvmID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, int64(1), count, "Expected 1 quota rule for NFS volume, got %d", count)
	})

	t.Run("WhenQuotaRulesAreSoftDeleted", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		// Update volume to have NFS protocols
		volume.VolumeAttributes = &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFSv3},
		}
		err = store.db.Save(volume).Error()
		assert.NoError(tt, err, "Failed to update volume with NFS protocols")

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
		}
		err = store.db.Create(quotaRule).Error()
		assert.NoError(tt, err, "Failed to create quota rule")

		// Soft delete the quota rule
		_, err = store.DeleteQuotaRule(context.Background(), "quota-rule-uuid")
		assert.NoError(tt, err, "Failed to delete quota rule")

		count, err := store.GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, int64(0), count, "Expected 0 quota rules after soft delete, got %d", count)
	})

	t.Run("WhenVolumesAreSoftDeleted", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account, _, volume, err := CreateTestData(store)
		assert.NoError(tt, err, "Failed to create test data")

		// Update volume to have NFS protocols
		volume.VolumeAttributes = &datamodel.VolumeAttributes{
			Protocols: []string{utils.ProtocolNFSv3},
		}
		err = store.db.Save(volume).Error()
		assert.NoError(tt, err, "Failed to update volume with NFS protocols")

		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				UUID: "quota-rule-uuid",
			},
			Name:           "quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "1000",
			DiskLimitInKib: 1048576,
			AccountID:      account.ID,
			VolumeID:       volume.ID,
		}
		err = store.db.Create(quotaRule).Error()
		assert.NoError(tt, err, "Failed to create quota rule")

		// Soft delete the volume
		now := time.Now()
		volume.DeletedAt = &gorm.DeletedAt{Time: now, Valid: true}
		err = store.db.Save(volume).Error()
		assert.NoError(tt, err, "Failed to soft delete volume")

		count, err := store.GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, int64(0), count, "Expected 0 quota rules for soft deleted volume, got %d", count)
	})
}
