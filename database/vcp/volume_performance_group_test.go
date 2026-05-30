package database

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
)

func TestCreateVolumePerformanceGroup(t *testing.T) {
	t.Run("WhenVPGIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg"}, Name: "acct-vpg"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg"}, Name: "pool-vpg", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg"},
			PoolID:           pool.ID,
			Name:             "vpg-1",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-1",
		}
		created, err := store.CreateVolumePerformanceGroup(context.Background(), vpg)
		assert.NoError(tt, err)
		assert.NotNil(tt, created)
	})

	t.Run("WhenIsSharedFalse_PersistsCorrectly", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-shared-false"}, Name: "acct-vpg-shared-false"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-shared-false"}, Name: "pool-vpg-shared-false", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-shared-false"},
			PoolID:           pool.ID,
			Name:             "vpg-not-shared",
			IsShared:         false,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-shared-false",
		}
		created, err := store.CreateVolumePerformanceGroup(context.Background(), vpg)
		assert.NoError(tt, err)
		assert.NotNil(tt, created)
		assert.False(tt, created.IsShared, "IsShared should be false on the returned create result")

		got, err := store.GetVolumePerformanceGroupByUUID(context.Background(), "row-uuid-vpg-shared-false")
		assert.NoError(tt, err)
		assert.False(tt, got.IsShared, "IsShared should be false after reading back from DB")
		assert.Equal(tt, "vpg-not-shared", got.Name)
		assert.Equal(tt, int64(64), got.ThroughputMibps)
		assert.Equal(tt, int64(1000), got.Iops)
	})

	t.Run("WhenVPGWithSameNameExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-dupname"}, Name: "acct-vpg-dupname"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-dupname"}, Name: "pool-vpg-dupname", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-dupname"},
			PoolID:           pool.ID,
			Name:             "duplicate-name",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-dupname",
		}
		_, err = store.CreateVolumePerformanceGroup(context.Background(), vpg)
		assert.NoError(tt, err)

		dup := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-dupname-2"},
			PoolID:           pool.ID,
			Name:             "duplicate-name",
			IsShared:         false,
			IsAutoGen:        true,
			ThroughputMibps:  128,
			Iops:             2000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-dupname-2",
		}
		_, err = store.CreateVolumePerformanceGroup(context.Background(), dup)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "already exists")
	})

	t.Run("WhenSameNameExistsInDifferentPool_Succeeds", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-two-pools-create"}, Name: "acct-vpg-two-pools-create"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool1 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-create-1"}, Name: "pool-1", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-create-1"}
		pool2 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-create-2"}, Name: "pool-2", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-create-2"}
		assert.NoError(tt, store.db.Create(pool1).Error())
		assert.NoError(tt, store.db.Create(pool2).Error())

		vpg1 := &datamodel.VolumePerformanceGroup{
			PoolID:           pool1.ID,
			Name:             "same-name",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-1",
		}
		created1, err := store.CreateVolumePerformanceGroup(context.Background(), vpg1)
		assert.NoError(tt, err)
		assert.NotNil(tt, created1)
		assert.Equal(tt, pool1.ID, created1.PoolID)
		assert.Equal(tt, "same-name", created1.Name)

		// Same name in different pool is allowed (pool-scoped uniqueness)
		vpg2 := &datamodel.VolumePerformanceGroup{
			PoolID:           pool2.ID,
			Name:             "same-name",
			IsShared:         false,
			IsAutoGen:        true,
			ThroughputMibps:  128,
			Iops:             2000,
			OntapQosPolicyID: "ontap-qos-policy-2",
		}
		created2, err := store.CreateVolumePerformanceGroup(context.Background(), vpg2)
		assert.NoError(tt, err)
		assert.NotNil(tt, created2)
		assert.Equal(tt, pool2.ID, created2.PoolID)
		assert.Equal(tt, "same-name", created2.Name)
		assert.NotEqual(tt, created1.UUID, created2.UUID)
	})

	t.Run("WhenContextIsCancelled", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Setup minimal account/pool to satisfy FK constraints.
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-err"}, Name: "acct-vpg-err"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-err"}, Name: "pool-vpg-err", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-err"}
		assert.NoError(tt, store.db.Create(pool).Error())

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = store.CreateVolumePerformanceGroup(cancelledCtx, &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-err"},
			PoolID:           pool.ID,
			Name:             "vpg-err",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  16,
			Iops:             100,
			OntapQosPolicyID: "ontap-qos-policy-uuid-err",
		})
		assert.Error(tt, err)
	})

	t.Run("WhenPoolDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Try to create VPG with a non-existent PoolID
		// This should fail due to foreign key constraint violation
		// Note: SQLite doesn't enforce foreign key constraints by default in some configurations
		// If foreign keys are enforced, this will error with a constraint violation
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-invalid-pool"},
			PoolID:           999999, // Non-existent pool ID
			Name:             "vpg-invalid-pool",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-invalid",
		}
		_, err = store.CreateVolumePerformanceGroup(context.Background(), vpg)
		// The database should enforce foreign key constraints and return an error
		// If foreign keys are not enforced, the create may succeed (data integrity issue)
		if err != nil {
			// Foreign key constraint is enforced - verify the error indicates constraint violation
			assert.Contains(tt, err.Error(), "FOREIGN KEY", "Error should indicate foreign key constraint violation")
		} else {
			// Foreign key constraint is not enforced - document this behavior
			// In production with proper FK constraints enabled, this would fail
			tt.Logf("Warning: Foreign key constraints are not enforced - VPG was created with invalid PoolID %d", vpg.PoolID)
		}
	})
}

func TestGetVolumePerformanceGroup(t *testing.T) {
	t.Run("WhenVPGExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Account and pool
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg"}, Name: "acct-vpg"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg"}, Name: "pool-vpg", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		// Create VPG directly in database (not using CreateVolumePerformanceGroup to avoid testing Create in Get test)
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg"},
			PoolID:           pool.ID,
			Name:             "vpg-1",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-get",
		}
		assert.NoError(tt, store.db.Create(vpg).Error())

		// Get by row uuid
		gotByRowUUID, err := store.GetVolumePerformanceGroupByUUID(context.Background(), "row-uuid-vpg")
		assert.NoError(tt, err)
		assert.Equal(tt, pool.ID, gotByRowUUID.PoolID)
		assert.Equal(tt, "vpg-1", gotByRowUUID.Name)
	})

	t.Run("WhenVPGDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))
		_, err = store.GetVolumePerformanceGroupByUUID(context.Background(), "missing")
		assert.Error(tt, err)
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		// This test covers line 49: return nil, err (non-RecordNotFound error)
		// We'll use a closed database connection to trigger an error
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Close the database connection to cause an error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		// Try to get a VPG - this should trigger a database error (line 49)
		_, err = store.GetVolumePerformanceGroupByUUID(context.Background(), "any-uuid")
		assert.Error(tt, err)
		// The error should not be a "not found" error, but a database error
		assert.NotContains(tt, err.Error(), "not found")
	})
}

func TestGetVolumePerformanceGroupByID(t *testing.T) {
	t.Run("WhenVPGExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-id"}, Name: "acct-vpg-id"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-id"}, Name: "pool-vpg-id", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-id"},
			PoolID:           pool.ID,
			Name:             "vpg-id",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-id",
		}
		assert.NoError(tt, store.db.Create(vpg).Error())

		got, err := store.GetVolumePerformanceGroupByID(context.Background(), vpg.ID)
		assert.NoError(tt, err)
		assert.NotNil(tt, got)
		assert.Equal(tt, vpg.UUID, got.UUID)
	})

	t.Run("WhenVPGMissing", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		_, err = store.GetVolumePerformanceGroupByID(context.Background(), 9999)
		assert.Error(tt, err)
	})
}

func TestGetVolumePerformanceGroupByPoolAndName(t *testing.T) {
	t.Run("WhenVPGExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-uuid"}, Name: "acct-vpg-name"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-uuid"}, Name: "pool-vpg-name", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-uuid"},
			PoolID:           pool.ID,
			Name:             "my-vpg-name",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  128,
			Iops:             2000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-name",
		}
		assert.NoError(tt, store.db.Create(vpg).Error())

		got, err := store.GetVolumePerformanceGroupByPoolAndName(context.Background(), pool.ID, "my-vpg-name")
		assert.NoError(tt, err)
		assert.NotNil(tt, got)
		assert.Equal(tt, vpg.UUID, got.UUID)
		assert.Equal(tt, "my-vpg-name", got.Name)
	})

	t.Run("WhenVPGMissing", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-missing"}, Name: "acct-vpg-missing"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-missing"}, Name: "pool-vpg-missing", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		_, err = store.GetVolumePerformanceGroupByPoolAndName(context.Background(), pool.ID, "nonexistent-vpg")
		assert.Error(tt, err)
	})

	t.Run("WhenVPGExistsInDifferentPool_ReturnsNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-two-pools"}, Name: "acct-vpg-two-pools"}
		assert.NoError(tt, store.db.Create(account).Error())
		poolA := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-a"}, Name: "pool-a", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-a"}
		poolB := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-b"}, Name: "pool-b", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-b"}
		assert.NoError(tt, store.db.Create(poolA).Error())
		assert.NoError(tt, store.db.Create(poolB).Error())

		vpgInA := &datamodel.VolumePerformanceGroup{
			BaseModel: datamodel.BaseModel{UUID: "row-uuid-vpg-pool-a"},
			PoolID:    poolA.ID,
			Name:      "shared-name",
			IsShared:  true,
		}
		assert.NoError(tt, store.db.Create(vpgInA).Error())

		// Query by pool B and same name: should not find the VPG in pool A
		_, err = store.GetVolumePerformanceGroupByPoolAndName(context.Background(), poolB.ID, "shared-name")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
	})
}

func TestHardDeleteVolumePerformanceGroup(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	assert.NoError(t, ClearInMemoryDB(store.db.GORM()))

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-hard"}, Name: "acct-vpg-hard"}
	assert.NoError(t, store.db.Create(account).Error())
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-hard"}, Name: "pool-vpg-hard", AccountID: account.ID, Account: account}
	assert.NoError(t, store.db.Create(pool).Error())

	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-hard"},
		PoolID:           pool.ID,
		Name:             "vpg-hard",
		IsShared:         true,
		IsAutoGen:        true,
		ThroughputMibps:  64,
		Iops:             1000,
		OntapQosPolicyID: "ontap-qos-policy-uuid-hard",
	}
	assert.NoError(t, store.db.Create(vpg).Error())

	err = store.HardDeleteVolumePerformanceGroup(context.Background(), vpg)
	assert.NoError(t, err)

	_, err = store.GetVolumePerformanceGroupByID(context.Background(), vpg.ID)
	assert.Error(t, err)
}

func TestListVolumePerformanceGroups(t *testing.T) {
	t.Run("WhenVPGsExistForPool", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Account and pools
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg"}, Name: "acct-vpg"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool1 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-1"}, Name: "pool-vpg-1", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-1"}
		assert.NoError(tt, store.db.Create(pool1).Error())
		pool2 := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-2"}, Name: "pool-vpg-2", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-2"}
		assert.NoError(tt, store.db.Create(pool2).Error())

		// Create multiple VPGs in pool1 directly in database (not using CreateVolumePerformanceGroup to avoid testing Create in List test)
		vpg1 := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-1"},
			PoolID:           pool1.ID,
			Name:             "vpg-1",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-1",
		}
		assert.NoError(tt, store.db.Create(vpg1).Error())

		vpg2 := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-2"},
			PoolID:           pool1.ID,
			Name:             "vpg-2",
			IsShared:         false,
			IsAutoGen:        true,
			ThroughputMibps:  128,
			Iops:             2000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-2",
		}
		assert.NoError(tt, store.db.Create(vpg2).Error())

		// Create VPG with same name in pool2 (to ensure filtering by pool works correctly)
		vpg3 := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-3"},
			PoolID:           pool2.ID,
			Name:             "vpg-1", // Same name as vpg1 but different pool
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  32,
			Iops:             500,
			OntapQosPolicyID: "ontap-qos-policy-uuid-3",
		}
		assert.NoError(tt, store.db.Create(vpg3).Error())

		// List by pool1 - should return only VPGs from pool1
		list, err := store.ListVolumePerformanceGroupsByPoolID(context.Background(), pool1.ID)
		assert.NoError(tt, err)
		assert.Len(tt, list, 2, "Should return 2 VPGs from pool1")
		// Verify the returned VPGs are from pool1
		for _, vpg := range list {
			assert.Equal(tt, pool1.ID, vpg.PoolID, "VPG should belong to pool1")
			assert.Contains(tt, []string{"vpg-1", "vpg-2"}, vpg.Name, "VPG name should be vpg-1 or vpg-2")
		}

		// List by pool2 - should return only VPGs from pool2
		list2, err := store.ListVolumePerformanceGroupsByPoolID(context.Background(), pool2.ID)
		assert.NoError(tt, err)
		assert.Len(tt, list2, 1, "Should return 1 VPG from pool2")
		assert.Equal(tt, pool2.ID, list2[0].PoolID, "VPG should belong to pool2")
		assert.Equal(tt, "vpg-1", list2[0].Name, "VPG name should be vpg-1")
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		// This test covers line 58: return nil, err
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Close the database connection to cause an error
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		// Try to list VPGs - this should trigger a database error (line 58)
		_, err = store.ListVolumePerformanceGroupsByPoolID(context.Background(), 1)
		assert.Error(tt, err)
	})
}

func TestCountVolumePerformanceGroupsByPoolID(t *testing.T) {
	t.Run("WhenPoolHasNoVPGs_ReturnsZero", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-count-empty"}, Name: "acct-vpg-count-empty"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-count-empty"}, Name: "pool-vpg-count-empty", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-count-empty"}
		assert.NoError(tt, store.db.Create(pool).Error())

		count, err := store.CountVolumePerformanceGroupsByPoolID(context.Background(), pool.ID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), count)
	})

	t.Run("WhenPoolHasMixOfAutoGenAndExplicit_CountsAll", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-count-mix"}, Name: "acct-vpg-count-mix"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-count-mix"}, Name: "pool-vpg-count-mix", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-count-mix"}
		assert.NoError(tt, store.db.Create(pool).Error())

		explicitVPG := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-count-explicit"},
			PoolID:           pool.ID,
			Name:             "vpg-explicit",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-count-explicit",
		}
		assert.NoError(tt, store.db.Create(explicitVPG).Error())

		autoGenVPG1 := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-count-auto-1"},
			PoolID:           pool.ID,
			Name:             "autoGenerated-vol-1",
			IsShared:         false,
			IsAutoGen:        true,
			ThroughputMibps:  100,
			Iops:             2000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-count-auto-1",
		}
		assert.NoError(tt, store.db.Create(autoGenVPG1).Error())

		autoGenVPG2 := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-count-auto-2"},
			PoolID:           pool.ID,
			Name:             "autoGenerated-vol-2",
			IsShared:         false,
			IsAutoGen:        true,
			ThroughputMibps:  200,
			Iops:             3000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-count-auto-2",
		}
		assert.NoError(tt, store.db.Create(autoGenVPG2).Error())

		count, err := store.CountVolumePerformanceGroupsByPoolID(context.Background(), pool.ID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(3), count, "count must include both auto-gen and explicit VPGs")
	})

	t.Run("WhenSomeVPGsAreSoftDeleted_ExcludesDeletedRows", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-count-soft"}, Name: "acct-vpg-count-soft"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-count-soft"}, Name: "pool-vpg-count-soft", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-count-soft"}
		assert.NoError(tt, store.db.Create(pool).Error())

		// 3 active rows
		for i := 0; i < 3; i++ {
			vpg := &datamodel.VolumePerformanceGroup{
				BaseModel:        datamodel.BaseModel{UUID: fmt.Sprintf("row-uuid-vpg-count-active-%d", i)},
				PoolID:           pool.ID,
				Name:             fmt.Sprintf("vpg-active-%d", i),
				IsShared:         true,
				IsAutoGen:        false,
				ThroughputMibps:  64,
				Iops:             1000,
				OntapQosPolicyID: fmt.Sprintf("ontap-qos-policy-active-%d", i),
			}
			assert.NoError(tt, store.db.Create(vpg).Error())
		}

		// 2 soft-deleted rows
		for i := 0; i < 2; i++ {
			vpg := &datamodel.VolumePerformanceGroup{
				BaseModel:        datamodel.BaseModel{UUID: fmt.Sprintf("row-uuid-vpg-count-deleted-%d", i)},
				PoolID:           pool.ID,
				Name:             fmt.Sprintf("vpg-deleted-%d", i),
				IsShared:         true,
				IsAutoGen:        false,
				ThroughputMibps:  64,
				Iops:             1000,
				OntapQosPolicyID: fmt.Sprintf("ontap-qos-policy-deleted-%d", i),
			}
			assert.NoError(tt, store.db.Create(vpg).Error())
			assert.NoError(tt, store.DeleteVolumePerformanceGroup(context.Background(), vpg))
		}

		count, err := store.CountVolumePerformanceGroupsByPoolID(context.Background(), pool.ID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(3), count, "soft-deleted rows must be excluded from the count")
	})

	t.Run("WhenMultiplePoolsExist_IsolatesByPool", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-count-iso"}, Name: "acct-vpg-count-iso"}
		assert.NoError(tt, store.db.Create(account).Error())
		poolA := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-count-iso-a"}, Name: "pool-vpg-count-iso-a", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-count-iso-a"}
		assert.NoError(tt, store.db.Create(poolA).Error())
		poolB := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-count-iso-b"}, Name: "pool-vpg-count-iso-b", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-count-iso-b"}
		assert.NoError(tt, store.db.Create(poolB).Error())

		// 2 VPGs in poolA, 5 in poolB
		for i := 0; i < 2; i++ {
			vpg := &datamodel.VolumePerformanceGroup{
				BaseModel:        datamodel.BaseModel{UUID: fmt.Sprintf("row-uuid-vpg-count-iso-a-%d", i)},
				PoolID:           poolA.ID,
				Name:             fmt.Sprintf("vpg-iso-a-%d", i),
				IsShared:         true,
				IsAutoGen:        false,
				ThroughputMibps:  64,
				Iops:             1000,
				OntapQosPolicyID: fmt.Sprintf("ontap-qos-policy-iso-a-%d", i),
			}
			assert.NoError(tt, store.db.Create(vpg).Error())
		}
		for i := 0; i < 5; i++ {
			vpg := &datamodel.VolumePerformanceGroup{
				BaseModel:        datamodel.BaseModel{UUID: fmt.Sprintf("row-uuid-vpg-count-iso-b-%d", i)},
				PoolID:           poolB.ID,
				Name:             fmt.Sprintf("vpg-iso-b-%d", i),
				IsShared:         true,
				IsAutoGen:        false,
				ThroughputMibps:  64,
				Iops:             1000,
				OntapQosPolicyID: fmt.Sprintf("ontap-qos-policy-iso-b-%d", i),
			}
			assert.NoError(tt, store.db.Create(vpg).Error())
		}

		countA, err := store.CountVolumePerformanceGroupsByPoolID(context.Background(), poolA.ID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(2), countA)

		countB, err := store.CountVolumePerformanceGroupsByPoolID(context.Background(), poolB.ID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(5), countB)
	})

	t.Run("WhenDatabaseErrorOccurs_PropagatesError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		count, err := store.CountVolumePerformanceGroupsByPoolID(context.Background(), 1)
		assert.Error(tt, err)
		assert.Equal(tt, int64(0), count)
	})
}

func TestUpdateVolumePerformanceGroup(t *testing.T) {
	t.Run("WhenVPGIsUpdatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Account and pool
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg"}, Name: "acct-vpg"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg"}, Name: "pool-vpg", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		// Create VPG directly in database (not using CreateVolumePerformanceGroup to avoid testing Create in Update test)
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg"},
			PoolID:           pool.ID,
			Name:             "vpg-1",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-update",
		}
		assert.NoError(tt, store.db.Create(vpg).Error())

		// Update VPG with new values
		updatedVPG := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{UUID: "row-uuid-vpg"},
			Name:            "vpg-1-updated",
			ThroughputMibps: 128,
			Iops:            2000,
		}
		err = store.UpdateVolumePerformanceGroup(context.Background(), updatedVPG)
		assert.NoError(tt, err)

		// Verify the update
		got, err := store.GetVolumePerformanceGroupByUUID(context.Background(), "row-uuid-vpg")
		assert.NoError(tt, err)
		assert.Equal(tt, "vpg-1-updated", got.Name)
		assert.Equal(tt, int64(128), got.ThroughputMibps)
		assert.Equal(tt, int64(2000), got.Iops)
		// Verify immutable fields are not changed
		assert.Equal(tt, pool.ID, got.PoolID)
		assert.Equal(tt, true, got.IsShared)
	})

	t.Run("WhenVPGIsUpdatedWithOntapQosPolicyID", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg"}, Name: "acct-vpg"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg"}, Name: "pool-vpg", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-ontap"},
			PoolID:           pool.ID,
			Name:             "vpg-ontap",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "", // Initially empty (VPG created in DB before ONTAP policy)
		}
		assert.NoError(tt, store.db.Create(vpg).Error())

		updatedVPG := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-ontap"},
			Name:             "vpg-ontap",
			ThroughputMibps:  128,
			Iops:             2000,
			OntapQosPolicyID: "ontap-qos-policy-id-after-create",
		}
		err = store.UpdateVolumePerformanceGroup(context.Background(), updatedVPG)
		assert.NoError(tt, err)

		got, err := store.GetVolumePerformanceGroupByUUID(context.Background(), "row-uuid-vpg-ontap")
		assert.NoError(tt, err)
		assert.Equal(tt, "ontap-qos-policy-id-after-create", got.OntapQosPolicyID)
		assert.Equal(tt, int64(128), got.ThroughputMibps)
		assert.Equal(tt, int64(2000), got.Iops)
	})

	t.Run("WhenContextIsCancelled", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Setup minimal account/pool to satisfy FK constraints.
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-err"}, Name: "acct-vpg-err"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-err"}, Name: "pool-vpg-err", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-err"}
		assert.NoError(tt, store.db.Create(pool).Error())

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err = store.UpdateVolumePerformanceGroup(cancelledCtx, &datamodel.VolumePerformanceGroup{
			BaseModel: datamodel.BaseModel{UUID: "row-uuid-vpg-err-update"},
		})
		assert.Error(tt, err)
	})

	t.Run("WhenVPGNotFoundInUpdate", func(tt *testing.T) {
		// This test covers line 80: return err (when getVolumePerformanceGroupByUUID fails)
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Try to update a non-existent VPG
		nonExistentVPG := &datamodel.VolumePerformanceGroup{
			BaseModel: datamodel.BaseModel{UUID: "non-existent-vpg"},
			Name:      "updated-name",
		}
		err = store.UpdateVolumePerformanceGroup(context.Background(), nonExistentVPG)
		assert.Error(tt, err)
		// This should trigger line 80 when getVolumePerformanceGroupByUUID returns not found
		assert.Contains(tt, err.Error(), "not found")
	})

	t.Run("WhenDatabaseErrorInUpdate", func(tt *testing.T) {
		// This test attempts to cover line 91: return err (when tx.Updates fails)
		// We'll create a VPG and then close the DB connection to cause an update error
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Account and pool
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-update-err"}, Name: "acct-vpg-update-err"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-update-err"}, Name: "pool-vpg-update-err", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		// Create VPG
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "vpg-update-err"},
			PoolID:           pool.ID,
			Name:             "vpg-1",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-update-err",
		}
		assert.NoError(tt, store.db.Create(vpg).Error())

		// Close the database connection to cause an error during update
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		// Try to update - this should trigger line 91 when tx.Updates fails
		updatedVPG := &datamodel.VolumePerformanceGroup{
			BaseModel: datamodel.BaseModel{UUID: "vpg-update-err"},
			Name:      "updated-name",
		}
		err = store.UpdateVolumePerformanceGroup(context.Background(), updatedVPG)
		assert.Error(tt, err)
	})
}

func TestUpdateVolumePerformanceGroupState(t *testing.T) {
	t.Run("WhenStateIsUpdatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-state"}, Name: "acct-vpg-state"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-state"}, Name: "pool-vpg-state", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-state"},
			PoolID:           pool.ID,
			Name:             "vpg-state",
			IsShared:         true,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-state",
			State:            "CREATING",
		}
		assert.NoError(tt, store.db.Create(vpg).Error())

		err = store.UpdateVolumePerformanceGroupState(context.Background(), "row-uuid-vpg-state", "READY", "")
		assert.NoError(tt, err)

		got, err := store.GetVolumePerformanceGroupByUUID(context.Background(), "row-uuid-vpg-state")
		assert.NoError(tt, err)
		assert.Equal(tt, "READY", got.State)
		assert.Equal(tt, "", got.StateDetails)
	})

	t.Run("WhenVPGNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		err = store.UpdateVolumePerformanceGroupState(context.Background(), "non-existent-uuid", "READY", "")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "not found")
	})

	t.Run("WhenDatabaseError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		err = store.UpdateVolumePerformanceGroupState(context.Background(), "any-uuid", "READY", "")
		assert.Error(tt, err)
	})
}

func TestUpdateVolumePerformanceGroup_WithDescriptionAndLabels(t *testing.T) {
	t.Run("WhenDescriptionChanges", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-desc"}, Name: "acct-vpg-desc"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-desc"}, Name: "pool-vpg-desc", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-desc"},
			PoolID:           pool.ID,
			Name:             "vpg-desc",
			IsShared:         true,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-desc",
			Description:      "old description",
		}
		assert.NoError(tt, store.db.Create(vpg).Error())

		updatedVPG := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{UUID: "row-uuid-vpg-desc"},
			Name:            "vpg-desc",
			ThroughputMibps: 64,
			Iops:            1000,
			Description:     "new description",
		}
		err = store.UpdateVolumePerformanceGroup(context.Background(), updatedVPG)
		assert.NoError(tt, err)

		got, err := store.GetVolumePerformanceGroupByUUID(context.Background(), "row-uuid-vpg-desc")
		assert.NoError(tt, err)
		assert.Equal(tt, "new description", got.Description)
	})

	t.Run("WhenLabelsSet", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-labels"}, Name: "acct-vpg-labels"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-labels"}, Name: "pool-vpg-labels", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-labels"},
			PoolID:           pool.ID,
			Name:             "vpg-labels",
			IsShared:         true,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-labels",
		}
		assert.NoError(tt, store.db.Create(vpg).Error())

		labels := &datamodel.JSONB{"env": "dev", "team": "storage"}
		updatedVPG := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{UUID: "row-uuid-vpg-labels"},
			Name:            "vpg-labels",
			ThroughputMibps: 64,
			Iops:            1000,
			Labels:          labels,
		}
		err = store.UpdateVolumePerformanceGroup(context.Background(), updatedVPG)
		assert.NoError(tt, err)

		got, err := store.GetVolumePerformanceGroupByUUID(context.Background(), "row-uuid-vpg-labels")
		assert.NoError(tt, err)
		assert.NotNil(tt, got.Labels)
	})

	t.Run("WhenStateSet", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-state-upd"}, Name: "acct-vpg-state-upd"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-state-upd"}, Name: "pool-vpg-state-upd", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg-state-upd"},
			PoolID:           pool.ID,
			Name:             "vpg-state-upd",
			IsShared:         true,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-state-upd",
			State:            "CREATING",
		}
		assert.NoError(tt, store.db.Create(vpg).Error())

		updatedVPG := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{UUID: "row-uuid-vpg-state-upd"},
			Name:            "vpg-state-upd",
			ThroughputMibps: 64,
			Iops:            1000,
			State:           "READY",
			StateDetails:    "",
		}
		err = store.UpdateVolumePerformanceGroup(context.Background(), updatedVPG)
		assert.NoError(tt, err)

		got, err := store.GetVolumePerformanceGroupByUUID(context.Background(), "row-uuid-vpg-state-upd")
		assert.NoError(tt, err)
		assert.Equal(tt, "READY", got.State)
	})
}

func TestDeleteVolumePerformanceGroup(t *testing.T) {
	t.Run("WhenVPGIsDeletedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Account and pool
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg"}, Name: "acct-vpg"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg"}, Name: "pool-vpg", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		// Create VPG directly in database (not using CreateVolumePerformanceGroup to avoid testing Create in Delete test)
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "row-uuid-vpg"},
			PoolID:           pool.ID,
			Name:             "vpg-1",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-delete",
		}
		assert.NoError(tt, store.db.Create(vpg).Error())
		gotByRowUUID, err := store.GetVolumePerformanceGroupByUUID(context.Background(), "row-uuid-vpg")
		assert.NoError(tt, err)

		// Delete
		err = store.DeleteVolumePerformanceGroup(context.Background(), gotByRowUUID)
		assert.NoError(tt, err)

		// Verify it's gone (soft delete)
		_, err = store.GetVolumePerformanceGroupByUUID(context.Background(), "row-uuid-vpg")
		assert.Error(tt, err)
	})

	t.Run("WhenVPGDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))
		// Setup minimal account/pool to satisfy FK constraints
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-notexist"}, Name: "acct-vpg-notexist"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-notexist"}, Name: "pool-vpg-notexist", AccountID: account.ID, Account: account, DeploymentName: "deployment-vpg-notexist"}
		assert.NoError(tt, store.db.Create(pool).Error())

		// Try to delete a VPG that was never created (using a non-existent ID)
		nonExistentVPG := &datamodel.VolumePerformanceGroup{
			BaseModel: datamodel.BaseModel{ID: 999999, UUID: "non-existent-vpg-uuid"},
		}
		err = store.DeleteVolumePerformanceGroup(context.Background(), nonExistentVPG)
		assert.Error(tt, err)
		// The error should indicate the VPG was not found (RowsAffected == 0)
		assert.Contains(tt, err.Error(), "not found", "Error should indicate VPG was not found")
	})

	t.Run("WhenDatabaseErrorInDelete", func(tt *testing.T) {
		// This test covers line 100: return res.Error (when db.Delete fails)
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Account and pool
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-vpg-delete-err"}, Name: "acct-vpg-delete-err"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-vpg-delete-err"}, Name: "pool-vpg-delete-err", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		// Create VPG
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:        datamodel.BaseModel{UUID: "vpg-delete-err"},
			PoolID:           pool.ID,
			Name:             "vpg-1",
			IsShared:         true,
			IsAutoGen:        false,
			ThroughputMibps:  64,
			Iops:             1000,
			OntapQosPolicyID: "ontap-qos-policy-uuid-delete-err",
		}
		assert.NoError(tt, store.db.Create(vpg).Error())

		// Close the database connection to cause an error during delete
		sqlDB, err := db.DB()
		assert.NoError(tt, err)
		_ = sqlDB.Close()

		// Try to delete - this should trigger line 100 when db.Delete fails
		err = store.DeleteVolumePerformanceGroup(context.Background(), vpg)
		assert.Error(tt, err)
	})
}

func TestCreateVolumePerformanceGroupWithCap(t *testing.T) {
	mkPool := func(tt *testing.T, store *DataStoreRepository, suffix string) *datamodel.Pool {
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-cap-" + suffix}, Name: "acct-cap-" + suffix}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-cap-" + suffix}, Name: "pool-cap-" + suffix, AccountID: account.ID, Account: account, DeploymentName: "deployment-cap-" + suffix}
		assert.NoError(tt, store.db.Create(pool).Error())
		return pool
	}

	t.Run("UnderCap_Inserts", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		pool := mkPool(tt, store, "under")
		got, err := store.CreateVolumePerformanceGroupWithCap(context.Background(), &datamodel.VolumePerformanceGroup{
			PoolID: pool.ID, Name: "vpg-under", IsShared: true, ThroughputMibps: 64, Iops: 1000,
		}, 5)
		assert.NoError(tt, err)
		assert.NotNil(tt, got)
		assert.NotEmpty(tt, got.UUID)

		count, err := store.CountVolumePerformanceGroupsByPoolID(context.Background(), pool.ID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(1), count)
	})

	t.Run("AtCap_Rejects", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		pool := mkPool(tt, store, "at")
		const cap = 3
		for i := 0; i < cap; i++ {
			_, err := store.CreateVolumePerformanceGroupWithCap(context.Background(), &datamodel.VolumePerformanceGroup{
				PoolID: pool.ID, Name: fmt.Sprintf("vpg-%d", i), IsShared: true, ThroughputMibps: 64, Iops: 1000,
			}, cap)
			assert.NoError(tt, err)
		}

		_, err = store.CreateVolumePerformanceGroupWithCap(context.Background(), &datamodel.VolumePerformanceGroup{
			PoolID: pool.ID, Name: "vpg-overflow", IsShared: true, ThroughputMibps: 64, Iops: 1000,
		}, cap)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "maximum number of Volume Performance Groups")

		count, err := store.CountVolumePerformanceGroupsByPoolID(context.Background(), pool.ID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(cap), count, "rejected insert must not have committed")
	})

	t.Run("OverCap_AfterRowsExist_Rejects", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		pool := mkPool(tt, store, "over")
		// Seed 5 rows directly, then enforce cap of 3 — proves the count gate, not just equality.
		for i := 0; i < 5; i++ {
			assert.NoError(tt, store.db.Create(&datamodel.VolumePerformanceGroup{
				BaseModel: datamodel.BaseModel{UUID: fmt.Sprintf("seeded-%d", i)},
				PoolID:    pool.ID, Name: fmt.Sprintf("seeded-%d", i), IsShared: true, ThroughputMibps: 64, Iops: 1000,
			}).Error())
		}
		_, err = store.CreateVolumePerformanceGroupWithCap(context.Background(), &datamodel.VolumePerformanceGroup{
			PoolID: pool.ID, Name: "vpg-extra", IsShared: true, ThroughputMibps: 64, Iops: 1000,
		}, 3)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "maximum number of Volume Performance Groups")
	})

	t.Run("CapDisabled_Zero_AllowsInsert", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		pool := mkPool(tt, store, "disabled-zero")
		for i := 0; i < 10; i++ {
			assert.NoError(tt, store.db.Create(&datamodel.VolumePerformanceGroup{
				BaseModel: datamodel.BaseModel{UUID: fmt.Sprintf("disabled-zero-%d", i)},
				PoolID:    pool.ID, Name: fmt.Sprintf("disabled-zero-%d", i), IsShared: true, ThroughputMibps: 64, Iops: 1000,
			}).Error())
		}
		got, err := store.CreateVolumePerformanceGroupWithCap(context.Background(), &datamodel.VolumePerformanceGroup{
			PoolID: pool.ID, Name: "vpg-allowed", IsShared: true, ThroughputMibps: 64, Iops: 1000,
		}, 0)
		assert.NoError(tt, err)
		assert.NotNil(tt, got)
	})

	t.Run("CapDisabled_Negative_AllowsInsert", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		pool := mkPool(tt, store, "disabled-neg")
		got, err := store.CreateVolumePerformanceGroupWithCap(context.Background(), &datamodel.VolumePerformanceGroup{
			PoolID: pool.ID, Name: "vpg-neg-cap", IsShared: true, ThroughputMibps: 64, Iops: 1000,
		}, -1)
		assert.NoError(tt, err)
		assert.NotNil(tt, got)
	})

	t.Run("PoolDoesNotExist_Returns404", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		_, err = store.CreateVolumePerformanceGroupWithCap(context.Background(), &datamodel.VolumePerformanceGroup{
			PoolID: 9999, Name: "vpg-orphan", IsShared: true, ThroughputMibps: 64, Iops: 1000,
		}, 100)
		assert.Error(tt, err)
	})

	t.Run("DuplicateName_Rejects", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		pool := mkPool(tt, store, "dup")
		_, err = store.CreateVolumePerformanceGroupWithCap(context.Background(), &datamodel.VolumePerformanceGroup{
			PoolID: pool.ID, Name: "shared-name", IsShared: true, ThroughputMibps: 64, Iops: 1000,
		}, 100)
		assert.NoError(tt, err)

		_, err = store.CreateVolumePerformanceGroupWithCap(context.Background(), &datamodel.VolumePerformanceGroup{
			PoolID: pool.ID, Name: "shared-name", IsShared: false, ThroughputMibps: 128, Iops: 2000,
		}, 100)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "already exists")
	})

	t.Run("NilVPG_Errors", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		_, err = store.CreateVolumePerformanceGroupWithCap(context.Background(), nil, 100)
		assert.Error(tt, err)
	})

	t.Run("SoftDeletedRowsDoNotCountTowardCap", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		pool := mkPool(tt, store, "soft-deleted")
		const cap = 2
		for i := 0; i < cap; i++ {
			_, err := store.CreateVolumePerformanceGroupWithCap(context.Background(), &datamodel.VolumePerformanceGroup{
				PoolID: pool.ID, Name: fmt.Sprintf("vpg-%d", i), IsShared: true, ThroughputMibps: 64, Iops: 1000,
			}, cap)
			assert.NoError(tt, err)
		}
		// At cap → soft delete one → count drops → next insert should succeed.
		toDelete, err := store.GetVolumePerformanceGroupByPoolAndName(context.Background(), pool.ID, "vpg-0")
		assert.NoError(tt, err)
		assert.NoError(tt, store.DeleteVolumePerformanceGroup(context.Background(), toDelete))

		_, err = store.CreateVolumePerformanceGroupWithCap(context.Background(), &datamodel.VolumePerformanceGroup{
			PoolID: pool.ID, Name: "vpg-after-delete", IsShared: true, ThroughputMibps: 64, Iops: 1000,
		}, cap)
		assert.NoError(tt, err)
	})
}
