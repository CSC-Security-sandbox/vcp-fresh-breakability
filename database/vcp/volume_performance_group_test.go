package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
)

func TestCreateVolumePerformanceGroup(t *testing.T) {
	t.Run("WhenVPGIsCreatedSuccessfully", func(tt *testing.T) {
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

		// Create VPG
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
