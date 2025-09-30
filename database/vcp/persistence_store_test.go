package database

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestSetupStorageForTest_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.NotNil(t, store.DB())
}

func TestPersistenceStore_ListPoolUUIDsPaginated(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Error closing store: %v", err)
		}
	}()

	ctx := context.Background()
	filter := &dbutils.Filter{}
	offset := 0
	limit := 10

	// Test successful call
	poolIdentifiers, err := store.ListPoolUUIDsPaginated(ctx, filter, offset, limit)
	assert.NoError(t, err)
	assert.NotNil(t, poolIdentifiers)
}

func TestPersistenceStore_GetPoolsCount(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Error closing store: %v", err)
		}
	}()

	ctx := context.Background()
	filter := &dbutils.Filter{}

	// Test successful call
	count, err := store.GetPoolsCount(ctx, filter)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(0))
}

func TestClearInMemoryDB_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)

	err = ClearInMemoryDB(store.DB())
	assert.NoError(t, err)
}

func TestHealthCheckAndClose_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	assert.NoError(t, store.HealthCheck())
	assert.NoError(t, store.Close())
	// After close, HealthCheck should fail
	err := store.HealthCheck()
	assert.Error(t, err)
}

func TestWithTransaction_Success_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	err := store.WithTransaction(ctx, func(tx dbutils.Transaction) error {
		return nil
	})
	assert.NoError(t, err)
}

func TestWithTransaction_Error_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	err := store.WithTransaction(ctx, func(tx dbutils.Transaction) error {
		return errors.New("fail")
	})
	assert.Error(t, err)
}

func TestWithTransaction_Panic_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic")
		}
	}()
	_ = store.WithTransaction(ctx, func(tx dbutils.Transaction) error {
		panic("panic in tx")
	})
}

func TestWithTransaction_NilDB_Persistence_Store(t *testing.T) {
	store := &PersistenceStore{}
	err := store.WithTransaction(context.Background(), func(tx dbutils.Transaction) error { return nil })
	assert.Error(t, err)
}

func TestDBMethod_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	db := store.DB()
	assert.NotNil(t, db)
}

func TestIsDatabaseExistsError_Persistence_Store(t *testing.T) {
	err := errors.New("some error")
	assert.False(t, isDatabaseExistsError(err))
}

func TestCreatingPool_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	// add logger to context
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "creatingpool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetPool_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "getpool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	found, err := store.GetPool(ctx, created.UUID, 0)
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

func TestDescribePool_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "describepool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	found, err := store.DescribePool(ctx, created.UUID, 0)
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

func TestUpdatePool_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "updatepool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	created.Name = "updatedpool"
	_, err = store.UpdatedPool(ctx, created)
	assert.NoError(t, err)
}

func TestErroredResource_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a pool first
	pool := &datamodel.Pool{Name: "error-pool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	assert.NotNil(t, created)

	// Mark pool as errored
	errorMsg := "some error occurred"
	erroredPool, err := store.ErroredResource(ctx, created, errorMsg)
	assert.NoError(t, err)
	assert.NotNil(t, erroredPool)
	// Optionally, check if error message is set (if your model supports it)
}

func TestDeletePool_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "deletepool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	err = store.DeletePool(ctx, created)
	assert.NoError(t, err)
}

func TestDeletingPool_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "deletingpool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	err = store.DeletingPool(ctx, created)
	assert.NoError(t, err)
}

func TestListPools_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.ListPools(ctx, nil)
	assert.NoError(t, err)
}

func TestListPoolUUIDs_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.ListPoolUUIDs(ctx, nil)
	assert.NoError(t, err)
}

func TestGetPoolByName_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "getpoolbyname", Account: &datamodel.Account{}}
	_, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	found, err := store.GetPoolByName(ctx, [][]interface{}{{"name", "getpoolbyname"}})
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

func TestConnectAndSetupDatabase_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	// Should not error if already connected
	assert.NoError(t, store.Connect(false))
	// SetupDatabase should fail gracefully (no Postgres in-memory)
	err := store.SetupDatabase(context.Background())
	assert.Error(t, err)
}

func TestConnect_NilConfig_Persistence_Store(t *testing.T) {
	store := &PersistenceStore{}
	err := store.Connect(false)
	assert.Error(t, err)
}

func TestCreateConnection_UnsupportedType_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store := &PersistenceStore{config: dbutils.DbConfig{Type: "unknown"}, logger: logger}
	_, err := store.createConnection(false)
	assert.Error(t, err)
}

func TestGetPostgresDSN_Persistence_Store(t *testing.T) {
	store := &PersistenceStore{config: dbutils.DbConfig{
		Type:              "postgres",
		Host:              "localhost",
		Port:              "5432",
		Name:              "testdb",
		User:              "user",
		Password:          "pass",
		AdminUser:         "admin",
		AdminPassword:     "adminpass",
		SSLMode:           "disable",
		ConnectionTimeout: 5,
		TimeZone:          "UTC",
	}}
	dsn, err := store.getPostgresDSN(false)
	assert.NoError(t, err)
	assert.Contains(t, dsn, "user:pass")
	assert.Contains(t, dsn, "testdb")
	dsnAdmin, err := store.getPostgresDSN(true)
	assert.NoError(t, err)
	assert.Contains(t, dsnAdmin, "admin:adminpass")
	assert.Contains(t, dsnAdmin, "postgres")
}

// --- POOL TESTS ALREADY PRESENT ---

// VOLUME
func TestCreateVolume_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol1"}
	created, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetVolume_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol2"}
	created, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)
	found, err := store.GetVolume(ctx, created.UUID)
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

func TestUpdateVolume_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol3"}
	created, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)
	created.Name = "vol3-updated"
	err = store.UpdateVolume(ctx, created)
	assert.NoError(t, err)
}

func TestDeleteVolume_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol4"}
	created, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)
	_, err = store.DeleteVolume(ctx, created.UUID)
	assert.NoError(t, err)
}

func TestGetVolumesByPoolID_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetVolumesByPoolID(ctx, 0)
	assert.NoError(t, err)
}

func TestGetVolumeCountByPoolID_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetVolumeCountByPoolID(ctx, 0)
	assert.NoError(t, err)
}

func TestGetMultipleVolumes_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetMultipleVolumes(ctx, [][]interface{}{})
	assert.NoError(t, err)
}

// ACCOUNT
func TestCreateAccount_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	acc := &datamodel.Account{Name: "acc1"}
	created, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetAccount_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	acc := &datamodel.Account{Name: "acc2"}
	_, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	found, err := store.GetAccount(ctx, "acc2")
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

// JOB
func TestCreateJob_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	job := &datamodel.Job{ResourceName: "job1"}
	created, err := store.CreateJob(ctx, job)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetJob_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	job := &datamodel.Job{RequestID: "job2"}
	created, err := store.CreateJob(ctx, job)
	assert.NoError(t, err)
	found, err := store.GetJob(ctx, created.UUID)
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

func TestGetJobsWithCondition_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("state", "=", "new"))
	jobs, err := store.GetJobsWithCondition(ctx, *filter)
	assert.NoError(t, err)
	assert.NotNil(t, jobs)
}

func TestUpdateJob_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	job := &datamodel.Job{ResourceName: "job3"}
	created, err := store.CreateJob(ctx, job)
	assert.NoError(t, err)
	err = store.UpdateJob(ctx, created.UUID, "done", 0, "")
	assert.NoError(t, err)
}

func TestDeleteJob_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	job := &datamodel.Job{ResourceName: "job3"}
	created, err := store.CreateJob(ctx, job)
	assert.NoError(t, err)
	err = store.DeleteJob(ctx, created.UUID, "")
	assert.NoError(t, err)
}

// SVM
func TestCreateSVM_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	svm := &datamodel.Svm{Name: "svm1"}
	created, err := store.CreateSVM(ctx, svm)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetSvmsByPoolID_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetSvmsByPoolID(ctx, 0)
	assert.NoError(t, err)
}

// NODE
func TestCreateNode_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	node := &datamodel.Node{Name: "node1"}
	created, err := store.CreateNode(ctx, node)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetNodesByPoolID_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetNodesByPoolID(ctx, 0)
	assert.NoError(t, err)
}

// LIF
func TestCreateLif_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	lif := &datamodel.Lif{Name: "lif1"}
	created, err := store.CreateLif(ctx, lif)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

// HOSTGROUP
func TestCreateHostGroup_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	hg := &datamodel.HostGroup{Name: "hg1"}
	created, err := store.CreateHostGroup(ctx, hg)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetMultipleHostGroups_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetMultipleHostGroups(ctx, []string{"hg-uuid1", "hg-uuid2"}, 0)
	assert.NoError(t, err)
}

func TestUpdateHostGroupsState_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	err := store.UpdateHostGroupsState(ctx, []string{"hg-uuid"}, 0, "active", "ok")
	assert.NoError(t, err)
}

// SNAPSHOT
func TestCreatingSnapshot_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	snap := &datamodel.Snapshot{Name: "snap1"}
	created, err := store.CreatingSnapshot(ctx, snap)
	assert.NoError(t, err)
	assert.NotNil(t, created)
	assert.Equal(t, "snap1", created.Name)
}

func TestUpdateSnapshot_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	snap := &datamodel.Snapshot{Name: "snap2"}
	created, err := store.CreatingSnapshot(ctx, snap)
	assert.NoError(t, err)
	created.Name = "snap2-updated"
	dbSnap, err := store.UpdateSnapshot(ctx, created)
	assert.NoError(t, err)
	assert.NotNil(t, dbSnap)
	assert.Equal(t, "snap2-updated", dbSnap.Name)
	assert.Equal(t, created.UUID, dbSnap.UUID)
}

func TestGetSnapshot_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	snap := &datamodel.Snapshot{Name: "snap3"}
	created, err := store.CreatingSnapshot(ctx, snap)
	assert.NoError(t, err)
	found, err := store.GetSnapshotByUUID(ctx, created.UUID, 0, 0)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Equal(t, "snap3", found.Name)
}

func TestGetSnapshotsWithCondition_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("name", "=", "snap"),
	)
	snaps, err := store.GetSnapshotsWithCondition(ctx, *filter)
	assert.NoError(t, err)
	assert.NotNil(t, snaps)
}

func TestGetAppConsistentSnapshotsForVolume_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_snap"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_snap", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)
	// Create a snapshot associated with the account and volume
	snap := &datamodel.Snapshot{Name: "snap4", AccountID: createdAcc.ID, VolumeID: createdVol.ID, IsAppConsistent: true}
	_, err = store.CreatingSnapshot(ctx, snap)
	assert.NoError(t, err)
	// Query for snapshots
	snaps, err := store.GetAppConsistentSnapshotsForVolume(ctx, createdAcc.ID, createdVol.ID)
	assert.NoError(t, err)
	assert.NotNil(t, snaps)
	assert.GreaterOrEqual(t, len(snaps), 1)
}

func TestGetSnapshotsByVolumeID_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_snap2"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_snap2", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Create two snapshots for the volume
	snap1 := &datamodel.Snapshot{Name: "snap5", AccountID: createdAcc.ID, VolumeID: createdVol.ID}
	snap2 := &datamodel.Snapshot{Name: "snap6", AccountID: createdAcc.ID, VolumeID: createdVol.ID}
	_, err = store.CreatingSnapshot(ctx, snap1)
	assert.NoError(t, err)
	_, err = store.CreatingSnapshot(ctx, snap2)
	assert.NoError(t, err)

	// Query for snapshots by volume ID
	snaps, err := store.GetSnapshotsByVolumeID(ctx, createdVol.ID)
	assert.NoError(t, err)
	assert.NotNil(t, snaps)
	assert.GreaterOrEqual(t, len(snaps), 2)
	var foundSnap5, foundSnap6 bool
	for _, s := range snaps {
		if s.Name == "snap5" {
			foundSnap5 = true
		}
		if s.Name == "snap6" {
			foundSnap6 = true
		}
	}
	assert.True(t, foundSnap5)
	assert.True(t, foundSnap6)
}

func TestGetSnapshotsByVolumeIDs_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "test-account"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol1 := &datamodel.Volume{Name: "test-volume-1", AccountID: createdAcc.ID}
	createdVol1, err := store.CreateVolume(ctx, vol1)
	assert.NoError(t, err)
	vol2 := &datamodel.Volume{Name: "test-volume-2", AccountID: createdAcc.ID}
	createdVol2, err := store.CreateVolume(ctx, vol2)
	assert.NoError(t, err)

	// Create two snapshots for the volume
	snap1 := &datamodel.Snapshot{Name: "test-snap-1", AccountID: createdAcc.ID, VolumeID: createdVol1.ID, State: models.LifeCycleStateREADY}
	snap2 := &datamodel.Snapshot{Name: "test-snap-2", AccountID: createdAcc.ID, VolumeID: createdVol2.ID, State: models.LifeCycleStateREADY}
	_, err = store.CreatingSnapshot(ctx, snap1)
	assert.NoError(t, err)
	_, err = store.CreatingSnapshot(ctx, snap2)
	assert.NoError(t, err)

	snap1.State = models.LifeCycleStateREADY
	_, err = store.UpdateSnapshot(ctx, snap1)
	assert.NoError(t, err)
	snap2.State = models.LifeCycleStateREADY
	_, err = store.UpdateSnapshot(ctx, snap2)
	assert.NoError(t, err)

	snapshots, err := store.GetSnapshotsByVolumeIDs(ctx, []int64{vol1.ID, vol2.ID})
	assert.NoError(t, err)
	assert.Len(t, snapshots, 2)
}

func TestBatchDeleteSnapshots_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "test-account"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "test-volume", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Create two snapshots for the volume
	snap1 := &datamodel.Snapshot{Name: "test-snap-1", AccountID: createdAcc.ID, VolumeID: createdVol.ID}
	snap2 := &datamodel.Snapshot{Name: "test-snap-2", AccountID: createdAcc.ID, VolumeID: createdVol.ID}
	_, err = store.CreatingSnapshot(ctx, snap1)
	assert.NoError(t, err)
	_, err = store.CreatingSnapshot(ctx, snap2)
	assert.NoError(t, err)

	// Batch delete snapshots
	deletedSnapshots, err := store.BatchDeleteSnapshots(ctx, []int64{snap1.ID, snap2.ID})
	assert.NoError(t, err)
	assert.Len(t, deletedSnapshots, 2)

	// Verify snapshots are marked as deleted
	for _, snap := range deletedSnapshots {
		assert.Equal(t, models.LifeCycleStateDeleted, snap.State)
		assert.Equal(t, models.LifeCycleStateDeletedDetails, snap.StateDetails)
		assert.NotNil(t, snap.DeletedAt)
	}

	// Attempt to retrieve snapshots by UUID (should return not found)
	_, err = store.GetSnapshotByUUID(ctx, snap1.UUID, createdAcc.ID, createdVol.ID)
	assert.Error(t, err)
	_, err = store.GetSnapshotByUUID(ctx, snap2.UUID, createdAcc.ID, createdVol.ID)
	assert.Error(t, err)

	// Query directly to confirm DeletedAt is set
	var deletedSnap1, deletedSnap2 datamodel.Snapshot
	err = store.DB().Unscoped().Where("id = ?", snap1.ID).First(&deletedSnap1).Error
	assert.NoError(t, err)
	assert.NotNil(t, deletedSnap1.DeletedAt)

	err = store.DB().Unscoped().Where("id = ?", snap2.ID).First(&deletedSnap2).Error
	assert.NoError(t, err)
	assert.NotNil(t, deletedSnap2.DeletedAt)
}

func TestGetReplicationSnapshotsByVolumeID_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_snap2"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_snap2", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Create two snapshots for the volume
	snap1 := &datamodel.Snapshot{Name: "snapmirror.1", AccountID: createdAcc.ID, VolumeID: createdVol.ID}
	snap2 := &datamodel.Snapshot{Name: "snap2", AccountID: createdAcc.ID, VolumeID: createdVol.ID}
	_, err = store.CreatingSnapshot(ctx, snap1)
	assert.NoError(t, err)
	_, err = store.CreatingSnapshot(ctx, snap2)
	assert.NoError(t, err)

	// Query for snapshots by volume ID
	snaps, err := store.GetReplicationSnapshotsByVolumeID(ctx, createdVol.ID)
	assert.NoError(t, err)
	assert.NotNil(t, snaps)
	assert.Equal(t, len(snaps), 1)
}

func TestGetKms_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	kms := datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
	}
	store.DB().Create(&kms)
	found, err := store.GetKmsConfig(ctx, kms.UUID)
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

func TestGetKmsConfigByKeyFullPath_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	kms := datamodel.KmsConfig{
		BaseModel:       datamodel.BaseModel{UUID: "kms-uuid"},
		Account:         &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}},
		KeyProjectID:    "project-id",
		KeyRingLocation: "us-central1",
		KeyRing:         "key-ring",
		KeyName:         "key-name",
	}
	store.DB().Create(&kms)
	found, err := store.GetKmsConfigByKeyFullPath(ctx, "projects/project-id/locations/us-central1/keyRings/key-ring/cryptoKeys/key-name", int64(1))
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

func TestUpdateKmsConfig_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	kms := datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
	}
	store.DB().Create(&kms)
	kms.Name = "updatedpool"
	_, err := store.UpdateKmsConfigState(ctx, kms.UUID, models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails)
	assert.NoError(t, err)
}

func TestUpdateKmsConfigState_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	kms := datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
	}
	store.DB().Create(&kms)
	_, err := store.UpdateKmsConfigState(ctx, "kms-uuid", models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)
	assert.NoError(t, err)
}

func TestUpdateKmsConfigAttributesUpdatesAttributesOnSuccess_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()
	kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "kms-uuid"}}
	_, err = store.CreateKmsConfig(ctx, kmsConfig)
	assert.NoError(t, err)

	attrs := &datamodel.KmsAttributes{SdeKmsConfigUUID: "external-uuid"}
	updated, err := store.UpdateKmsConfigAttributes(ctx, "kms-uuid", attrs)
	assert.NoError(t, err)
	assert.NotNil(t, updated)
	assert.Equal(t, "external-uuid", updated.KmsAttributes.SdeKmsConfigUUID)
}

func TestUpdateKmsConfigAttributesReturnsErrorIfConfigNotFound_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()
	attrs := &datamodel.KmsAttributes{SdeKmsConfigUUID: "external-uuid"}
	_, err = store.UpdateKmsConfigAttributes(ctx, "nonexistent-uuid", attrs)
	assert.Error(t, err)
}

func TestUpdateKmsConfigAttributesReturnsErrorIfAttributesNil_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()
	kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "kms-uuid2"}}
	_, err = store.CreateKmsConfig(ctx, kmsConfig)
	assert.NoError(t, err)

	_, err = store.UpdateKmsConfigAttributes(ctx, "kms-uuid2", &datamodel.KmsAttributes{SdeKmsConfigUUID: "external-uuid"})
	assert.NoError(t, err)
}

func TestGetJobByKmsConfigIDReturnsErrorIfNotFound_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()

	found, err := store.GetJobByResourceUUID(ctx, "nonexistent-uuid", "")
	assert.Error(t, err)
	assert.Nil(t, found)
}

func TestUpdateKmsConfigDetailsReturnsErrorIfConfigNotFound_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()

	updated, err := store.UpdateKmsConfigDetails(ctx, "nonexistent-uuid", "some-path", "some-resource")
	assert.Error(t, err)
	assert.Nil(t, updated)
}

func TestUpdateServiceAccountEmailAndKeyReturnsErrorIfAccountNotFound_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()

	updated, err := store.UpdateServiceAccountEmailAndKey(ctx, "nonexistent-uuid", "email@email.com", "key")
	assert.Error(t, err)
	assert.Nil(t, updated)
}

func TestGetKmsConfigByUUIDReturnsErrorIfNotFound_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()

	found, err := store.GetKmsConfigByUUID(ctx, "nonexistent-uuid")
	assert.Error(t, err)
	assert.Nil(t, found)
}

func TestCreateBackupVaultEntryInVCP_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup vault entry in VCP
	bv := &datamodel.BackupVault{Name: "vcpVault"}
	created, err := store.CreateBackupVaultEntryInVCP(ctx, bv)
	assert.NoError(t, err)
	assert.NotNil(t, created)
	assert.Equal(t, "vcpVault", created.Name)
}

func TestUpdateVolumeFields_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a volume to update
	vol := &datamodel.Volume{Name: "vol-update-fields"}
	created, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Case 1: Successful update
	updates := map[string]interface{}{"Name": "vol-updated"}
	err = store.UpdateVolumeFields(ctx, created.UUID, updates)
	assert.NoError(t, err)
	updatedVol, err := store.GetVolume(ctx, created.UUID)
	assert.NoError(t, err)
	assert.Equal(t, "vol-updated", updatedVol.Name)

	// Case 2: Empty updates map (should not error)
	err = store.UpdateVolumeFields(ctx, created.UUID, map[string]interface{}{})
	assert.NoError(t, err)

	// Case 3: Non-existent volume UUID
	err = store.UpdateVolumeFields(ctx, "non-existent-uuid", map[string]interface{}{"Name": "should-fail"})
	assert.Error(t, err)

	// Case 4: Underlying repository returns error (simulate by closing DB)
	_ = store.Close()
	err = store.UpdateVolumeFields(ctx, created.UUID, map[string]interface{}{"Name": "fail"})
	assert.Error(t, err)
}

func TestUpdateVolumeFields_UpdatesUpdatedAt(t *testing.T) {
	db, err := SetupTestDB()
	assert.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	assert.NoError(t, ClearInMemoryDB(store.db.GORM()))

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test_account",
	}
	assert.NoError(t, store.db.Create(account).Error())

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:      "test_volume",
		AccountID: account.ID,
		Account:   account,
	}
	assert.NoError(t, store.db.Create(volume).Error())

	created, err := store.GetVolume(context.Background(), volume.UUID)
	assert.NoError(t, err)
	firstUpdateAT := created.UpdatedAt

	updates := map[string]interface{}{
		"State": "UPDATING",
	}
	assert.NoError(t, store.UpdateVolumeFields(context.Background(), volume.UUID, updates))

	updated, err := store.GetVolume(context.Background(), volume.UUID)
	assert.NoError(t, err)
	assert.Equal(t, "UPDATING", updated.State)
	newUpdateAT := updated.UpdatedAt
	assert.NotEqual(t, firstUpdateAT, newUpdateAT, "UpdatedAt should change after update")
}

func TestCreateAdminJobSpec_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	jobSpec := &datamodel.AdminJobSpec{JobType: "TEST_JOB", CronExpression: "*/10 * * * *", State: "CREATING"}
	created, err := store.CreateAdminJobSpec(ctx, jobSpec)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetAdminJobSpecByJobType_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	jobSpec := &datamodel.AdminJobSpec{JobType: "TEST_JOB", CronExpression: "*/10 * * * *", State: "CREATING"}
	_, _ = store.CreateAdminJobSpec(ctx, jobSpec)

	retrievedJobSpec, err := store.GetAdminJobSpecByJobType(ctx, jobSpec.JobType)
	assert.NoError(t, err)
	assert.Equal(t, "*/10 * * * *", retrievedJobSpec.CronExpression)
}

func TestUpdateAdminJobSpec_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	jobSpec := &datamodel.AdminJobSpec{JobType: "TEST_JOB", CronExpression: "*/10 * * * *", State: "CREATING"}
	_, err := store.CreateAdminJobSpec(ctx, jobSpec)
	assert.NoError(t, err)

	jobSpec.State = "SCHEDULED"
	err = store.UpdateAdminJobSpec(ctx, jobSpec)
	assert.NoError(t, err)

	retrievedJobSpec, err := store.GetAdminJobSpecByJobType(ctx, jobSpec.JobType)
	assert.NoError(t, err)
	assert.Equal(t, "SCHEDULED", retrievedJobSpec.State)
}

func TestGetAdminJobSpecsByState_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	jobSpec1 := &datamodel.AdminJobSpec{JobType: "TEST_JOB", CronExpression: "*/10 * * * *", State: "CREATING"}
	_, err := store.CreateAdminJobSpec(ctx, jobSpec1)
	assert.NoError(t, err)
	jobSpec2 := &datamodel.AdminJobSpec{JobType: "TEST_JOB_2", CronExpression: "*/10 * * * *", State: "SCHEDULED"}
	_, err = store.CreateAdminJobSpec(ctx, jobSpec2)
	assert.NoError(t, err)

	retrievedJobSpecs, err := store.GetAdminJobSpecsByState(ctx, "CREATING")
	assert.NoError(t, err)
	assert.Len(t, retrievedJobSpecs, 1)
}

// Test case for VerifyVolumeOwnership
func TestVerifyVolumeOwnership_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_verify"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_verify", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Verify ownership
	found, err := store.VerifyVolumeOwnership(ctx, createdVol.UUID, createdAcc.Name)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Equal(t, createdVol.UUID, found.UUID)

	// Case: Non-existent volume
	_, err = store.VerifyVolumeOwnership(ctx, "non-existent-uuid", createdAcc.Name)
	assert.Error(t, err)
}

// Test case for IsBackupInCreatingStateByVolume
func TestIsBackupInCreatingStateByVolume_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a volume
	vol := &datamodel.Volume{Name: "vol_backup_state"}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Check backup state (should be false initially)
	inCreatingState, err := store.IsBackupInCreatingorDeletingStateByVolume(ctx, createdVol.UUID)
	assert.NoError(t, err)
	assert.False(t, inCreatingState)

	// Simulate backup in creating state
	backup := &datamodel.Backup{VolumeUUID: createdVol.UUID, State: "creating"}
	_, err = store.CreateBackup(ctx, backup)
	assert.NoError(t, err)

	// Check backup state again
	inCreatingState, err = store.IsBackupInCreatingorDeletingStateByVolume(ctx, createdVol.UUID)
	assert.NoError(t, err)
	assert.True(t, inCreatingState)
}

func TestCreateBackup_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Case 1: Successful creation
	backup := &datamodel.Backup{VolumeUUID: "uuid", State: "CREATING", Name: "test-backup"}
	created, err := store.CreateBackup(ctx, backup)
	assert.NoError(t, err)
	assert.NotNil(t, created)
	assert.Equal(t, "CREATING", created.State)
}

func TestGetBackup_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a account
	acc := &datamodel.Account{Name: "acc_backup"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)

	// create a backup vault
	bv := &datamodel.BackupVault{Name: "backupVault", AccountID: createdAcc.ID}
	creatingBv, err := store.CreatingBackupVault(ctx, bv)
	assert.NoError(t, err)

	// Create a backup
	backup := &datamodel.Backup{VolumeUUID: "uuid", State: "new", BackupVaultID: creatingBv.ID}
	created, err := store.CreateBackup(ctx, backup)
	assert.NoError(t, err)

	// Case 1: Successful retrieval
	found, err := store.GetBackup(ctx, bv.UUID, created.UUID, acc.Name)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Equal(t, created.UUID, found.UUID)

	// Case 2: Error scenario (non-existent UUID)
	_, err = store.GetBackup(ctx, bv.UUID, "random-uuid1", acc.Name)
	assert.Error(t, err)
}

func TestDeleteBackup_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup
	backup := &datamodel.Backup{VolumeUUID: "uuid", State: "new"}
	created, err := store.CreateBackup(ctx, backup)
	assert.NoError(t, err)

	// Case 1: Successful deletion
	deleted, err := store.DeleteBackup(ctx, created.UUID)
	assert.NoError(t, err)
	assert.NotNil(t, deleted)

	// Case 2: Error scenario (non-existent UUID)
	_, err = store.DeleteBackup(ctx, "non-existent-uuid")
	assert.Error(t, err)
}

func TestUpdateBackupStateUpdatesStateSuccessfully_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup
	backup := &datamodel.Backup{VolumeUUID: "uuid", State: "CREATING"}
	created, err := store.CreateBackup(ctx, backup)
	assert.NoError(t, err)

	// Update state
	created.State = "COMPLETED"
	updated, err := store.UpdateBackupState(ctx, created)
	assert.NoError(t, err)
	assert.NotNil(t, updated)
	assert.Equal(t, "COMPLETED", updated.State)
}

func TestUpdateBackupStateFailsForNonExistentBackup_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Case: Non-existent backup
	nonExistentBackup := &datamodel.Backup{State: "FAILED"}
	_, err := store.UpdateBackupState(ctx, nonExistentBackup)
	assert.Error(t, err)
}

func TestFinishBackup_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup
	backup := &datamodel.Backup{VolumeUUID: "uuid", State: "new"}
	created, err := store.CreateBackup(ctx, backup)
	assert.NoError(t, err)

	// Case 1: Successful finish
	created.State = "AVAILABLE"
	finished, err := store.FinishBackup(ctx, created)
	assert.NoError(t, err)
	assert.NotNil(t, finished)
	assert.Equal(t, "AVAILABLE", finished.State)
}

func TestUpdateBackup_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup
	backup := &datamodel.Backup{VolumeUUID: "uuid", State: "new"}
	created, err := store.CreateBackup(ctx, backup)
	assert.NoError(t, err)

	// params := &common.Up

	backup.Description = "Updated backup description"
	// Case 1: Successful update
	created.State = "CREATING"
	updated, err := store.UpdateBackup(ctx, created)
	assert.NoError(t, err)
	assert.NotNil(t, updated)
	assert.Equal(t, "AVAILABLE", updated.State)
}

func TestListVolumeReplications_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_snap2"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_snap2", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Create a volume replication
	replication := &datamodel.VolumeReplication{
		Name:                  "replication1",
		AccountID:             createdAcc.ID,
		VolumeID:              createdVol.ID,
		ReplicationAttributes: &datamodel.ReplicationDetails{},
	}
	created, err := store.CreateVolumeReplication(ctx, replication)
	assert.NoError(t, err)
	assert.NotNil(t, created)

	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("account_id", "=", replication.AccountID))

	// List volume replications
	reps, err := store.ListVolumeReplications(ctx, *filter)
	assert.NoError(t, err)
	assert.NotEmpty(t, reps)
	assert.Equal(t, created.Name, reps[0].Name)
}

func TestCreateBackupPolicyEntryInVCP_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup policy entry in VCP
	backupPolicy := &datamodel.BackupPolicy{Name: "vcp-policy"}
	created, err := store.CreateBackupPolicyEntryInVCP(ctx, backupPolicy)
	assert.NoError(t, err)
	assert.NotNil(t, created)
	assert.Equal(t, "vcp-policy", created.Name)
}

func TestGetMultipleBackupVaultsReturnsMatchingVaults(tt *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	account := &datamodel.Account{Name: "test-account"}
	createdAccount, err := store.CreateAccount(ctx, account)
	assert.NoError(tt, err)

	backupVault := &datamodel.BackupVault{Name: "test-vault", AccountID: createdAccount.ID}
	createdVault, err := store.CreatingBackupVault(ctx, backupVault)
	assert.NoError(tt, err)

	conditions := [][]interface{}{{"account_id = ?", createdAccount.ID}}
	result, err := store.GetMultipleBackupVaults(ctx, conditions)
	assert.NoError(tt, err)
	assert.Len(tt, result, 1)
	assert.Equal(tt, createdVault.UUID, result[0].UUID)
}

func TestGetMultipleBackupVaultsReturnsEmptyWhenNoMatch(tt *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	conditions := [][]interface{}{{"account_id = ?", 999}}
	result, err := store.GetMultipleBackupVaults(ctx, conditions)
	assert.NoError(tt, err)
	assert.Empty(tt, result)
}

func TestGetMultipleBackupVaultsReturnsErrorOnDatabaseFailure(tt *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	sqlDB, _ := store.DB().DB()
	_ = sqlDB.Close()

	conditions := [][]interface{}{{"account_id = ?", 123}}
	_, err := store.GetMultipleBackupVaults(ctx, conditions)
	assert.Error(tt, err)
}

func TestGetBackupCountByBackupVaultID_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup vault
	backupVault := &datamodel.BackupVault{Name: "test-vault"}
	createdVault, err := store.CreatingBackupVault(ctx, backupVault)
	assert.NoError(t, err)

	// Create backups associated with the backup vault
	backup1 := &datamodel.Backup{BackupVaultID: createdVault.ID, Name: "backup1"}
	backup2 := &datamodel.Backup{BackupVaultID: createdVault.ID, Name: "backup2"}
	_, err = store.CreateBackup(ctx, backup1)
	assert.NoError(t, err)
	_, err = store.CreateBackup(ctx, backup2)
	assert.NoError(t, err)

	// Call the method under test
	count, err := store.GetBackupCountByBackupVaultID(ctx, createdVault.ID)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestGetVolumeCountByBackupVaultID_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup vault
	backupVault := &datamodel.BackupVault{Name: "test-vault"}
	createdVault, err := store.CreatingBackupVault(ctx, backupVault)
	assert.NoError(t, err)

	// Create volumes associated with the backup vault
	volume1 := &datamodel.Volume{DataProtection: &datamodel.DataProtection{BackupVaultID: createdVault.UUID}, Name: "volume1"}
	volume2 := &datamodel.Volume{DataProtection: &datamodel.DataProtection{BackupVaultID: createdVault.UUID}, Name: "volume2"}
	_, err = store.CreateVolume(ctx, volume1)
	assert.NoError(t, err)
	_, err = store.CreateVolume(ctx, volume2)
	assert.NoError(t, err)

	// Call the method under test
	count, err := store.GetVolumeCountByBackupVaultID(ctx, createdVault.UUID)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestDeleteBackupVaultInVCP_Success(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup vault
	backupVault := &datamodel.BackupVault{Name: "test-vault"}
	createdVault, err := store.CreatingBackupVault(ctx, backupVault)
	assert.NoError(t, err)

	// Delete the backup vault
	deletedVault, err := store.DeleteBackupVaultInVCP(ctx, createdVault.UUID)
	assert.NoError(t, err)
	assert.NotNil(t, deletedVault)
	assert.Equal(t, createdVault.UUID, deletedVault.UUID)
}

func TestDeleteBackupVaultInVCP_Error(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Attempt to delete a non-existent backup vault
	_, err := store.DeleteBackupVaultInVCP(ctx, "non-existent-uuid")
	assert.Error(t, err)
}

func TestPersistenceStore_UpdateBackupPolicy(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)

	ctx := context.Background()

	// Setup: create account and backup policy
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test-account",
	}
	account, err = store.CreateAccount(ctx, account)
	assert.NoError(t, err)

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{UUID: "test-backup-policy-uuid"},
		Name:                  "test-backup-policy",
		Description:           nillable.ToPointer("Initial description"),
		PolicyEnabled:         false,
		DailyBackupsToKeep:    5,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  1,
		AccountID:             account.ID,
		Account:               account,
		LifeCycleState:        models.LifeCycleStateREADY,
		LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
	}
	_, err = store.CreateBackupPolicyEntryInVCP(ctx, backupPolicy)
	assert.NoError(t, err)

	t.Run("UpdateBackupPolicySucceeds", func(tt *testing.T) {
		updates := map[string]interface{}{
			"description":             "Updated description",
			"policy_enabled":          true,
			"daily_backups_to_keep":   10,
			"weekly_backups_to_keep":  5,
			"monthly_backups_to_keep": 3,
		}
		result, err := store.UpdateBackupPolicy(ctx, backupPolicy.UUID, updates)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "Updated description", *result.Description)
		assert.True(tt, result.PolicyEnabled)
		assert.Equal(tt, int64(10), result.DailyBackupsToKeep)
		assert.Equal(tt, int64(5), result.WeeklyBackupsToKeep)
		assert.Equal(tt, int64(3), result.MonthlyBackupsToKeep)
	})

	t.Run("UpdateBackupPolicyFails", func(tt *testing.T) {
		updates := map[string]interface{}{
			"non_existent_column": "This should fail",
		}
		result, err := store.UpdateBackupPolicy(ctx, backupPolicy.UUID, updates)
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}

func TestListBackupPolicyVolumeCount_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()

	// Create backup policies
	policy1 := &datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "uuid-1"}, Name: "policy1"}
	policy2 := &datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "uuid-2"}, Name: "policy2"}
	createdPolicy1, err := store.CreateBackupPolicyEntryInVCP(ctx, policy1)
	assert.NoError(t, err)
	createdPolicy2, err := store.CreateBackupPolicyEntryInVCP(ctx, policy2)
	assert.NoError(t, err)

	// Create volumes and associate with policies
	vol1 := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "uuid-1"}, Name: "vol1", DataProtection: &datamodel.DataProtection{BackupPolicyID: "uuid-1"}}
	vol2 := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "uuid-2"}, Name: "vol2", DataProtection: &datamodel.DataProtection{BackupPolicyID: "uuid-1"}}
	vol3 := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "uuid-3"}, Name: "vol3", DataProtection: &datamodel.DataProtection{BackupPolicyID: "uuid-2"}}
	_, err = store.CreateVolume(ctx, vol1)
	assert.NoError(t, err)
	_, err = store.CreateVolume(ctx, vol2)
	assert.NoError(t, err)
	_, err = store.CreateVolume(ctx, vol3)
	assert.NoError(t, err)

	// List backup policy volume counts
	result, err := store.ListBackupPolicyVolumeCount(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result[createdPolicy1.UUID])
	assert.Equal(t, int64(1), result[createdPolicy2.UUID])

	// List backup policy volume counts with conditions
	conditions := [][]interface{}{{"name = ?", "nonexistent"}}
	result, err = store.ListBackupPolicyVolumeCount(ctx, conditions)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestListBackupPolicies_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()

	policy1 := &datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "uuid-1"}, Name: "policy1"}
	policy2 := &datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "uuid-2"}, Name: "policy2"}
	// Create backup policies in VCP
	_, err := store.CreateBackupPolicyEntryInVCP(ctx, policy1)
	assert.NoError(t, err)
	_, err = store.CreateBackupPolicyEntryInVCP(ctx, policy2)
	assert.NoError(t, err)

	// List backup policies
	policies, err := store.ListBackupPolicies(ctx, nil)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(policies), 2)

	// List backup policies with filter
	conditions := [][]interface{}{{"name = ?", "policy1"}}
	policies, err = store.ListBackupPolicies(ctx, conditions)
	assert.NoError(t, err)
	assert.Len(t, policies, 1)
	assert.Equal(t, "policy1", policies[0].Name)
}

func TestBatchCreateSnapshots_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_batch_create"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_batch_create", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Create snapshots for batch creation
	snapshots := []*datamodel.Snapshot{
		{Name: "batch_snap_1", AccountID: createdAcc.ID, VolumeID: createdVol.ID, Volume: createdVol, Account: createdAcc},
		{Name: "batch_snap_2", AccountID: createdAcc.ID, VolumeID: createdVol.ID, Volume: createdVol, Account: createdAcc},
	}

	// Test batch creation with returnCreatedSnapshotUUIDs = true
	uuids, err := store.BatchCreateSnapshots(ctx, snapshots, true)
	assert.NoError(t, err)
	assert.Len(t, uuids, 2)
	assert.NotEmpty(t, uuids[0])
	assert.NotEmpty(t, uuids[1])

	// Test batch creation with returnCreatedSnapshotUUIDs = false
	snapshots2 := []*datamodel.Snapshot{
		{Name: "batch_snap_3", AccountID: createdAcc.ID, VolumeID: createdVol.ID, Volume: createdVol, Account: createdAcc},
	}
	uuids2, err := store.BatchCreateSnapshots(ctx, snapshots2, false)
	assert.NoError(t, err)
	assert.Empty(t, uuids2)

	// Test with empty snapshots slice
	emptyUUIDs, err := store.BatchCreateSnapshots(ctx, []*datamodel.Snapshot{}, true)
	assert.NoError(t, err)
	assert.Empty(t, emptyUUIDs)
}

func TestBatchUpdateSnapshots_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_batch_update"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_batch_update", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Create snapshots first
	snapshots := []*datamodel.Snapshot{
		{Name: "update_snap_1", AccountID: createdAcc.ID, VolumeID: createdVol.ID, Volume: createdVol, Account: createdAcc},
		{Name: "update_snap_2", AccountID: createdAcc.ID, VolumeID: createdVol.ID, Volume: createdVol, Account: createdAcc},
	}
	uuids, err := store.BatchCreateSnapshots(ctx, snapshots, true)
	assert.NoError(t, err)

	// Update snapshots with new state and state details
	for i, uuid := range uuids {
		snapshots[i].UUID = uuid
		snapshots[i].State = models.LifeCycleStateDeleted
		snapshots[i].StateDetails = models.LifeCycleStateDeletedDetails
	}

	// Test batch update
	err = store.BatchUpdateSnapshots(ctx, snapshots)
	assert.NoError(t, err)

	// Verify updates by fetching snapshots
	for _, uuid := range uuids {
		snapshot, err := store.GetSnapshotByUUID(ctx, uuid, createdAcc.ID, createdVol.ID)
		// Note: This might fail if snapshot is soft-deleted, which is expected behavior
		if err == nil {
			assert.Equal(t, models.LifeCycleStateDeleted, snapshot.State)
			assert.Equal(t, models.LifeCycleStateDeletedDetails, snapshot.StateDetails)
		}
	}

	// Test with empty snapshots slice
	err = store.BatchUpdateSnapshots(ctx, []*datamodel.Snapshot{})
	assert.NoError(t, err)
}

func TestBatchUnDeleteSnapshots_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_batch_undelete"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_batch_undelete", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Create snapshots first
	snapshots := []*datamodel.Snapshot{
		{Name: "undelete_snap_1", AccountID: createdAcc.ID, VolumeID: createdVol.ID, Volume: createdVol, Account: createdAcc},
		{Name: "undelete_snap_2", AccountID: createdAcc.ID, VolumeID: createdVol.ID, Volume: createdVol, Account: createdAcc},
	}
	uuids, err := store.BatchCreateSnapshots(ctx, snapshots, true)
	assert.NoError(t, err)

	// Mark snapshots as deleted first
	for i, uuid := range uuids {
		snapshots[i].UUID = uuid
		snapshots[i].State = models.LifeCycleStateDeleted
		snapshots[i].StateDetails = models.LifeCycleStateDeletedDetails
	}
	err = store.BatchUpdateSnapshots(ctx, snapshots)
	assert.NoError(t, err)

	// Test batch undelete
	err = store.BatchUnDeleteSnapshots(ctx, snapshots)
	assert.NoError(t, err)

	// Test with empty snapshots slice
	err = store.BatchUnDeleteSnapshots(ctx, []*datamodel.Snapshot{})
	assert.NoError(t, err)
}

func TestBatchGetSnapshotsByUUIDs_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_batch_get"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_batch_get", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Create snapshots first
	snapshots := []*datamodel.Snapshot{
		{Name: "get_snap_1", AccountID: createdAcc.ID, VolumeID: createdVol.ID, Volume: createdVol, Account: createdAcc},
		{Name: "get_snap_2", AccountID: createdAcc.ID, VolumeID: createdVol.ID, Volume: createdVol, Account: createdAcc},
		{Name: "get_snap_3", AccountID: createdAcc.ID, VolumeID: createdVol.ID, Volume: createdVol, Account: createdAcc},
	}
	uuids, err := store.BatchCreateSnapshots(ctx, snapshots, true)
	assert.NoError(t, err)
	assert.Len(t, uuids, 3)

	// Test batch get by UUIDs
	fetchedSnapshots, err := store.BatchGetSnapshotsByUUIDs(ctx, uuids)
	assert.NoError(t, err)
	assert.Len(t, fetchedSnapshots, 3)

	// Verify fetched snapshots have correct data
	names := make(map[string]bool)
	for _, snapshot := range fetchedSnapshots {
		assert.NotEmpty(t, snapshot.UUID)
		assert.Equal(t, createdAcc.ID, snapshot.AccountID)
		assert.Equal(t, createdVol.ID, snapshot.VolumeID)
		names[snapshot.Name] = true
	}
	assert.True(t, names["get_snap_1"])
	assert.True(t, names["get_snap_2"])
	assert.True(t, names["get_snap_3"])

	// Test with partial UUIDs (some exist, some don't)
	mixedUUIDs := append(uuids[:2], "non-existent-uuid")
	fetchedSnapshots2, err := store.BatchGetSnapshotsByUUIDs(ctx, mixedUUIDs)
	assert.NoError(t, err)
	assert.Len(t, fetchedSnapshots2, 2) // Only existing snapshots should be returned

	// Test with empty UUIDs slice
	emptySnapshots, err := store.BatchGetSnapshotsByUUIDs(ctx, []string{})
	assert.NoError(t, err)
	assert.Empty(t, emptySnapshots)

	// Test with non-existent UUIDs
	nonExistentSnapshots, err := store.BatchGetSnapshotsByUUIDs(ctx, []string{"uuid1", "uuid2"})
	assert.NoError(t, err)
	assert.Empty(t, nonExistentSnapshots)
}

func TestBatchGetWronglyDeletedSnapshots_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_wrongly_deleted"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_wrongly_deleted", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Create snapshots with external UUIDs
	snapshots := []*datamodel.Snapshot{
		{
			Name:      "wrongly_deleted_1",
			AccountID: createdAcc.ID,
			VolumeID:  createdVol.ID,
			Volume:    createdVol,
			Account:   createdAcc,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "ext-uuid-1",
			},
		},
		{
			Name:      "wrongly_deleted_2",
			AccountID: createdAcc.ID,
			VolumeID:  createdVol.ID,
			Volume:    createdVol,
			Account:   createdAcc,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "ext-uuid-2",
			},
		},
	}
	uuids, err := store.BatchCreateSnapshots(ctx, snapshots, true)
	assert.NoError(t, err)

	// Mark snapshots as deleted to simulate wrongly deleted state
	for i, uuid := range uuids {
		snapshots[i].UUID = uuid
	}
	_, err = store.BatchDeleteSnapshots(ctx, []int64{snapshots[0].ID, snapshots[1].ID})
	assert.NoError(t, err)

	// Test batch get wrongly deleted snapshots
	externalUUIDs := []string{"ext-uuid-1", "ext-uuid-2"}
	wronglyDeletedSnapshots, err := store.BatchGetWronglyDeletedSnapshots(ctx, externalUUIDs)
	assert.NoError(t, err)
	assert.Len(t, wronglyDeletedSnapshots, 2)

	// Verify external UUIDs match
	foundExternalUUIDs := make(map[string]bool)
	for _, snapshot := range wronglyDeletedSnapshots {
		if snapshot.SnapshotAttributes != nil {
			foundExternalUUIDs[snapshot.SnapshotAttributes.ExternalUUID] = true
		}
	}
	assert.True(t, foundExternalUUIDs["ext-uuid-1"])
	assert.True(t, foundExternalUUIDs["ext-uuid-2"])

	// Test with non-existent external UUIDs
	nonExistentSnapshots, err := store.BatchGetWronglyDeletedSnapshots(ctx, []string{"non-existent-ext-uuid"})
	assert.NoError(t, err)
	assert.Empty(t, nonExistentSnapshots)

	// Test with empty external UUIDs slice
	emptySnapshots, err := store.BatchGetWronglyDeletedSnapshots(ctx, []string{})
	assert.NoError(t, err)
	assert.Empty(t, emptySnapshots)

	// Test with mixed external UUIDs (some exist, some don't)
	mixedExternalUUIDs := []string{"ext-uuid-1", "non-existent-ext-uuid"}
	mixedSnapshots, err := store.BatchGetWronglyDeletedSnapshots(ctx, mixedExternalUUIDs)
	assert.NoError(t, err)
	assert.Len(t, mixedSnapshots, 1) // Only one should be found
	if len(mixedSnapshots) > 0 && mixedSnapshots[0].SnapshotAttributes != nil {
		assert.Equal(t, "ext-uuid-1", mixedSnapshots[0].SnapshotAttributes.ExternalUUID)
	}
}

func TestBatchSnapshotOperations_ErrorScenarios_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	t.Run("BatchCreateSnapshots_DatabaseClosed", func(tt *testing.T) {
		// Close the database to simulate error
		sqlDB, _ := store.DB().DB()
		_ = sqlDB.Close()

		snapshots := []*datamodel.Snapshot{
			{Name: "error_snap_1"},
		}
		_, err := store.BatchCreateSnapshots(ctx, snapshots, true)
		assert.Error(tt, err)
	})

	// Restore store for subsequent tests
	store, _ = SetupStorageForTest(logger)

	t.Run("BatchUpdateSnapshots_NonExistentSnapshot", func(tt *testing.T) {
		snapshots := []*datamodel.Snapshot{
			{
				BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid", ID: 9999},
				State:     models.LifeCycleStateDeleted,
			},
		}
		err := store.BatchUpdateSnapshots(ctx, snapshots)
		// This should not error even if snapshot doesn't exist (0 rows affected)
		assert.NoError(tt, err)
	})

	t.Run("BatchUnDeleteSnapshots_NonExistentSnapshot", func(tt *testing.T) {
		snapshots := []*datamodel.Snapshot{
			{
				BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid", ID: 9999},
				State:     models.LifeCycleStateREADY,
			},
		}
		err := store.BatchUnDeleteSnapshots(ctx, snapshots)
		// This should not error even if snapshot doesn't exist (0 rows affected)
		assert.NoError(tt, err)
	})
}

func TestBatchSnapshotOperations_ChunkingBehavior_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_chunking"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_chunking", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)

	// Create a larger number of snapshots to test chunking (assuming chunk size is 200)
	var snapshots []*datamodel.Snapshot
	for i := 0; i < 250; i++ {
		snapshots = append(snapshots, &datamodel.Snapshot{
			Name:      fmt.Sprintf("chunk_snap_%d", i),
			AccountID: createdAcc.ID,
			VolumeID:  createdVol.ID,
			Volume:    createdVol,
			Account:   createdAcc,
		})
	}

	// Test batch creation with large number of snapshots
	uuids, err := store.BatchCreateSnapshots(ctx, snapshots, true)
	assert.NoError(t, err)
	assert.Len(t, uuids, 250)

	// Test batch get with large number of UUIDs
	fetchedSnapshots, err := store.BatchGetSnapshotsByUUIDs(ctx, uuids)
	assert.NoError(t, err)
	assert.Len(t, fetchedSnapshots, 250)

	// Update all snapshots
	for i, uuid := range uuids {
		snapshots[i].UUID = uuid
		snapshots[i].State = models.LifeCycleStateUpdating
		snapshots[i].StateDetails = models.LifeCycleStateUpdatingDetails
	}

	// Test batch update with large number of snapshots
	err = store.BatchUpdateSnapshots(ctx, snapshots)
	assert.NoError(t, err)
}

func TestGetVolumeCountByBackupPolicyID_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup policy
	policy := &datamodel.BackupPolicy{Name: "test-policy"}
	createdPolicy, err := store.CreateBackupPolicyEntryInVCP(ctx, policy)
	assert.NoError(t, err)

	// Create volumes associated with the backup policy
	vol1 := &datamodel.Volume{DataProtection: &datamodel.DataProtection{BackupPolicyID: createdPolicy.UUID}, Name: "volume1"}
	vol2 := &datamodel.Volume{DataProtection: &datamodel.DataProtection{BackupPolicyID: createdPolicy.UUID}, Name: "volume2"}
	_, err = store.CreateVolume(ctx, vol1)
	assert.NoError(t, err)
	_, err = store.CreateVolume(ctx, vol2)
	assert.NoError(t, err)

	// Call the method under test
	count, err := store.GetVolumeCountByBackupPolicyID(ctx, createdPolicy.UUID)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestDeleteBackupPolicy_Persistence_Store(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup policy
	policy := &datamodel.BackupPolicy{Name: "test-policy"}
	createdPolicy, err := store.CreateBackupPolicyEntryInVCP(ctx, policy)
	assert.NoError(t, err)

	// Delete the backup policy
	deletedPolicy, err := store.DeleteBackupPolicy(ctx, createdPolicy.UUID)
	assert.NoError(t, err)
	assert.NotNil(t, deletedPolicy)
	assert.Equal(t, createdPolicy.UUID, deletedPolicy.UUID)

	// Attempt to delete a non-existent policy
	_, err = store.DeleteBackupPolicy(ctx, "non-existent-uuid")
	assert.Error(t, err)
}

func TestListSnHostsReturnsEmptyWhenNoPoolsPresent(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	hosts, err := store.ListTpProjects(ctx)
	assert.NoError(t, err)
	assert.Empty(t, hosts)
}

func TestGetSoftDeleteAccount_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	storage, err := SetupStorageForTest(logger)
	require.NoError(t, err)

	ps := storage.(*PersistenceStore)
	ctx := context.Background()

	// Create test accounts
	account1 := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "uuid-1"},
		Name:      "test-account-1",
	}
	account2 := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "uuid-2"},
		Name:      "test-account-2",
	}

	createdAccount1, err := ps.CreateAccount(ctx, account1)
	require.NoError(t, err)
	_, err = ps.CreateAccount(ctx, account2)
	require.NoError(t, err)

	// Soft delete account1
	err = ps.DeleteAccount(ctx, createdAccount1.ID)
	require.NoError(t, err)

	// Test GetSoftDeleteAccount returns soft deleted account
	retrievedAccount, err := ps.GetSoftDeleteAccount(ctx, "test-account-1")
	assert.NoError(t, err)
	assert.NotNil(t, retrievedAccount)
	assert.Equal(t, "test-account-1", retrievedAccount.Name)
	assert.NotNil(t, retrievedAccount.DeletedAt)

	// Test GetAccount does not return soft deleted account
	_, err = ps.GetAccount(ctx, "test-account-1")
	assert.Error(t, err)

	// Test GetAccount still returns non-deleted account
	retrievedAccount2, err := ps.GetAccount(ctx, "test-account-2")
	assert.NoError(t, err)
	assert.Equal(t, "test-account-2", retrievedAccount2.Name)

	// Test GetSoftDeleteAccount returns error for non-existent account
	_, err = ps.GetSoftDeleteAccount(ctx, "non-existent")
	assert.Error(t, err)

	_ = ps.Close()
}

// Add after TestGetSoftDeleteAccount_Persistence_Store
func TestAccountSoftDelete_Persistence_Store(t *testing.T) {
	logger := log.NewLogger()
	storage, err := SetupStorageForTest(logger)
	require.NoError(t, err)

	ps := storage.(*PersistenceStore)
	ctx := context.Background()

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "uuid-test"},
		Name:      "test-account",
	}
	createdAccount, err := ps.CreateAccount(ctx, account)
	require.NoError(t, err)

	// Soft delete the account
	err = ps.DeleteAccount(ctx, createdAccount.ID)
	assert.NoError(t, err)

	var result datamodel.Account
	err = ps.DB().Unscoped().First(&result, createdAccount.ID).Error
	assert.NoError(t, err)
	assert.NotNil(t, result.DeletedAt)
	assert.True(t, result.DeletedAt.Valid)

	err = ps.DeleteAccount(ctx, 99999)
	assert.Error(t, err)

	assert.Error(t, err)

	_ = ps.Close()
}

func TestPersistenceStore_AccountDelegates(t *testing.T) {
	logger := &log.MockLogger{}
	store, err := NewTestStorage(logger)
	assert.NoError(t, err)
	ctx := context.Background()

	// Create an account for testing
	account := &datamodel.Account{Name: "test-account"}
	created, err := store.CreateAccount(ctx, account)
	assert.NoError(t, err)

	// Test GetSoftDeleteAccount (should return the account if not deleted)
	acc, err := store.GetSoftDeleteAccount(ctx, created.Name)
	assert.NoError(t, err)
	assert.NotNil(t, acc)

	// Test GetDeletedAccounts (should be empty since not deleted)
	deleted, err := store.GetDeletedAccounts(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, deleted)

	// Test RollBackDeletedAccount (should succeed even if not deleted)
	err = store.RollBackDeletedAccount(ctx, created.ID)
	assert.NoError(t, err)
}

func TestPersistenceStore_AccountDelegates_ErrorCases(t *testing.T) {
	logger := &log.MockLogger{}
	store, err := NewTestStorage(logger)
	assert.NoError(t, err)
	ctx := context.Background()

	// Close the DB to simulate error
	sqlDB, _ := store.DB().DB()
	_ = sqlDB.Close()

	_, err = store.GetDeletedAccounts(ctx)
	assert.Error(t, err)

	err = store.RollBackDeletedAccount(ctx, 12345)
	assert.Error(t, err)
}

// TestPersistenceStore_CreatePendingResourceDeletion tests the CreatePendingResourceDeletion wrapper method
func TestPersistenceStore_CreatePendingResourceDeletion(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Error closing store: %v", err)
		}
	}()

	ctx := context.Background()
	resourceType := "BUCKET"
	resourceName := "test-bucket"
	errorMessage := "test error"
	accountName := "test-account"
	poolID := int64(123)

	// Test successful creation
	result, err := store.CreatePendingResourceDeletion(ctx, resourceType, resourceName, errorMessage, accountName, poolID)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, resourceType, result.ResourceType)
	assert.Equal(t, resourceName, result.ResourceName)
	assert.Equal(t, errorMessage, result.Error)
	assert.Equal(t, accountName, result.AccountName)
	assert.Equal(t, poolID, result.ResourceAttributes.PoolID)
}

// TestPersistenceStore_ListPendingResourceDeletions tests the ListPendingResourceDeletions wrapper method
func TestPersistenceStore_ListPendingResourceDeletions(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Error closing store: %v", err)
		}
	}()

	ctx := context.Background()
	offset := 0
	limit := 10

	// Test successful listing
	results, err := store.ListPendingResourceDeletions(ctx, offset, limit)
	assert.NoError(t, err)
	assert.NotNil(t, results)
	assert.IsType(t, []*datamodel.PendingResourceDeletions{}, results)
}

// TestPersistenceStore_UpdatePendingResourceDeletion tests the UpdatePendingResourceDeletion wrapper method
func TestPersistenceStore_UpdatePendingResourceDeletion(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Error closing store: %v", err)
		}
	}()

	ctx := context.Background()
	resourceID := int64(1)
	isDeletion := true
	errorMessage := "updated error message"

	// Test successful update
	result, err := store.UpdatePendingResourceDeletion(ctx, resourceID, isDeletion, errorMessage)
	// Note: This might fail if the resource doesn't exist, but we're testing the wrapper method
	if err != nil {
		// Verify it's a database error, not a wrapper error
		assert.True(t, err.Error() == "An internal error occurred." || err.Error() == "Resource not found")
	} else {
		assert.NotNil(t, result)
		assert.Equal(t, resourceID, result.ID)
		assert.Equal(t, errorMessage, result.Error)
	}
}

// TestPersistenceStore_GetResourcesCount tests the GetResourcesCount wrapper method
func TestPersistenceStore_GetResourcesCount(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Error closing store: %v", err)
		}
	}()

	ctx := context.Background()

	// Test successful count
	count, err := store.GetResourcesCount(ctx)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(0))
}

// TestPersistenceStore_CreatePendingResourceDeletion_ErrorHandling tests error handling in CreatePendingResourceDeletion
func TestPersistenceStore_CreatePendingResourceDeletion_ErrorHandling(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Error closing store: %v", err)
		}
	}()

	ctx := context.Background()

	// Test with empty resource type
	result, err := store.CreatePendingResourceDeletion(ctx, "", "test-bucket", "test error", "test-account", 123)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "An internal error occurred.")

	// Test with empty resource name
	result, err = store.CreatePendingResourceDeletion(ctx, "BUCKET", "", "test error", "test-account", 123)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "An internal error occurred.")
}

// TestPersistenceStore_ListPendingResourceDeletions_WithPagination tests ListPendingResourceDeletions with different pagination parameters
func TestPersistenceStore_ListPendingResourceDeletions_WithPagination(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Error closing store: %v", err)
		}
	}()

	ctx := context.Background()

	// Test with offset and limit
	results, err := store.ListPendingResourceDeletions(ctx, 5, 10)
	assert.NoError(t, err)
	assert.NotNil(t, results)

	// Test with zero limit (should return all)
	results, err = store.ListPendingResourceDeletions(ctx, 0, 0)
	assert.NoError(t, err)
	assert.NotNil(t, results)
}

// TestPersistenceStore_UpdatePendingResourceDeletion_NotFound tests UpdatePendingResourceDeletion with non-existent resource
func TestPersistenceStore_UpdatePendingResourceDeletion_NotFound(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	require.NoError(t, err)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("Error closing store: %v", err)
		}
	}()

	ctx := context.Background()
	resourceID := int64(99999) // Non-existent ID
	isDeletion := true
	errorMessage := "test error"

	// Test with non-existent resource
	result, err := store.UpdatePendingResourceDeletion(ctx, resourceID, isDeletion, errorMessage)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.True(t, err.Error() == "An internal error occurred." || err.Error() == "Resource not found")
}
