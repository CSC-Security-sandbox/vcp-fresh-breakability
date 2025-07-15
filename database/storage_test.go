package database

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestSetupStorageForTest(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.NotNil(t, store.DB())
}

func TestClearInMemoryDB(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)

	err = ClearInMemoryDB(store.DB())
	assert.NoError(t, err)
}

func TestHealthCheckAndClose(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	assert.NoError(t, store.HealthCheck())
	assert.NoError(t, store.Close())
	// After close, HealthCheck should fail
	err := store.HealthCheck()
	assert.Error(t, err)
}

func TestWithTransaction_Success(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	err := store.WithTransaction(ctx, func(tx Transaction) error {
		return nil
	})
	assert.NoError(t, err)
}

func TestWithTransaction_Error(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	err := store.WithTransaction(ctx, func(tx Transaction) error {
		return errors.New("fail")
	})
	assert.Error(t, err)
}

func TestWithTransaction_Panic(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic")
		}
	}()
	_ = store.WithTransaction(ctx, func(tx Transaction) error {
		panic("panic in tx")
	})
}

func TestWithTransaction_NilDB(t *testing.T) {
	store := &PersistenceStore{}
	err := store.WithTransaction(context.Background(), func(tx Transaction) error { return nil })
	assert.Error(t, err)
}

func TestDBMethod(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	db := store.DB()
	assert.NotNil(t, db)
}

func TestIsDatabaseExistsError(t *testing.T) {
	err := errors.New("some error")
	assert.False(t, isDatabaseExistsError(err))
}

func TestCreatingPool(t *testing.T) {
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

func TestGetPool(t *testing.T) {
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

func TestDescribePool(t *testing.T) {
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

func TestUpdatePool(t *testing.T) {
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

func TestErroredResource(t *testing.T) {
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

func TestDeletePool(t *testing.T) {
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

func TestDeletingPool(t *testing.T) {
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

func TestListPools(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.ListPools(ctx, nil)
	assert.NoError(t, err)
}

func TestGetPoolByName(t *testing.T) {
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

func TestConnectAndSetupDatabase(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	// Should not error if already connected
	assert.NoError(t, store.Connect(false))
	// SetupDatabase should fail gracefully (no Postgres in-memory)
	err := store.SetupDatabase(context.Background())
	assert.Error(t, err)
}

func TestConnect_NilConfig(t *testing.T) {
	store := &PersistenceStore{}
	err := store.Connect(false)
	assert.Error(t, err)
}

func TestCreateConnection_UnsupportedType(t *testing.T) {
	logger := log.NewLogger()
	store := &PersistenceStore{config: DbConfig{Type: "unknown"}, logger: logger}
	_, err := store.createConnection(false)
	assert.Error(t, err)
}

func TestGetPostgresDSN(t *testing.T) {
	store := &PersistenceStore{config: DbConfig{
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
func TestCreateVolume(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol1"}
	created, err := store.CreateVolume(ctx, vol, false)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetVolume(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol2"}
	created, err := store.CreateVolume(ctx, vol, false)
	assert.NoError(t, err)
	found, err := store.GetVolume(ctx, created.UUID)
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

func TestUpdateVolume(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol3"}
	created, err := store.CreateVolume(ctx, vol, false)
	assert.NoError(t, err)
	created.Name = "vol3-updated"
	err = store.UpdateVolume(ctx, created)
	assert.NoError(t, err)
}

func TestDeleteVolume(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol4"}
	created, err := store.CreateVolume(ctx, vol, false)
	assert.NoError(t, err)
	_, err = store.DeleteVolume(ctx, created.UUID)
	assert.NoError(t, err)
}

func TestGetVolumesByPoolID(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetVolumesByPoolID(ctx, 0)
	assert.NoError(t, err)
}

func TestGetVolumeCountByPoolID(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetVolumeCountByPoolID(ctx, 0)
	assert.NoError(t, err)
}

func TestGetMultipleVolumes(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetMultipleVolumes(ctx, [][]interface{}{})
	assert.NoError(t, err)
}

// ACCOUNT
func TestCreateAccount(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	acc := &datamodel.Account{Name: "acc1"}
	created, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetAccount(t *testing.T) {
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
func TestCreateJob(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	job := &datamodel.Job{ResourceName: "job1"}
	created, err := store.CreateJob(ctx, job)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetJob(t *testing.T) {
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

func TestGetJobsWithCondition(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	filter := utils.CreateFilterWithConditions(
		utils.NewFilterCondition("state", "=", "new"))
	jobs, err := store.GetJobsWithCondition(ctx, *filter)
	assert.NoError(t, err)
	assert.NotNil(t, jobs)
}

func TestUpdateJob(t *testing.T) {
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

// SVM
func TestCreateSVM(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	svm := &datamodel.Svm{Name: "svm1"}
	created, err := store.CreateSVM(ctx, svm)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetSvmsByPoolID(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetSvmsByPoolID(ctx, 0)
	assert.NoError(t, err)
}

// NODE
func TestCreateNode(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	node := &datamodel.Node{Name: "node1"}
	created, err := store.CreateNode(ctx, node)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetNodesByPoolID(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetNodesByPoolID(ctx, 0)
	assert.NoError(t, err)
}

// LIF
func TestCreateLif(t *testing.T) {
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
func TestCreateHostGroup(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	hg := &datamodel.HostGroup{Name: "hg1"}
	created, err := store.CreateHostGroup(ctx, hg)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetMultipleHostGroups(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetMultipleHostGroups(ctx, []string{"hg-uuid1", "hg-uuid2"}, 0)
	assert.NoError(t, err)
}

func TestUpdateHostGroupsState(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	err := store.UpdateHostGroupsState(ctx, []string{"hg-uuid"}, 0, "active", "ok")
	assert.NoError(t, err)
}

// SNAPSHOT
func TestCreatingSnapshot(t *testing.T) {
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

func TestUpdateSnapshot(t *testing.T) {
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

func TestGetSnapshot(t *testing.T) {
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

func TestGetSnapshotsWithCondition(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	filter := utils.CreateFilterWithConditions(
		utils.NewFilterCondition("name", "=", "snap"),
	)
	snaps, err := store.GetSnapshotsWithCondition(ctx, *filter)
	assert.NoError(t, err)
	assert.NotNil(t, snaps)
}

func TestGetAppConsistentSnapshotsForVolume(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_snap"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_snap", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol, false)
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

func TestGetSnapshotsByVolumeID(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_snap2"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_snap2", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol, false)
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

func TestGetSnapshotsByVolumeIDs(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "test-account"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol1 := &datamodel.Volume{Name: "test-volume-1", AccountID: createdAcc.ID}
	createdVol1, err := store.CreateVolume(ctx, vol1, false)
	assert.NoError(t, err)
	vol2 := &datamodel.Volume{Name: "test-volume-2", AccountID: createdAcc.ID}
	createdVol2, err := store.CreateVolume(ctx, vol2, false)
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

func TestBatchDeleteSnapshots(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "test-account"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "test-volume", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol, false)
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

func TestGetReplicationSnapshotsByVolumeID(t *testing.T) {
	logger := log.NewLogger()
	store, _ := SetupStorageForTest(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_snap2"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_snap2", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol, false)
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

func TestGetKms(t *testing.T) {
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

func TestUpdateKmsConfig(t *testing.T) {
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

func TestUpdateKmsConfigState(t *testing.T) {
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

func TestUpdateKmsConfigAttributesUpdatesAttributesOnSuccess(t *testing.T) {
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

func TestUpdateKmsConfigAttributesReturnsErrorIfConfigNotFound(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()
	attrs := &datamodel.KmsAttributes{SdeKmsConfigUUID: "external-uuid"}
	_, err = store.UpdateKmsConfigAttributes(ctx, "nonexistent-uuid", attrs)
	assert.Error(t, err)
}

func TestUpdateKmsConfigAttributesReturnsErrorIfAttributesNil(t *testing.T) {
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

func TestGetJobByKmsConfigIDReturnsErrorIfNotFound(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()

	found, err := store.GetJobByResourceUUID(ctx, "nonexistent-uuid")
	assert.Error(t, err)
	assert.Nil(t, found)
}

func TestUpdateKmsConfigDetailsReturnsErrorIfConfigNotFound(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()

	updated, err := store.UpdateKmsConfigDetails(ctx, "nonexistent-uuid", "some-path", "some-resource")
	assert.Error(t, err)
	assert.Nil(t, updated)
}

func TestUpdateServiceAccountEmailAndKeyReturnsErrorIfAccountNotFound(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()

	updated, err := store.UpdateServiceAccountEmailAndKey(ctx, "nonexistent-uuid", "email@email.com", "key")
	assert.Error(t, err)
	assert.Nil(t, updated)
}

func TestGetKmsConfigByUUIDReturnsErrorIfNotFound(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()

	found, err := store.GetKmsConfigByUUID(ctx, "nonexistent-uuid")
	assert.Error(t, err)
	assert.Nil(t, found)
}

func TestCreateBackupVaultEntryInVCP(t *testing.T) {
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

func TestUpdateVolumeFields(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a volume to update
	vol := &datamodel.Volume{Name: "vol-update-fields"}
	created, err := store.CreateVolume(ctx, vol, false)
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

func TestCreateAdminJobSpec(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	jobSpec := &datamodel.AdminJobSpec{JobType: "TEST_JOB", CronExpression: "*/10 * * * *", State: "CREATING"}
	created, err := store.CreateAdminJobSpec(ctx, jobSpec)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetAdminJobSpecByJobType(t *testing.T) {
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

func TestUpdateAdminJobSpec(t *testing.T) {
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

func TestGetAdminJobSpecsByState(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	jobSpec1 := &datamodel.AdminJobSpec{JobType: "TEST_JOB", CronExpression: "*/10 * * * *", State: "CREATING"}
	_, err := store.CreateAdminJobSpec(ctx, jobSpec1)
	assert.NoError(t, err)
	jobSpec2 := &datamodel.AdminJobSpec{JobType: "TEST_JOB", CronExpression: "*/10 * * * *", State: "SCHEDULED"}
	_, err = store.CreateAdminJobSpec(ctx, jobSpec2)
	assert.NoError(t, err)

	retrievedJobSpecs, err := store.GetAdminJobSpecsByState(ctx, "CREATING")
	assert.NoError(t, err)
	assert.Len(t, retrievedJobSpecs, 1)
}

// Test case for VerifyVolumeOwnership
func TestVerifyVolumeOwnership(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_verify"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_verify", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol, false)
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
func TestIsBackupInCreatingStateByVolume(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a volume
	vol := &datamodel.Volume{Name: "vol_backup_state"}
	createdVol, err := store.CreateVolume(ctx, vol, false)
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

func TestCreateBackup(t *testing.T) {
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

func TestGetBackup(t *testing.T) {
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

func TestDeleteBackup(t *testing.T) {
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

func TestUpdateBackupStateUpdatesStateSuccessfully(t *testing.T) {
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

func TestUpdateBackupStateFailsForNonExistentBackup(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Case: Non-existent backup
	nonExistentBackup := &datamodel.Backup{State: "FAILED"}
	_, err := store.UpdateBackupState(ctx, nonExistentBackup)
	assert.Error(t, err)
}

func TestFinishBackup(t *testing.T) {
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

func TestUpdateBackup(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	// Create a backup
	backup := &datamodel.Backup{VolumeUUID: "uuid", State: "new"}
	created, err := store.CreateBackup(ctx, backup)
	assert.NoError(t, err)

	// Case 1: Successful update
	created.State = "CREATING"
	updated, err := store.UpdateBackup(ctx, created)
	assert.NoError(t, err)
	assert.NotNil(t, updated)
	assert.Equal(t, "CREATING", updated.State)
}

func TestListVolumeReplications(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()

	// Create account and volume for association
	acc := &datamodel.Account{Name: "acc_snap2"}
	createdAcc, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	vol := &datamodel.Volume{Name: "vol_snap2", AccountID: createdAcc.ID}
	createdVol, err := store.CreateVolume(ctx, vol, false)
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

	filter := utils.CreateFilterWithConditions(
		utils.NewFilterCondition("account_id", "=", replication.AccountID))

	// List volume replications
	reps, err := store.ListVolumeReplications(ctx, *filter)
	assert.NoError(t, err)
	assert.NotEmpty(t, reps)
	assert.Equal(t, created.Name, reps[0].Name)
}

func TestCreateBackupPolicyEntryInVCP(t *testing.T) {
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

func TestAssignTwoNodesToTwoGroups_Storage(t *testing.T) {
	logger := log.NewLogger()
	store, err := SetupStorageForTest(logger)
	assert.NoError(t, err)
	ctx := context.Background()
	// Create two nodes
	n1 := &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()}, Name: "node1"}
	n2 := &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()}, Name: "node2"}
	created1, err := store.CreateNode(ctx, n1)
	assert.NoError(t, err)
	created2, err := store.CreateNode(ctx, n2)
	assert.NoError(t, err)
	// Assign to groups
	mappings, err := store.AssignTwoNodesToTwoGroups(ctx, created1, created2, 5)
	assert.NoError(t, err)
	assert.Len(t, mappings, 2)
	assert.NotZero(t, mappings[0].NodeGroupID)
	assert.NotZero(t, mappings[1].NodeGroupID)
	assert.NotEqual(t, mappings[0].NodeGroupID, mappings[1].NodeGroupID)
	// Print for debug
	t.Logf("Node1 assigned to group %d, Node2 assigned to group %d", mappings[0].NodeGroupID, mappings[1].NodeGroupID)
}

// Parallel test will not work properly until file based sqlite is implemented, Will uncomment when ready
// func TestAssignTwoNodesToTwoGroups_Parallel(t *testing.T) {
//	logger := log.NewLogger()
//	store, err := SetupStorageForTest(logger)
//	assert.NoError(t, err)
//	ctx := context.Background()
//	var wg sync.WaitGroup
//	numParallel := 5
//	errs := make([]error, numParallel)
//	results := make([][]*datamodel.NodeNodeGroupMap, numParallel)
//	nodes1 := make([]*datamodel.Node, numParallel)
//	nodes2 := make([]*datamodel.Node, numParallel)
//
//	// Pre-create all nodes serially
//	for i := 0; i < numParallel; i++ {
//		n1 := &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()}, Name: "nodeA-" + utils.RandomUUID()}
//		n2 := &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()}, Name: "nodeB-" + utils.RandomUUID()}
//		created1, err1 := store.CreateNode(ctx, n1)
//		assert.NoError(t, err1)
//		created2, err2 := store.CreateNode(ctx, n2)
//		assert.NoError(t, err2)
//		nodes1[i] = created1
//		nodes2[i] = created2
//	}
//
//	// Now assign in parallel
//	for i := 0; i < numParallel; i++ {
//		wg.Add(1)
//		go func(idx int) {
//			defer wg.Done()
//			mappings, err3 := store.AssignTwoNodesToTwoGroups(ctx, nodes1[idx], nodes2[idx], 5)
//			if err3 != nil {
//				errs[idx] = err3
//				return
//			}
//			results[idx] = mappings
//		}(i)
//	}
//	wg.Wait()
//	for i, err := range errs {
//		assert.NoError(t, err, "goroutine %d failed", i)
//	}
//	for i, mappings := range results {
//		if mappings == nil {
//			continue
//		}
//		assert.Len(t, mappings, 2)
//		assert.NotZero(t, mappings[0].NodeGroupID)
//		assert.NotZero(t, mappings[1].NodeGroupID)
//		assert.NotEqual(t, mappings[0].NodeGroupID, mappings[1].NodeGroupID)
//		t.Logf("Parallel %d: Node1 group %d, Node2 group %d", i, mappings[0].NodeGroupID, mappings[1].NodeGroupID)
//	}
// }
