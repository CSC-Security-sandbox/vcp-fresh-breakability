package database

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestNewTestStorage(t *testing.T) {
	logger := &log.MockLogger{}
	store, err := NewTestStorage(logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.NotNil(t, store.db)
	assert.NotNil(t, store.dataStore)
}

func TestClearInMemoryDB(t *testing.T) {
	logger := &log.MockLogger{}
	store, err := NewTestStorage(logger)
	assert.NoError(t, err)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)
}

func TestHealthCheckAndClose(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	assert.NoError(t, store.HealthCheck())
	assert.NoError(t, store.Close())
	// After close, HealthCheck should fail
	err := store.HealthCheck()
	assert.Error(t, err)
}

func TestWithTransaction_Success(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	err := store.WithTransaction(ctx, func(tx Transaction) error {
		return nil
	})
	assert.NoError(t, err)
}

func TestWithTransaction_Error(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	err := store.WithTransaction(ctx, func(tx Transaction) error {
		return errors.New("fail")
	})
	assert.Error(t, err)
}

func TestWithTransaction_Panic(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
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
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	db := store.DB()
	assert.NotNil(t, db)
}

func TestIsDatabaseExistsError(t *testing.T) {
	err := errors.New("some error")
	assert.False(t, isDatabaseExistsError(err))
}

func TestCreatingPool(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	// add logger to context
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "creatingpool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetPool(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "getpool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	found, err := store.GetPool(ctx, created.UUID, 0)
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

func TestUpdatePool(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "updatepool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	created.Name = "updatedpool"
	err = store.UpdatePool(ctx, created)
	assert.NoError(t, err)
}

func TestDeletePool(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "deletepool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	err = store.DeletePool(ctx, created)
	assert.NoError(t, err)
}

func TestDeletingPool(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	pool := &datamodel.Pool{Name: "deletingpool", Account: &datamodel.Account{}}
	created, err := store.CreatingPool(ctx, pool)
	assert.NoError(t, err)
	err = store.DeletingPool(ctx, created)
	assert.NoError(t, err)
}

func TestListPools(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.ListPools(ctx, [][]interface{}{})
	assert.NoError(t, err)
}

func TestGetPoolByName(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
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
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
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
	logger := &log.MockLogger{}
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
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol1"}
	created, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetVolume(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol2"}
	created, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)
	found, err := store.GetVolume(ctx, created.UUID)
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

func TestUpdateVolume(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol3"}
	created, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)
	created.Name = "vol3-updated"
	err = store.UpdateVolume(ctx, created)
	assert.NoError(t, err)
}

func TestDeleteVolume(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	vol := &datamodel.Volume{Name: "vol4"}
	created, err := store.CreateVolume(ctx, vol)
	assert.NoError(t, err)
	_, err = store.DeleteVolume(ctx, created.UUID)
	assert.NoError(t, err)
}

func TestGetVolumesByPoolID(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetVolumesByPoolID(ctx, 0)
	assert.NoError(t, err)
}

func TestGetVolumeCountByPoolID(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetVolumeCountByPoolID(ctx, 0)
	assert.NoError(t, err)
}

func TestGetMultipleVolumes(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetMultipleVolumes(ctx, [][]interface{}{})
	assert.NoError(t, err)
}

// ACCOUNT
func TestCreateAccount(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	acc := &datamodel.Account{Name: "acc1"}
	created, err := store.CreateAccount(ctx, acc)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetAccount(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
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
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	job := &datamodel.Job{ResourceName: "job1"}
	created, err := store.CreateJob(ctx, job)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetJob(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	job := &datamodel.Job{RequestID: "job2"}
	created, err := store.CreateJob(ctx, job)
	assert.NoError(t, err)
	found, err := store.GetJob(ctx, created.UUID)
	assert.NoError(t, err)
	assert.NotNil(t, found)
}

func TestUpdateJob(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	job := &datamodel.Job{ResourceName: "job3"}
	created, err := store.CreateJob(ctx, job)
	assert.NoError(t, err)
	err = store.UpdateJob(ctx, created.UUID, "done")
	assert.NoError(t, err)
}

// SVM
func TestCreateSVM(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	svm := &datamodel.Svm{Name: "svm1"}
	created, err := store.CreateSVM(ctx, svm)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetSvmsByPoolID(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetSvmsByPoolID(ctx, 0)
	assert.NoError(t, err)
}

// NODE
func TestCreateNode(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	node := &datamodel.Node{Name: "node1"}
	created, err := store.CreateNode(ctx, node)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetNodesByPoolID(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetNodesByPoolID(ctx, 0)
	assert.NoError(t, err)
}

// LIF
func TestCreateLif(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	lif := &datamodel.Lif{Name: "lif1"}
	created, err := store.CreateLif(ctx, lif)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

// HOSTGROUP
func TestCreateHostGroup(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	hg := &datamodel.HostGroup{Name: "hg1"}
	created, err := store.CreateHostGroup(ctx, hg)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestGetMultipleHostGroups(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	_, err := store.GetMultipleHostGroups(ctx, []string{"hg-uuid1", "hg-uuid2"}, 0)
	assert.NoError(t, err)
}

func TestUpdateHostGroupsState(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	err := store.UpdateHostGroupsState(ctx, []string{"hg-uuid"}, 0, "active", "ok")
	assert.NoError(t, err)
}

// SNAPSHOT
func TestCreatingSnapshot(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	snap := &datamodel.Snapshot{Name: "snap1"}
	created, err := store.CreatingSnapshot(ctx, snap)
	assert.NoError(t, err)
	assert.NotNil(t, created)
	assert.Equal(t, "snap1", created.Name)
}

func TestUpdateSnapshot(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	snap := &datamodel.Snapshot{Name: "snap2"}
	created, err := store.CreatingSnapshot(ctx, snap)
	assert.NoError(t, err)
	created.Name = "snap2-updated"
	err = store.UpdateSnapshot(ctx, created)
	assert.NoError(t, err)
}

func TestGetSnapshot(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	snap := &datamodel.Snapshot{Name: "snap3"}
	created, err := store.CreatingSnapshot(ctx, snap)
	assert.NoError(t, err)
	found, err := store.GetSnapshot(ctx, created.UUID)
	assert.NoError(t, err)
	assert.NotNil(t, found)
	assert.Equal(t, "snap3", found.Name)
}

func TestGetAppConsistentSnapshotsForVolume(t *testing.T) {
	logger := &log.MockLogger{}
	store, _ := NewTestStorage(logger)
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
