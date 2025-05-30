package database

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/repository"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	dblogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/logger"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	pgInvalidCatalogName = "3D000" // Database doesn't exist
	pgDuplicateDatabase  = "42P04" // Database already exists
	DatabaseTypePostgres = "postgres"
	DatabaseTypeSQLite   = "sqlite3"
)

var (
	logSQLEnabled = env.GetBool("LOG_SQL", false)
)

type PersistenceStore struct {
	config DbConfig
	db     *gormwrapper.Wrapper
	mu     sync.RWMutex
	logger log.Logger

	dataStore *repository.DataStoreRepository
}

func init() {
	Register("postgres", NewStorage)
	Register("sqlite3", NewStorage)
}

func NewStorage(config DbConfig, logger log.Logger) (Storage, error) {
	db := &PersistenceStore{
		config: config,
		logger: logger,
	}
	return db, nil
}

// NewTestStorage creates a new instance of PersistenceStore for testing with an in-memory SQLite database.
func NewTestStorage(logger log.Logger) (*PersistenceStore, error) {
	db, err := SetupInMemoryDB()
	if err != nil {
		return nil, err
	}

	wrapper := gormwrapper.New(db)
	return &PersistenceStore{
		db:        wrapper,
		logger:    logger,
		dataStore: repository.NewDataStoreRepository(wrapper),
	}, nil
}

// SetupInMemoryDB sets up an in-memory SQLite database for testing.
func SetupInMemoryDB() (*gorm.DB, error) {
	// Use ":memory:" for an in-memory database
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Perform any necessary migrations or setup here
	err = db.AutoMigrate(&datamodel.Pool{},
		&datamodel.Volume{},
		&datamodel.Account{},
		&datamodel.Svm{},
		&datamodel.Node{},
		&datamodel.Lif{},
		&datamodel.HostGroup{},
		&datamodel.Job{},
		&datamodel.Snapshot{},
		&datamodel.VolumeReplication{},
		&datamodel.ServiceAccount{},
		&datamodel.KmsConfig{},
	)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// ClearInMemoryDB deletes all data from the in-memory database.
func ClearInMemoryDB(db *gorm.DB) error {
	tables := []interface{}{
		&datamodel.Pool{},
		&datamodel.Volume{},
		&datamodel.Account{},
		&datamodel.Svm{},
		&datamodel.Node{},
		&datamodel.Lif{},
		&datamodel.HostGroup{},
		&datamodel.Job{},
		&datamodel.Snapshot{},
		&datamodel.KmsConfig{},
		&datamodel.ServiceAccount{},
	}

	for _, table := range tables {
		if err := db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(table).Error; err != nil {
			return err
		}
	}
	return nil
}

func (s *PersistenceStore) SetupDatabase(ctx context.Context) error {
	if err := s.connect(true); err != nil {
		return fmt.Errorf("database initialization failed: %w", err)
	}
	// Create database and user
	if err := s.createDatabaseAndUser(); err != nil {
		return fmt.Errorf("failed to setup database: %w", err)
	}
	return nil
}

func (s *PersistenceStore) Connect(isAdmin bool) error {
	return s.connect(isAdmin)
}

func (s *PersistenceStore) connect(isAdmin bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil && s.HealthCheck() == nil {
		return nil // Already connected
	}

	db, err := s.createConnection(isAdmin)
	if err != nil {
		return fmt.Errorf("failed to create database connection: %w", err)
	}

	s.db = gormwrapper.New(db)
	s.dataStore = repository.NewDataStoreRepository(s.db)
	return nil
}

// createConnection establishes a new database connection
func (s *PersistenceStore) createConnection(isAdmin bool) (*gorm.DB, error) {
	logLevel := logger.Error
	if logSQLEnabled {
		logLevel = logger.Info
	}

	gormConfig := &gorm.Config{
		Logger: dblogger.NewGormLogger(s.logger, logLevel),
		NowFunc: func() time.Time {
			return time.Now().UTC()
		},
		PrepareStmt:            true,
		SkipDefaultTransaction: true,
	}

	var dialector gorm.Dialector
	switch s.config.Type {
	case DatabaseTypePostgres:
		dsn, err := s.getPostgresDSN(isAdmin)
		if err != nil {
			return nil, fmt.Errorf("failed to get DSN: %w", err)
		}
		dialector = postgres.Open(dsn)
	case DatabaseTypeSQLite:
		dialector = sqlite.Open(fmt.Sprintf("file:%v?mode=memory&cache=shared", utils.RandomUUID()))
	default:
		return nil, fmt.Errorf("unsupported database type: %s", s.config.Type)
	}

	db, err := gorm.Open(dialector, gormConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get SQL DB: %w", err)
	}

	sqlDB.SetMaxOpenConns(s.config.MaxOpenConns)
	sqlDB.SetMaxIdleConns(s.config.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(s.config.ConnMaxLifetime)

	return db, nil
}

// getPostgresDSN constructs the database connection string
func (s *PersistenceStore) getPostgresDSN(isAdmin bool) (string, error) {
	var username, password string
	if isAdmin {
		username = s.config.AdminUser
		password = s.config.AdminPassword
	} else {
		username = s.config.User
		password = s.config.Password
	}

	query := url.Values{}
	query.Add("sslmode", s.config.SSLMode)
	query.Add("connect_timeout", strconv.Itoa(s.config.ConnectionTimeout))
	query.Add("timezone", s.config.TimeZone)
	// For admin connections, connect to the admin database
	dbName := s.config.Name
	if isAdmin {
		dbName = "postgres"
	}

	u := &url.URL{
		Scheme:   s.config.Type,
		User:     url.UserPassword(username, password),
		Host:     fmt.Sprintf("%s:%s", s.config.Host, s.config.Port),
		Path:     dbName,
		RawQuery: query.Encode(),
	}

	return u.String(), nil
}

func (s *PersistenceStore) createDatabaseAndUser() error {
	createDBSQL := fmt.Sprintf(
		`CREATE DATABASE %s WITH OWNER = %s  CONNECTION LIMIT = -1`,
		pq.QuoteIdentifier(s.config.Name),
		pq.QuoteIdentifier(s.config.AdminUser))

	if err := s.db.Exec(createDBSQL).Error(); err != nil && !isDatabaseExistsError(err) {
		return fmt.Errorf("create database failed: %w", err)
	}

	// TODO : explore different authentication methods
	createUserSQL := fmt.Sprintf(
		`DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s) THEN
				CREATE USER %s WITH PASSWORD %s;
			END IF;
		END
		$$`,
		pq.QuoteLiteral(s.config.User),
		pq.QuoteIdentifier(s.config.User),
		pq.QuoteLiteral(s.config.Password),
	)

	if err := s.db.Exec(createUserSQL).Error(); err != nil {
		return fmt.Errorf("create user failed: %w", err)
	}

	// Grant privileges - NEW FIXED VERSION
	grantDatabaseSQL := fmt.Sprintf(
		`GRANT ALL PRIVILEGES ON DATABASE %s TO %s`,
		pq.QuoteIdentifier(s.config.Name),
		pq.QuoteIdentifier(s.config.User))

	if err := s.db.Exec(grantDatabaseSQL).Error(); err != nil {
		return fmt.Errorf("grant database privileges failed: %w", err)
	}

	grantSchemaSQL := fmt.Sprintf(
		`GRANT ALL PRIVILEGES ON SCHEMA public TO %s`,
		pq.QuoteIdentifier(s.config.User))

	if err := s.db.Exec(grantSchemaSQL).Error(); err != nil {
		return fmt.Errorf("grant schema privileges failed: %w", err)
	}

	return nil
}

func (s *PersistenceStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db == nil {
		return nil
	}

	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (s *PersistenceStore) HealthCheck() error {
	if s.db == nil {
		return fmt.Errorf("database connection is closed")
	}

	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Ping()
}

func (s *PersistenceStore) WithTransaction(ctx context.Context, fn func(Transaction) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.db == nil {
		return errors.New("database connection is closed")
	}

	tx := s.db.WithContext(ctx).Begin()
	if tx.Error() != nil {
		return tx.Error()
	}

	var err error
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	err = fn(tx)
	return err
}

func (s *PersistenceStore) DB() *gorm.DB {
	return s.db.GORM()
}

func isDatabaseExistsError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgDuplicateDatabase
}

// Implement PersistenceStore interface by delegating to repositories

func (s *PersistenceStore) CreatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	return s.dataStore.CreatedPool(ctx, pool)
}

func (s *PersistenceStore) CreatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	return s.dataStore.CreatingPool(ctx, pool)
}
func (s *PersistenceStore) GetPool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.Pool, error) {
	return s.dataStore.GetPool(ctx, poolUUID, accountID)
}

func (s *PersistenceStore) UpdatePool(ctx context.Context, pool *datamodel.Pool) error {
	return s.dataStore.UpdatePool(ctx, pool)
}

func (s *PersistenceStore) DeletePool(ctx context.Context, pool *datamodel.Pool) error {
	return s.dataStore.DeletePool(ctx, pool)
}

func (s *PersistenceStore) DeletingPool(ctx context.Context, pool *datamodel.Pool) error {
	return s.dataStore.DeletingPool(ctx, pool)
}

func (s *PersistenceStore) ListPools(ctx context.Context, conditions [][]interface{}) ([]*datamodel.Pool, error) {
	return s.dataStore.ListPools(ctx, conditions)
}

func (s *PersistenceStore) GetPoolByName(ctx context.Context, conditions [][]interface{}) (*datamodel.Pool, error) {
	return s.dataStore.GetPoolByName(ctx, conditions)
}

func (s *PersistenceStore) SavePoolWithVsaClusterDetails(ctx context.Context, pool *datamodel.Pool, cluster *datamodel.ClusterDetails) error {
	return s.dataStore.SavePoolWithVsaClusterDetails(ctx, pool, cluster)
}

func (s *PersistenceStore) CreateVolume(ctx context.Context, volume *datamodel.Volume) (*datamodel.Volume, error) {
	return s.dataStore.CreateVolume(ctx, volume)
}

func (s *PersistenceStore) CreateVolumeReplication(ctx context.Context, volumeRep *datamodel.VolumeReplication) (*datamodel.VolumeReplication, error) {
	return s.dataStore.CreateVolumeReplication(ctx, volumeRep)
}

func (s *PersistenceStore) GetVolumeReplication(ctx context.Context, id string) (*datamodel.VolumeReplication, error) {
	return s.dataStore.GetVolumeReplication(ctx, id)
}

func (s *PersistenceStore) UpdateVolumeReplicationStates(ctx context.Context, volumeRep *datamodel.VolumeReplication) error {
	return s.dataStore.UpdateVolumeReplicationStates(ctx, volumeRep)
}

func (s *PersistenceStore) UpdateVolumeReplicationTransferStats(ctx context.Context, volumeRep *datamodel.VolumeReplication) error {
	return s.dataStore.UpdateVolumeReplicationTransferStats(ctx, volumeRep)
}

func (s *PersistenceStore) DeleteVolumeReplication(ctx context.Context, volumeReplicationID string) (*datamodel.VolumeReplication, error) {
	return s.dataStore.DeleteVolumeReplication(ctx, volumeReplicationID)
}

func (s *PersistenceStore) GetVolume(ctx context.Context, id string) (*datamodel.Volume, error) {
	return s.dataStore.GetVolume(ctx, id)
}

func (s *PersistenceStore) UpdateVolume(ctx context.Context, volume *datamodel.Volume) error {
	return s.dataStore.UpdateVolume(ctx, volume)
}

func (s *PersistenceStore) DeleteVolume(ctx context.Context, id string) (*datamodel.Volume, error) {
	return s.dataStore.DeleteVolume(ctx, id)
}

func (s *PersistenceStore) UpdateVolumeState(ctx context.Context, id string, state string, stateDetails string) (*datamodel.Volume, error) {
	return s.dataStore.UpdateVolumeState(ctx, id, state, stateDetails)
}

func (s *PersistenceStore) ListVolumes(ctx context.Context) ([]*datamodel.Volume, error) {
	return s.dataStore.ListVolumes(ctx)
}

func (s *PersistenceStore) GetVolumesByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Volume, error) {
	return s.dataStore.GetVolumesByPoolID(ctx, poolID)
}

func (s *PersistenceStore) GetVolumeCountByPoolID(ctx context.Context, poolID int64) (int64, error) {
	return s.dataStore.GetVolumeCountByPoolID(ctx, poolID)
}

func (s *PersistenceStore) GetMultipleVolumes(ctx context.Context, conditions [][]interface{}) ([]*datamodel.Volume, error) {
	return s.dataStore.GetMultipleVolumes(ctx, conditions)
}

func (s *PersistenceStore) VerifyVolumeOwnership(ctx context.Context, volumeID string, accountName string) (*datamodel.Volume, error) {
	return s.dataStore.VerifyVolumeOwnership(ctx, volumeID, accountName)
}

func (s *PersistenceStore) GetAccount(ctx context.Context, name string) (*datamodel.Account, error) {
	return s.dataStore.GetAccount(ctx, name)
}

func (s *PersistenceStore) CreateAccount(ctx context.Context, account *datamodel.Account) (*datamodel.Account, error) {
	return s.dataStore.CreateAccount(ctx, account)
}

func (s *PersistenceStore) CreateJob(ctx context.Context, job *datamodel.Job) (*datamodel.Job, error) {
	return s.dataStore.CreateJob(ctx, job)
}

func (s *PersistenceStore) UpdateJob(ctx context.Context, id string, status string, trackingID int, errorDetails []byte) error {
	return s.dataStore.UpdateJob(ctx, id, status, trackingID, errorDetails)
}

func (s *PersistenceStore) GetJob(ctx context.Context, id string) (*datamodel.Job, error) {
	return s.dataStore.GetJob(ctx, id)
}

func (s *PersistenceStore) GetJobsWithCondition(ctx context.Context, filter utils.Filter) ([]*datamodel.Job, error) {
	return s.dataStore.GetJobsWithCondition(ctx, filter)
}

func (s *PersistenceStore) GetPoolByVendorID(ctx context.Context, vendorID string) (*datamodel.Pool, error) {
	return s.dataStore.GetPoolByVendorID(ctx, vendorID)
}

func (s *PersistenceStore) GetSvmForPoolID(ctx context.Context, poolID int64) (*datamodel.Svm, error) {
	return s.dataStore.GetSvmForPoolID(ctx, poolID)
}

func (s *PersistenceStore) CreateNode(ctx context.Context, node *datamodel.Node) (*datamodel.Node, error) {
	return s.dataStore.CreateNode(ctx, node)
}

func (s *PersistenceStore) GetNodesByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Node, error) {
	return s.dataStore.GetNodesByPoolID(ctx, poolID)
}

func (s *PersistenceStore) CreateSVM(ctx context.Context, svm *datamodel.Svm) (*datamodel.Svm, error) {
	return s.dataStore.CreateSVM(ctx, svm)
}

func (s *PersistenceStore) GetSvmsByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Svm, error) {
	return s.dataStore.GetSvmsByPoolID(ctx, poolID)
}

func (s *PersistenceStore) CreateLif(ctx context.Context, lif *datamodel.Lif) (*datamodel.Lif, error) {
	return s.dataStore.CreateLif(ctx, lif)
}

func (s *PersistenceStore) GetLifByNodeID(ctx context.Context, nodeID int64, accountID int64) (*datamodel.Lif, error) {
	return s.dataStore.GetLifByNodeID(ctx, nodeID, 0)
}

func (s *PersistenceStore) DeleteSVM(ctx context.Context, svm *datamodel.Svm) error {
	return s.dataStore.DeleteSVM(ctx, svm)
}

func (s *PersistenceStore) DeletingSVM(ctx context.Context, svm *datamodel.Svm) error {
	return s.dataStore.DeletingSVM(ctx, svm)
}

func (s *PersistenceStore) DeleteLif(ctx context.Context, lif *datamodel.Lif) error {
	return s.dataStore.DeleteLif(ctx, lif)
}

func (s *PersistenceStore) DeleteNode(ctx context.Context, node *datamodel.Node) error {
	return s.dataStore.DeleteNode(ctx, node)
}

func (s *PersistenceStore) DeletingNode(ctx context.Context, node *datamodel.Node) error {
	return s.dataStore.DeletingNode(ctx, node)
}

func (s *PersistenceStore) GetLifForNode(ctx context.Context, nodeID int64, accountID int64) (*datamodel.Lif, error) {
	return s.dataStore.GetLifForNode(ctx, nodeID, accountID)
}

func (s *PersistenceStore) GetHostGroup(ctx context.Context, hostGroupUUID string, accountID int64) (*datamodel.HostGroup, error) {
	return s.dataStore.GetHostGroup(ctx, hostGroupUUID, accountID)
}

func (s *PersistenceStore) CreateHostGroup(ctx context.Context, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error) {
	return s.dataStore.CreateHostGroup(ctx, hostGroup)
}

func (s *PersistenceStore) GetMultipleHostGroups(ctx context.Context, ids []string, accountID int64) ([]*datamodel.HostGroup, error) {
	return s.dataStore.GetMultipleHostGroups(ctx, ids, accountID)
}

func (s *PersistenceStore) DeleteHostGroup(ctx context.Context, hostGroupUUID string, accountID int64) (*datamodel.HostGroup, error) {
	return s.dataStore.DeleteHostGroup(ctx, hostGroupUUID, accountID)
}

func (s *PersistenceStore) UpdateHostGroupsState(ctx context.Context, hostGroupUUID []string, accountID int64, state string, stateDetails string) error {
	return s.dataStore.UpdateHostGroupsState(ctx, hostGroupUUID, accountID, state, stateDetails)
}

func (s *PersistenceStore) CreatingSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) (*datamodel.Snapshot, error) {
	return s.dataStore.CreatingSnapshot(ctx, snapshot)
}

func (s *PersistenceStore) UpdateSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) (*datamodel.Snapshot, error) {
	return s.dataStore.UpdateSnapshot(ctx, snapshot)
}

func (s *PersistenceStore) GetSnapshot(ctx context.Context, uuid string) (*datamodel.Snapshot, error) {
	return s.dataStore.GetSnapshotByUUID(ctx, uuid)
}

func (s *PersistenceStore) GetSnapshotsWithCondition(ctx context.Context, filter utils.Filter) ([]*datamodel.Snapshot, error) {
	return s.dataStore.GetSnapshotsWithCondition(ctx, filter)
}

func (s *PersistenceStore) DeletingSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error {
	return s.dataStore.DeletingSnapshot(ctx, snapshot)
}

func (s *PersistenceStore) DeleteSnapshot(ctx context.Context, id string) (*datamodel.Snapshot, error) {
	return s.dataStore.DeleteSnapshot(ctx, id)
}

func (s *PersistenceStore) GetAppConsistentSnapshotsForVolume(ctx context.Context, accountID, volumeID int64) ([]*datamodel.Snapshot, error) {
	return s.dataStore.GetAppConsistentSnapshotsForVolume(ctx, accountID, volumeID)
}

func (s *PersistenceStore) GetSnapshotsByVolumeID(ctx context.Context, volumeID int64) ([]*datamodel.Snapshot, error) {
	return s.dataStore.GetSnapshotsByVolumeID(ctx, volumeID)
}

func (s *PersistenceStore) GetMultipleKmsConfigs(ctx context.Context, conditions [][]interface{}) ([]*datamodel.KmsConfig, error) {
	return s.dataStore.GetMultipleKmsConfigs(ctx, conditions)
}
