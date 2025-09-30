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
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	dblogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/logger"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var logSQLEnabled = env.GetBool("LOG_SQL", false)

type PersistenceStore struct {
	config dbutils.DbConfig
	db     *gormwrapper.Wrapper
	mu     sync.RWMutex
	logger log.Logger

	dataStore retryEngine
}

func init() {
	Register("postgres", NewStorage)
	Register("sqlite3", NewStorage)
}

func NewStorage(config dbutils.DbConfig, logger log.Logger) (Storage, error) {
	db := &PersistenceStore{
		config: config,
		logger: logger,
	}
	return db, nil
}

// NewTestStorage creates a new instance of PersistenceStore for testing with an in-memory SQLite database.
func NewTestStorage(logger log.Logger) (Storage, error) {
	db, err := SetupInMemoryDB()
	if err != nil {
		return nil, err
	}

	wrapper := gormwrapper.New(db)
	return &PersistenceStore{
		db:        wrapper,
		logger:    logger,
		dataStore: retryEngine{dataStore: NewDataStoreRepository(wrapper)},
		config: dbutils.DbConfig{
			Type: dbutils.SQLite,
		},
	}, nil
}

// SetupInMemoryDB sets up an in-memory SQLite database for testing.
func SetupInMemoryDB() (*gorm.DB, error) {
	// Use ":memory:" for an in-memory database
	db, err := SetupTestDB()
	if err != nil {
		return nil, err
	}

	// Perform any necessary migrations or setup here
	err = db.AutoMigrate(getVcpModels()...)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s *PersistenceStore) Migrate(ctx context.Context) error {
	migrator, err := NewMigrator(s.config, s.logger)
	if err != nil {
		return err
	}
	err = migrator.Migrate(s.db, ctx)
	if err != nil {
		return err
	}
	// Ensure view is always in sync after migrations
	if err := migrator.CreateOrUpdateViews(s.db); err != nil {
		s.logger.Errorf("Failed to create or update views: %v", err)
	}
	return nil
}

func (s *PersistenceStore) Rollback(ctx context.Context) error {
	migrator, err := NewMigrator(s.config, s.logger)
	if err != nil {
		return err
	}
	return migrator.Rollback(s.db, ctx)
}

// SetupDatabase sets up the database for the PersistenceStore.
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
	s.dataStore = retryEngine{NewDataStoreRepository(s.db)}
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
		TranslateError:         true,
	}

	var dialector gorm.Dialector
	switch s.config.Type {
	case dbutils.Postgres:
		dsn, err := s.getPostgresDSN(isAdmin)
		if err != nil {
			return nil, fmt.Errorf("failed to get DSN: %w", err)
		}
		dialector = postgres.Open(dsn)
	case dbutils.SQLite:
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

func (s *PersistenceStore) WithTransaction(ctx context.Context, fn func(dbutils.Transaction) error) error {
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

func SetupStorageForTest(logger log.Logger) (Storage, error) {
	ctx := context.Background()
	store, err := NewTestStorage(logger)
	if err != nil {
		return nil, err
	}
	err = store.Migrate(ctx)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (s *PersistenceStore) DB() *gorm.DB {
	return s.db.GORM()
}

func isDatabaseExistsError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == dbutils.PgDuplicateDatabase
}

// Implement PersistenceStore interface by delegating to repositories

func (s *PersistenceStore) CreatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	return s.dataStore.CreatedPool(ctx, pool)
}

func (s *PersistenceStore) CreatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	return s.dataStore.CreatingPool(ctx, pool)
}

func (s *PersistenceStore) DescribePool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.PoolView, error) {
	return s.dataStore.DescribePool(ctx, poolUUID, accountID)
}

func (s *PersistenceStore) GetPool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.PoolView, error) {
	return s.dataStore.GetPool(ctx, poolUUID, accountID)
}

func (s *PersistenceStore) UpdatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	return s.dataStore.UpdatingPool(ctx, pool)
}

func (s *PersistenceStore) UpdatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	return s.dataStore.UpdatedPool(ctx, pool)
}

func (s *PersistenceStore) UpdatePoolSubnetNames(ctx context.Context, poolUUID, snHostProject string, subnetNames []string) error {
	return s.dataStore.UpdatePoolSubnetNames(ctx, poolUUID, snHostProject, subnetNames)
}

func (s *PersistenceStore) UpdatePoolState(ctx context.Context, pool *datamodel.Pool, state string, stateDetails string) (*datamodel.Pool, error) {
	return s.dataStore.UpdatePoolState(ctx, pool, state, stateDetails)
}

func (s *PersistenceStore) UpdatePoolFields(ctx context.Context, poolUUID string, updates map[string]interface{}) error {
	return s.dataStore.UpdatePoolFields(ctx, poolUUID, updates)
}

func (s *PersistenceStore) GetPoolsByAccountName(ctx context.Context, accountName string) ([]*datamodel.Pool, error) {
	return s.dataStore.GetPoolsByAccountName(ctx, accountName)
}

func (s *PersistenceStore) DeletePool(ctx context.Context, pool *datamodel.Pool) error {
	return s.dataStore.DeletePool(ctx, pool)
}

func (s *PersistenceStore) DeletingPool(ctx context.Context, pool *datamodel.Pool) error {
	return s.dataStore.DeletingPool(ctx, pool)
}

func (s *PersistenceStore) ListPools(ctx context.Context, filter *dbutils.Filter) ([]*datamodel.PoolView, error) {
	return s.dataStore.ListPools(ctx, filter)
}

func (s *PersistenceStore) ListPoolUUIDs(ctx context.Context, filter *dbutils.Filter) ([]*PoolIdentifier, error) {
	return s.dataStore.ListPoolUUIDs(ctx, filter)
}

func (s *PersistenceStore) ListPoolUUIDsPaginated(ctx context.Context, filter *dbutils.Filter, offset, limit int) ([]*PoolIdentifier, error) {
	return s.dataStore.ListPoolUUIDsPaginated(ctx, filter, offset, limit)
}

func (s *PersistenceStore) GetPoolsCount(ctx context.Context, filter *dbutils.Filter) (int64, error) {
	return s.dataStore.GetPoolsCount(ctx, filter)
}

func (s *PersistenceStore) GetPoolByName(ctx context.Context, conditions [][]interface{}) (*datamodel.PoolView, error) {
	return s.dataStore.GetPoolByName(ctx, conditions)
}

func (s *PersistenceStore) SavePoolWithVsaDetails(ctx context.Context, pool *datamodel.Pool, cluster *datamodel.ClusterDetails) error {
	return s.dataStore.SavePoolWithVsaDetails(ctx, pool, cluster)
}

func (s *PersistenceStore) UpdatePoolWithKmsConfigID(ctx context.Context, pool *datamodel.Pool, kmsConfigUUID string) (*datamodel.Pool, error) {
	return s.dataStore.UpdatePoolWithKmsConfigID(ctx, pool, kmsConfigUUID)
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

func (s *PersistenceStore) UpdateVolumeReplication(ctx context.Context, volumeRep *datamodel.VolumeReplication) error {
	return s.dataStore.UpdateVolumeReplication(ctx, volumeRep)
}

func (s *PersistenceStore) UpdateVolumeReplicationFields(ctx context.Context, volumeRepUUID string, updates map[string]interface{}) error {
	return s.dataStore.UpdateVolumeReplicationFields(ctx, volumeRepUUID, updates)
}

func (s *PersistenceStore) UpdateVolumeReplicationStates(ctx context.Context, volumeRep *datamodel.VolumeReplication) error {
	return s.dataStore.UpdateVolumeReplicationStates(ctx, volumeRep)
}

func (s *PersistenceStore) UpdateVolumeReplicationTransferStats(ctx context.Context, volumeRep *datamodel.VolumeReplication) error {
	return s.dataStore.UpdateVolumeReplicationTransferStats(ctx, volumeRep)
}

func (s *PersistenceStore) DeleteVolumeReplication(ctx context.Context, replication *datamodel.VolumeReplication) (*datamodel.VolumeReplication, error) {
	return s.dataStore.DeleteVolumeReplication(ctx, replication)
}

func (s *PersistenceStore) GetVolumeReplicationByProjectId(ctx context.Context, accountId int64) ([]*datamodel.VolumeReplication, error) {
	return s.dataStore.GetVolumeReplicationByProjectId(ctx, accountId)
}

func (s *PersistenceStore) GetVolumeReplicationCount(ctx context.Context, accountName string) (int64, error) {
	return s.dataStore.GetVolumeReplicationCount(ctx, accountName)
}

func (s *PersistenceStore) GetVolumeReplicationCountByVolumeID(ctx context.Context, volumeID int64) (int64, error) {
	return s.dataStore.GetVolumeReplicationCountByVolumeID(ctx, volumeID)
}

func (s *PersistenceStore) ListVolumeReplications(ctx context.Context, filter dbutils.Filter) ([]*datamodel.VolumeReplication, error) {
	return s.dataStore.ListVolumeReplications(ctx, filter)
}

func (s *PersistenceStore) GetVolume(ctx context.Context, id string) (*datamodel.Volume, error) {
	return s.dataStore.GetVolume(ctx, id)
}

func (s *PersistenceStore) DescribeVolume(ctx context.Context, id string) (*datamodel.Volume, error) {
	return s.dataStore.DescribeVolume(ctx, id)
}

func (s *PersistenceStore) GetVolumeWithAccountID(ctx context.Context, id string, accountID int64) (*datamodel.Volume, error) {
	return s.dataStore.GetVolumeWithAccountID(ctx, id, accountID)
}

func (s *PersistenceStore) GetVolumeByNameAndAccountID(ctx context.Context, id string, accountID int64) (*datamodel.Volume, error) {
	return s.dataStore.GetVolumeByNameAndAccountID(ctx, id, accountID)
}

func (s *PersistenceStore) GetVolumeByNameAccountIDAndZone(ctx context.Context, name string, accountID int64, primaryZone string) (*datamodel.Volume, error) {
	return s.dataStore.GetVolumeByNameAccountIDAndZone(ctx, name, accountID, primaryZone)
}

func (s *PersistenceStore) GetVolumeByName(ctx context.Context, name string) (*datamodel.Volume, error) {
	return s.dataStore.GetVolumeByName(ctx, name)
}

func (s *PersistenceStore) UpdateVolume(ctx context.Context, volume *datamodel.Volume) error {
	return s.dataStore.UpdateVolume(ctx, volume)
}

func (s *PersistenceStore) RevertedVolume(ctx context.Context, volume *datamodel.Volume, snapshot *datamodel.Snapshot) ([]*datamodel.Snapshot, error) {
	return s.dataStore.RevertedVolume(ctx, volume, snapshot)
}

func (s *PersistenceStore) UpdateVolumeFields(ctx context.Context, volumeUUID string, updates map[string]interface{}) error {
	return s.dataStore.UpdateVolumeFields(ctx, volumeUUID, updates)
}

func (s *PersistenceStore) BatchUpdateVolumeFields(ctx context.Context, updates []datamodel.VolumeFieldUpdate) error {
	return s.dataStore.BatchUpdateVolumeFields(ctx, updates)
}

func (s *PersistenceStore) UpdateKmsConfig(ctx context.Context, kmsUUID string, updates map[string]interface{}) error {
	return s.dataStore.UpdateKmsConfig(ctx, kmsUUID, updates)
}

func (s *PersistenceStore) IsKmsConfigInUse(ctx context.Context, kmsConfigUUID string) (bool, error) {
	return s.dataStore.IsKmsConfigInUse(ctx, kmsConfigUUID)
}

func (s *PersistenceStore) DeleteVolume(ctx context.Context, id string) (*datamodel.Volume, error) {
	return s.dataStore.DeleteVolume(ctx, id)
}

func (s *PersistenceStore) DeleteVolumeAndChildResources(ctx context.Context, volumeUUID string) (*datamodel.Volume, error) {
	return s.dataStore.DeleteVolumeAndChildResources(ctx, volumeUUID)
}

func (s *PersistenceStore) UpdateVolumeState(ctx context.Context, id string, state string, stateDetails string) (*datamodel.Volume, error) {
	return s.dataStore.UpdateVolumeState(ctx, id, state, stateDetails)
}

func (s *PersistenceStore) ListVolumes(ctx context.Context, conditions [][]interface{}) ([]*datamodel.Volume, error) {
	return s.dataStore.ListVolumes(ctx, conditions)
}

func (s *PersistenceStore) GetVolumeCount(ctx context.Context, accountName string) (int64, error) {
	return s.dataStore.GetVolumeCount(ctx, accountName)
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

func (s *PersistenceStore) GetAccountByUUID(ctx context.Context, uuid string) (*datamodel.Account, error) {
	return s.dataStore.GetAccountByUUID(ctx, uuid)
}

func (s *PersistenceStore) UpdateAccountStateForHandleResource(ctx context.Context, accountUUID string, newState string) error {
	return s.dataStore.UpdateAccountStateForHandleResource(ctx, accountUUID, newState)
}

func (s *PersistenceStore) CreateAccount(ctx context.Context, account *datamodel.Account) (*datamodel.Account, error) {
	return s.dataStore.CreateAccount(ctx, account)
}

func (s *PersistenceStore) GetVolumeLatestBackupMap(ctx context.Context) (map[int64]*datamodel.VolumeLatestBackup, error) {
	return s.dataStore.GetVolumeLatestBackupMap(ctx)
}

func (s *PersistenceStore) CreateJob(ctx context.Context, job *datamodel.Job) (*datamodel.Job, error) {
	return s.dataStore.CreateJob(ctx, job)
}

func (s *PersistenceStore) UpdateJob(ctx context.Context, id string, status string, trackingID int, errorDetails string) error {
	return s.dataStore.UpdateJob(ctx, id, status, trackingID, errorDetails)
}

func (s *PersistenceStore) UpdateJobAttributes(ctx context.Context, jobUUID string, jobAttributes *datamodel.JobAttributes) error {
	return s.dataStore.UpdateJobAttributes(ctx, jobUUID, jobAttributes)
}

func (s *PersistenceStore) DeleteJob(ctx context.Context, id string, errorDetails string) error {
	return s.dataStore.DeleteJob(ctx, id, errorDetails)
}

func (s *PersistenceStore) GetJob(ctx context.Context, id string) (*datamodel.Job, error) {
	return s.dataStore.GetJob(ctx, id)
}

func (s *PersistenceStore) GetJobsWithCondition(ctx context.Context, filter dbutils.Filter) ([]*datamodel.Job, error) {
	return s.dataStore.GetJobsWithCondition(ctx, filter)
}

func (s *PersistenceStore) GetPoolByVendorID(ctx context.Context, vendorID string, accountID int64) (*datamodel.PoolView, error) {
	return s.dataStore.GetPoolByVendorID(ctx, vendorID, accountID)
}

func (s *PersistenceStore) GetOngoingMigrateKmsConfigJob(ctx context.Context, accountId int64) (*datamodel.Job, error) {
	return s.dataStore.GetOngoingMigrateKmsConfigJob(ctx, accountId)
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

func (s *PersistenceStore) GetNextSVMIndexByPoolID(ctx context.Context, poolID int64) (int64, error) {
	return s.dataStore.GetNextSVMIndexByPoolID(ctx, poolID)
}

func (s *PersistenceStore) UpdateSvmWithKmsConfigIDs(ctx context.Context, svm *datamodel.Svm, gcpKmsConfigUUID, externalGcpKmsConfigUUID string) (*datamodel.Svm, error) {
	return s.dataStore.UpdateSvmWithKmsConfigIDs(ctx, svm, gcpKmsConfigUUID, externalGcpKmsConfigUUID)
}

func (s *PersistenceStore) CreateLif(ctx context.Context, lif *datamodel.Lif) (*datamodel.Lif, error) {
	return s.dataStore.CreateLif(ctx, lif)
}

func (s *PersistenceStore) GetLifByNodeID(ctx context.Context, nodeID int64, accountID int64) (*datamodel.Lif, error) {
	return s.dataStore.GetLifByNodeID(ctx, nodeID, 0)
}

func (s *PersistenceStore) GetLifsForNodesWithProtocol(ctx context.Context, nodeIDs []int64, accountID int64, protocol string) ([]*datamodel.Lif, error) {
	return s.dataStore.GetLifsForNodesWithProtocol(ctx, nodeIDs, accountID, protocol)
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

func (s *PersistenceStore) ErroredNode(ctx context.Context, node *datamodel.Node, errMsg string) error {
	return s.dataStore.ErroredNode(ctx, node, errMsg)
}

func (s *PersistenceStore) ErroredSVM(ctx context.Context, svm *datamodel.Svm, errMsg string) error {
	return s.dataStore.ErroredSVM(ctx, svm, errMsg)
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

func (s *PersistenceStore) UpdateSnapshotForHandleResource(ctx context.Context, snapshot *datamodel.Snapshot) (*datamodel.Snapshot, error) {
	return s.dataStore.UpdateSnapshotForHandleResource(ctx, snapshot)
}

func (s *PersistenceStore) GetSnapshotByUUID(ctx context.Context, uuid string, accountID int64, volumeID int64) (*datamodel.Snapshot, error) {
	return s.dataStore.GetSnapshotByUUID(ctx, uuid, accountID, volumeID)
}

func (s *PersistenceStore) GetSnapshotByNameAndVolumeId(ctx context.Context, snapshotName string, accountID int64, volumeID int64) (*datamodel.Snapshot, error) {
	return s.dataStore.GetSnapshotByNameAndVolumeId(ctx, snapshotName, accountID, volumeID)
}

func (s *PersistenceStore) GetSnapshotByPoolID(ctx context.Context, uuid string, accountID int64, poolID int64, isParentSnapshot bool) (*datamodel.Snapshot, error) {
	return s.dataStore.GetSnapshotByPoolID(ctx, uuid, accountID, poolID, isParentSnapshot)
}

func (s *PersistenceStore) GetSnapshotsWithCondition(ctx context.Context, filter dbutils.Filter) ([]*datamodel.Snapshot, error) {
	return s.dataStore.GetSnapshotsWithCondition(ctx, filter)
}

func (s *PersistenceStore) GetWronglyDeletedSnapshot(ctx context.Context, snapshotExternalUUID string) (*datamodel.Snapshot, error) {
	return s.dataStore.GetWronglyDeletedSnapshot(ctx, snapshotExternalUUID)
}

func (s *PersistenceStore) UnDeleteSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error {
	return s.dataStore.UnDeleteSnapshot(ctx, snapshot)
}

func (s *PersistenceStore) DeletingSnapshot(ctx context.Context, snapshot *datamodel.Snapshot) error {
	return s.dataStore.DeletingSnapshot(ctx, snapshot)
}

func (s *PersistenceStore) DeleteSnapshot(ctx context.Context, id string) (*datamodel.Snapshot, error) {
	return s.dataStore.DeleteSnapshot(ctx, id)
}

func (s *PersistenceStore) BatchDeleteSnapshots(ctx context.Context, snapshotIDs []int64) ([]*datamodel.Snapshot, error) {
	return s.dataStore.BatchDeleteSnapshots(ctx, snapshotIDs)
}

func (s *PersistenceStore) BatchCreateSnapshots(ctx context.Context, newSnapshots []*datamodel.Snapshot, returnCreatedSnapshotUUIDs bool) ([]string, error) {
	return s.dataStore.BatchCreateSnapshots(ctx, newSnapshots, returnCreatedSnapshotUUIDs)
}

func (s *PersistenceStore) BatchUpdateSnapshots(ctx context.Context, snapshots []*datamodel.Snapshot) error {
	return s.dataStore.BatchUpdateSnapshots(ctx, snapshots)
}

func (s *PersistenceStore) BatchUnDeleteSnapshots(ctx context.Context, snapshots []*datamodel.Snapshot) error {
	return s.dataStore.BatchUnDeleteSnapshots(ctx, snapshots)
}

func (s *PersistenceStore) BatchGetSnapshotsByUUIDs(ctx context.Context, snapshotUUIDs []string) ([]*datamodel.Snapshot, error) {
	return s.dataStore.BatchGetSnapshotsByUUIDs(ctx, snapshotUUIDs)
}

func (s *PersistenceStore) BatchGetWronglyDeletedSnapshots(ctx context.Context, snapshotExternalUUIDs []string) ([]*datamodel.Snapshot, error) {
	return s.dataStore.BatchGetWronglyDeletedSnapshots(ctx, snapshotExternalUUIDs)
}

func (s *PersistenceStore) GetAppConsistentSnapshotsForVolume(ctx context.Context, accountID, volumeID int64) ([]*datamodel.Snapshot, error) {
	return s.dataStore.GetAppConsistentSnapshotsForVolume(ctx, accountID, volumeID)
}

func (s *PersistenceStore) GetSnapshotsByVolumeID(ctx context.Context, volumeID int64) ([]*datamodel.Snapshot, error) {
	return s.dataStore.GetSnapshotsByVolumeID(ctx, volumeID)
}

func (s *PersistenceStore) GetReplicationSnapshotsByVolumeID(ctx context.Context, volumeID int64) ([]*datamodel.Snapshot, error) {
	return s.dataStore.GetReplicationSnapshotsByVolumeID(ctx, volumeID)
}

func (s *PersistenceStore) GetSnapshotsByVolumeIDs(ctx context.Context, volumeIDs []int64) ([]*datamodel.Snapshot, error) {
	return s.dataStore.GetSnapshotsByVolumeIDs(ctx, volumeIDs)
}

func (s *PersistenceStore) GetSnapshotsByTypeAndVolumeID(ctx context.Context, snapshotType string, volumeID int64) ([]*datamodel.Snapshot, error) {
	return s.dataStore.GetSnapshotsByTypeAndVolumeID(ctx, snapshotType, volumeID)
}

func (s *PersistenceStore) GetMultipleKmsConfigs(ctx context.Context, conditions [][]interface{}) ([]*datamodel.KmsConfig, error) {
	return s.dataStore.GetMultipleKmsConfigs(ctx, conditions)
}

func (s *PersistenceStore) GetKmsConfig(ctx context.Context, kmsConfigUUID string) (*datamodel.KmsConfig, error) {
	return s.dataStore.GetKmsConfig(ctx, kmsConfigUUID)
}

func (s *PersistenceStore) UpdateKmsConfigState(ctx context.Context, kmsConfigUUID string, state string, stateDetails string) (*datamodel.KmsConfig, error) {
	return s.dataStore.UpdateKmsConfigState(ctx, kmsConfigUUID, state, stateDetails)
}

func (s *PersistenceStore) ListKmsConfigByAccountID(ctx context.Context, accountID int64) ([]*datamodel.KmsConfig, error) {
	return s.dataStore.ListKmsConfigByAccountID(ctx, accountID)
}

func (s *PersistenceStore) GetSvmsByKmsConfigID(ctx context.Context, kmsConfigID int64) ([]*datamodel.Svm, error) {
	return s.dataStore.GetSvmsByKmsConfigID(ctx, kmsConfigID)
}

func (s *PersistenceStore) CreateKmsConfig(ctx context.Context, kmsConfigParams *datamodel.KmsConfig) (*datamodel.KmsConfig, error) {
	return s.dataStore.CreateKmsConfig(ctx, kmsConfigParams)
}

func (s *PersistenceStore) DeleteKmsConfig(ctx context.Context, kmsConfigUUID, state, stateDetails string) (*datamodel.KmsConfig, error) {
	return s.dataStore.DeleteKmsConfig(ctx, kmsConfigUUID, state, stateDetails)
}

func (s *PersistenceStore) GetKmsConfigByUUID(ctx context.Context, uuid string) (*datamodel.KmsConfig, error) {
	return s.dataStore.GetKmsConfigByUUID(ctx, uuid)
}

func (s *PersistenceStore) UpdateKmsConfigAttributes(ctx context.Context, uuid string, attributes *datamodel.KmsAttributes) (*datamodel.KmsConfig, error) {
	return s.dataStore.UpdateKmsConfigAttributes(ctx, uuid, attributes)
}

func (s *PersistenceStore) GetJobByResourceUUID(ctx context.Context, resourceUUID string, jobType string) (*datamodel.Job, error) {
	return s.dataStore.GetJobByResourceUUID(ctx, resourceUUID, jobType)
}

func (s *PersistenceStore) ListOngoingPoolJobsWithKmsConfigId(ctx context.Context, kmsId, accountId int64) ([]*datamodel.Job, error) {
	return s.dataStore.ListOngoingPoolJobsWithKmsConfigId(ctx, kmsId, accountId)
}

func (s *PersistenceStore) UpdateKmsConfigStateForHandleResource(ctx context.Context, kmsConfigUUID string, stateDetails string, event string) (*datamodel.KmsConfig, error) {
	return s.dataStore.UpdateKmsConfigStateForHandleResource(ctx, kmsConfigUUID, stateDetails, event)
}

func (s *PersistenceStore) UpdateKmsConfigDetails(ctx context.Context, uuid string, keyFullPath string, resourceID string) (*datamodel.KmsConfig, error) {
	return s.dataStore.UpdateKmsConfigDetails(ctx, uuid, keyFullPath, resourceID)
}

func (s *PersistenceStore) GetKmsConfigByKeyFullPath(ctx context.Context, keyFullPath string, accountID int64) (*datamodel.KmsConfig, error) {
	return s.dataStore.GetKmsConfigByKeyFullPath(ctx, keyFullPath, accountID)
}

func (s *PersistenceStore) CreateKmsServiceAccount(ctx context.Context, serviceAccount *datamodel.ServiceAccount) (*datamodel.ServiceAccount, error) {
	return s.dataStore.CreateKmsServiceAccount(ctx, serviceAccount)
}

func (s *PersistenceStore) UpdateServiceAccountEmailAndKey(ctx context.Context, uuid string, email string, key string) (*datamodel.ServiceAccount, error) {
	return s.dataStore.UpdateServiceAccountEmailAndKey(ctx, uuid, email, key)
}

func (s *PersistenceStore) UpdateServiceAccountState(ctx context.Context, uuid string, state string, stateDetails string) (*datamodel.ServiceAccount, error) {
	return s.dataStore.UpdateServiceAccountState(ctx, uuid, state, stateDetails)
}

func (s *PersistenceStore) GetServiceAccountFromEmail(ctx context.Context, email string) (*datamodel.ServiceAccount, error) {
	return s.dataStore.GetServiceAccountFromEmail(ctx, email)
}

func (s *PersistenceStore) ListKmsServiceAccounts(ctx context.Context, filter *dbutils.Filter) ([]*datamodel.ServiceAccount, error) {
	return s.dataStore.ListKmsServiceAccounts(ctx, filter)
}

func (s *PersistenceStore) GetBackupVaultByNameAndOwnerID(ctx context.Context, backupVaultId string, account_id string) (*datamodel.BackupVault, error) {
	return s.dataStore.GetBackupVaultByNameAndOwnerID(ctx, backupVaultId, account_id)
}

func (s *PersistenceStore) CreatingBackupVault(ctx context.Context, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	return s.dataStore.CreatingBackupVault(ctx, bv)
}

func (s *PersistenceStore) ListBackupVaults(ctx context.Context, accountID int64) ([]*datamodel.BackupVault, error) {
	return s.dataStore.ListBackupVaults(ctx, accountID)
}

func (s *PersistenceStore) UpdateBackupVaultInVCP(ctx context.Context, backupVault *datamodel.BackupVault, vcpVault *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	return s.dataStore.UpdateBackupVaultInVCP(ctx, backupVault, vcpVault)
}

func (s *PersistenceStore) DeleteBackupVaultInVCP(ctx context.Context, backupVaultId string) (*datamodel.BackupVault, error) {
	return s.dataStore.DeleteBackupVaultInVCP(ctx, backupVaultId)
}

func (s *PersistenceStore) UpdateBackupVaultState(ctx context.Context, bv *datamodel.BackupVault, state, stateDetails string) (*datamodel.BackupVault, error) {
	return s.dataStore.UpdateBackupVaultState(ctx, bv, state, stateDetails)
}

func (s *PersistenceStore) CreateBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	return s.dataStore.CreateBackup(ctx, backup)
}

func (s *PersistenceStore) DeleteBackup(ctx context.Context, backupUUID string) (*datamodel.Backup, error) {
	return s.dataStore.DeleteBackup(ctx, backupUUID)
}

func (s *PersistenceStore) UpdateBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	return s.dataStore.UpdateBackup(ctx, backup)
}

func (s *PersistenceStore) FinishBackup(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	return s.dataStore.FinishBackup(ctx, backup)
}

func (s *PersistenceStore) UpdateBackupState(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	return s.dataStore.UpdateBackupState(ctx, backup)
}

func (s *PersistenceStore) GetBackupVault(ctx context.Context, backupVaultId string) (*datamodel.BackupVault, error) {
	return s.dataStore.GetBackupVault(ctx, backupVaultId)
}

func (s *PersistenceStore) IsBackupInCreatingorDeletingStateByVolume(ctx context.Context, volumeUUID string) (bool, error) {
	return s.dataStore.IsBackupInCreatingorDeletingStateByVolume(ctx, volumeUUID)
}

func (s *PersistenceStore) GetBackupsByBackupVaultOwnerIDAndFilter(ctx context.Context, backupVaultUUID string, accountID int64, filters [][]interface{}) ([]*datamodel.Backup, error) {
	return s.dataStore.GetBackupsByBackupVaultOwnerIDAndFilter(ctx, backupVaultUUID, accountID, filters)
}

func (s *PersistenceStore) GetBackupCountByBackupVaultID(ctx context.Context, backupVaultID int64) (int64, error) {
	return s.dataStore.GetBackupCountByBackupVaultID(ctx, backupVaultID)
}

func (s *PersistenceStore) GetVolumeCountByBackupVaultID(ctx context.Context, backupVaultUUID string) (int64, error) {
	return s.dataStore.GetVolumeCountByBackupVaultID(ctx, backupVaultUUID)
}

func (s *PersistenceStore) GetBackup(ctx context.Context, backupVaultUUID string, backupUUID string, accountName string) (*datamodel.Backup, error) {
	return s.dataStore.GetBackup(ctx, backupVaultUUID, backupUUID, accountName)
}

func (s *PersistenceStore) GetBackupVaultByUUIDndOwnerID(ctx context.Context, backupVaultUUID string, accountID int64) (*datamodel.BackupVault, error) {
	return s.dataStore.GetBackupVaultByUUIDndOwnerID(ctx, backupVaultUUID, accountID)
}

func (s *PersistenceStore) CreateBackupVaultEntryInVCP(ctx context.Context, backupVault *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	return s.dataStore.CreateBackupVaultEntryInVCP(ctx, backupVault)
}

func (s *PersistenceStore) UpdateBackupVault(ctx context.Context, backupVault *datamodel.BackupVault) error {
	return s.dataStore.UpdateBackupVault(ctx, backupVault)
}

func (s *PersistenceStore) IsLatestBackup(ctx context.Context, backupUUID, volumeUUID string) (bool, error) {
	return s.dataStore.IsLatestBackup(ctx, backupUUID, volumeUUID)
}

func (s *PersistenceStore) IsLatestBackupAnyState(ctx context.Context, backupUUID, volumeUUID string) (bool, error) {
	return s.dataStore.IsLatestBackupAnyState(ctx, backupUUID, volumeUUID)
}

func (s *PersistenceStore) BackupCountByVolumeID(ctx context.Context, volumeUUID string) (int64, error) {
	return s.dataStore.BackupCountByVolumeID(ctx, volumeUUID)
}

func (s *PersistenceStore) CreateAdminJobSpec(ctx context.Context, spec *datamodel.AdminJobSpec) (*datamodel.AdminJobSpec, error) {
	return s.dataStore.CreateAdminJobSpec(ctx, spec)
}

func (s *PersistenceStore) GetAdminJobSpecByJobType(ctx context.Context, jobType string) (*datamodel.AdminJobSpec, error) {
	return s.dataStore.GetAdminJobSpecByJobType(ctx, jobType)
}

func (s *PersistenceStore) UpdateAdminJobSpec(ctx context.Context, jobSpec *datamodel.AdminJobSpec) error {
	return s.dataStore.UpdateAdminJobSpec(ctx, jobSpec)
}

func (s *PersistenceStore) GetAdminJobSpecsByState(ctx context.Context, state string) ([]*datamodel.AdminJobSpec, error) {
	return s.dataStore.GetAdminJobSpecsByState(ctx, state)
}

func (s *PersistenceStore) ErroredResource(ctx context.Context, resource interface{}, errMessage string) (interface{}, error) {
	return s.dataStore.ErroredResource(ctx, resource, errMessage)
}

func (s *PersistenceStore) CreateNodeNodeGroupMap(ctx context.Context, m *datamodel.NodeNodeGroupMap) (*datamodel.NodeNodeGroupMap, error) {
	return s.dataStore.CreateNodeNodeGroupMap(ctx, m)
}

func (s *PersistenceStore) GetNodeNodeGroupMap(ctx context.Context, id int64) (*datamodel.NodeNodeGroupMap, error) {
	return s.dataStore.GetNodeNodeGroupMap(ctx, id)
}

func (s *PersistenceStore) UpdateNodeNodeGroupMap(ctx context.Context, m *datamodel.NodeNodeGroupMap) (*datamodel.NodeNodeGroupMap, error) {
	return s.dataStore.UpdateNodeNodeGroupMap(ctx, m)
}

func (s *PersistenceStore) AssignTwoNodesToTwoGroups(ctx context.Context, params datamodel.NodeGroupAssignmentParams) ([]*datamodel.NodeNodeGroupMap, error) {
	return s.dataStore.AssignTwoNodesToTwoGroups(ctx, params)
}

func (s *PersistenceStore) DeleteNodeNodeGroupMap(ctx context.Context, id int64) error {
	return s.dataStore.DeleteNodeNodeGroupMap(ctx, id)
}

func (s *PersistenceStore) GetAllVolumesForHG(ctx context.Context, hostGroupUUID string, accountID int64) ([]*datamodel.Volume, error) {
	return s.dataStore.GetAllVolumesForHG(ctx, hostGroupUUID, accountID)
}

func (s *PersistenceStore) UpdateHostGroup(ctx context.Context, hostGroupUUID string, accountID int64, description *string, Hosts *[]string) (*datamodel.HostGroup, error) {
	return s.dataStore.UpdateHostGroup(ctx, hostGroupUUID, accountID, description, Hosts)
}

func (s *PersistenceStore) ListHostGroupsByAccountID(ctx context.Context, accountID int64) ([]*datamodel.HostGroup, error) {
	return s.dataStore.ListHostGroupsByAccountID(ctx, accountID)
}

func (s *PersistenceStore) UpdateHostGroupsStateForHandleResource(ctx context.Context, hostGroupUUID string, accountID int64, state, stateDetails string) error {
	return s.dataStore.UpdateHostGroupsStateForHandleResource(ctx, hostGroupUUID, accountID, state, stateDetails)
}

func (s *PersistenceStore) GetBackupPolicyByUUIDAndOwnerID(ctx context.Context, backupPolicyUUID string, accountID int64) (*datamodel.BackupPolicy, error) {
	return s.dataStore.GetBackupPolicyByUUIDAndOwnerID(ctx, backupPolicyUUID, accountID)
}

func (s *PersistenceStore) GetBackupPolicyByNameAndOwnerID(ctx context.Context, backupPolicyName string, accountID int64) (*datamodel.BackupPolicy, error) {
	return s.dataStore.GetBackupPolicyByNameAndOwnerID(ctx, backupPolicyName, accountID)
}

func (s *PersistenceStore) GetBackupPolicyUUIDsFromBackupVaultUUID(ctx context.Context, backupVaultUUID string, accountID int64) ([]string, error) {
	return s.dataStore.GetBackupPolicyUUIDsFromBackupVaultUUID(ctx, backupVaultUUID, accountID)
}

func (s *PersistenceStore) GetBackupVaultUUIDsFromBackupPolicyUUID(ctx context.Context, backupPolicyUUID string, accountID int64) ([]string, error) {
	return s.dataStore.GetBackupVaultUUIDsFromBackupPolicyUUID(ctx, backupPolicyUUID, accountID)
}

func (s *PersistenceStore) GetVolumeCountByBackupPolicyID(ctx context.Context, backupPolicyUUID string) (int64, error) {
	return s.dataStore.GetVolumeCountByBackupPolicyID(ctx, backupPolicyUUID)
}

func (s *PersistenceStore) ListBackupPolicyVolumeCount(ctx context.Context, conditions [][]interface{}) (map[string]int64, error) {
	return s.dataStore.ListBackupPolicyVolumeCount(ctx, conditions)
}

func (s *PersistenceStore) ListBackupPolicies(ctx context.Context, conditions [][]interface{}) ([]*datamodel.BackupPolicy, error) {
	return s.dataStore.ListBackupPolicies(ctx, conditions)
}

func (s *PersistenceStore) CreateBackupPolicyEntryInVCP(ctx context.Context, backupPolicy *datamodel.BackupPolicy) (*datamodel.BackupPolicy, error) {
	return s.dataStore.CreateBackupPolicyEntryInVCP(ctx, backupPolicy)
}

func (s *PersistenceStore) DeleteBackupPolicy(ctx context.Context, backupPolicyUUID string) (*datamodel.BackupPolicy, error) {
	return s.dataStore.DeleteBackupPolicy(ctx, backupPolicyUUID)
}

func (s *PersistenceStore) FetchScheduledBackupsForDeletion(ctx context.Context, volume *datamodel.Volume, backupPolicy *datamodel.BackupPolicy) ([]*datamodel.Backup, error) {
	return s.dataStore.FetchScheduledBackupsForDeletion(ctx, volume, backupPolicy)
}

func (s *PersistenceStore) IsBackupShared(ctx context.Context, backup *datamodel.Backup) (bool, error) {
	return s.dataStore.IsBackupShared(ctx, backup)
}

func (s *PersistenceStore) GetBackupByNameAndBackupVaultID(ctx context.Context, backupName string, backupVaultID int64) (*datamodel.Backup, error) {
	return s.dataStore.GetBackupByNameAndBackupVaultID(ctx, backupName, backupVaultID)
}

func (s *PersistenceStore) GetMultipleBackupVaults(ctx context.Context, conditions [][]interface{}) ([]*datamodel.BackupVault, error) {
	return s.dataStore.GetMultipleBackupVaults(ctx, conditions)
}

func (s *PersistenceStore) CreateNodeGroup(ctx context.Context, group *datamodel.NodeGroup) (*datamodel.NodeGroup, error) {
	return s.dataStore.CreateNodeGroup(ctx, group)
}

func (s *PersistenceStore) GetNodeGroup(ctx context.Context, id int64) (*datamodel.NodeGroup, error) {
	return s.dataStore.GetNodeGroup(ctx, id)
}

func (s *PersistenceStore) UpdateNodeGroup(ctx context.Context, group *datamodel.NodeGroup) (*datamodel.NodeGroup, error) {
	return s.dataStore.UpdateNodeGroup(ctx, group)
}

func (s *PersistenceStore) DeleteNodeGroup(ctx context.Context, id int64) error {
	return s.dataStore.DeleteNodeGroup(ctx, id)
}

func (s *PersistenceStore) DeleteNodeGroupMap(ctx context.Context, nodeGroupMap *datamodel.NodeNodeGroupMap) error {
	return s.dataStore.DeleteNodeGroupMap(ctx, nodeGroupMap)
}

func (s *PersistenceStore) GetNodeGroupMapNodeCount(ctx context.Context, nodeGroupID int64) (int64, error) {
	return s.dataStore.GetNodeGroupMapNodeCount(ctx, nodeGroupID)
}

func (s *PersistenceStore) GetNodeNodeGroupMapByNodeID(ctx context.Context, nodeID int64) (*datamodel.NodeNodeGroupMap, error) {
	return s.dataStore.GetNodeNodeGroupMapByNodeID(ctx, nodeID)
}

func (s *PersistenceStore) UpdateBackupPolicy(ctx context.Context, uuid string, updates map[string]interface{}) (*datamodel.BackupPolicy, error) {
	return s.dataStore.UpdateBackupPolicy(ctx, uuid, updates)
}

func (s *PersistenceStore) GetBackupCountByVolumeUUIDs(ctx context.Context, volumeUUIDs []string, conditions [][]interface{}) (map[string]int64, error) {
	return s.dataStore.GetBackupCountByVolumeUUIDs(ctx, volumeUUIDs, conditions)
}

func (s *PersistenceStore) GetBackupsByVolumeUUID(ctx context.Context, volumeUUID string) ([]*datamodel.Backup, error) {
	return s.dataStore.GetBackupsByVolumeUUID(ctx, volumeUUID)
}

func (s *PersistenceStore) UpdateBackupLatestLogicalBackupSizeByVolume(ctx context.Context, volumeUUID, excludeBackupUUID string) error {
	return s.dataStore.UpdateBackupLatestLogicalBackupSizeByVolume(ctx, volumeUUID, excludeBackupUUID)
}

func (s *PersistenceStore) UpdateLatestBackupLogicalSize(ctx context.Context, volumeUUID string, newLogicalSize int64) error {
	return s.dataStore.UpdateLatestBackupLogicalSize(ctx, volumeUUID, newLogicalSize)
}

func (s *PersistenceStore) GetNextSerialNumberInRegion(ctx context.Context, prefix string) (string, error) {
	return s.dataStore.GetNextSerialNumberInRegion(ctx, prefix)
}

func (s *PersistenceStore) ListTpProjects(ctx context.Context) ([]string, error) {
	return s.dataStore.ListTpProjects(ctx)
}

func (s *PersistenceStore) GetSoftDeleteAccount(ctx context.Context, name string) (*datamodel.Account, error) {
	return s.dataStore.GetSoftDeleteAccount(ctx, name)
}

func (s *PersistenceStore) GetDeletedAccounts(ctx context.Context) ([]*datamodel.Account, error) {
	return s.dataStore.GetDeletedAccounts(ctx)
}

func (s *PersistenceStore) HardDeleteResourceByTable(ctx context.Context, table string, query string, id int64) error {
	return s.dataStore.HardDeleteResourceByTable(ctx, table, query, id)
}

func (s *PersistenceStore) DeleteAccount(ctx context.Context, accountID int64) error {
	return s.dataStore.DeleteAccount(ctx, accountID)
}

func (s *PersistenceStore) ListSvmsWithAccountId(ctx context.Context, accountId int64) ([]*datamodel.Svm, error) {
	return s.dataStore.ListSvmsWithAccountId(ctx, accountId)
}

func (s *PersistenceStore) RollBackDeletedAccount(ctx context.Context, accountID int64) error {
	return s.dataStore.RollBackDeletedAccount(ctx, accountID)
}

func (s *PersistenceStore) GetBackupLogicalSizeMetrics(ctx context.Context) ([]*datamodel.Backup, error) {
	return s.dataStore.GetBackupLogicalSizeMetrics(ctx)
}

func (s *PersistenceStore) ListVolumesWithAccounts(ctx context.Context) ([]*datamodel.Volume, error) {
	return s.dataStore.ListVolumesWithAccounts(ctx)
}

func (s *PersistenceStore) UpdateBackupFields(ctx context.Context, backupUUID string, updates map[string]interface{}) error {
	return s.dataStore.UpdateBackupFields(ctx, backupUUID, updates)
}

func (s *PersistenceStore) GetLatestBackupsGroupedByVolumeUUID(ctx context.Context) ([]datamodel.Backup, error) {
	return s.dataStore.GetLatestBackupsGroupedByVolumeUUID(ctx)
}

func (s *PersistenceStore) GetAccounts(ctx context.Context, includeDelete bool, pagination *dbutils.Pagination) ([]*datamodel.Account, error) {
	return s.dataStore.GetAccounts(ctx, includeDelete, pagination)
}

func (s *PersistenceStore) DeleteServiceAccount(ctx context.Context, serviceAccount *datamodel.ServiceAccount) error {
	return s.dataStore.DeleteServiceAccount(ctx, serviceAccount)
}
