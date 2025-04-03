package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/repository"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgconn"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
)

const (
	pgInvalidCatalogName = "3D000" //Database doesn't exist
	pgDuplicateDatabase  = "42P04" // Database already exists
)

type Storage struct {
	config database.DbConfig
	db     *gormwrapper.Wrapper
	mu     sync.RWMutex
	logger database.Logger

	dataStore *repository.DataStoreRepository
}

func init() {
	database.Register("postgres", New)
}

func New(config database.DbConfig) (database.Storage, error) {
	db := &Storage{
		config: config,
		logger: config.Logger,
	}

	// First try to connect with application credentials
	err := db.Connect()
	if err == nil {
		db.initRepositories()
		return db, nil
	}

	// If connection fails, and it's a "database doesn't exist" error, initialize fresh
	//if isDatabaseNotExistError(err) && config.Type == DatabaseTypePostgres {
	if isDatabaseNotExistError(err) {
		db.logger.Info("Database doesn't exist, attempting initialization")

		// Temporarily switch to admin credentials
		originalUser := config.User
		originalPassword := config.Password
		config.User = config.AdminUser
		config.Password = config.AdminPassword

		if err := db.connect(true); err != nil {
			return nil, fmt.Errorf("database initialization failed: %w", err)
		}

		// Create database and user
		if err := db.createDatabaseAndUser(originalUser, originalPassword); err != nil {
			return nil, fmt.Errorf("failed to setup database: %w", err)
		}

		// Restore original credentials
		config.User = originalUser
		config.Password = originalPassword

		// Reinitialize with application credentials
		if err := db.Connect(); err != nil {
			return nil, fmt.Errorf("failed to connect after initialization: %w", err)
		}
		return db, nil
	}

	return nil, fmt.Errorf("database connection failed: %w", err)

}

// initRepository initializes  repository
func (s *Storage) initRepositories() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dataStore = repository.NewDataStoreRepository(s.db.GORM())
}

func (s *Storage) Connect() error {
	return s.connect(false) // Default to application credentials
}

func (d *Storage) connect(isAdmin bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	db, err := d.createConnection(isAdmin)
	if err != nil {
		return fmt.Errorf("failed to create database connection: %w", err)
	}

	d.db = gormwrapper.New(db)
	return nil
}

// createConnection establishes a new database connection
func (d *Storage) createConnection(isAdmin bool) (*gorm.DB, error) {
	dsn, err := d.getDSN(isAdmin)
	if err != nil {
		return nil, fmt.Errorf("failed to get DSN: %w", err)
	}

	gormConfig := &gorm.Config{
		Logger: database.NewGormLogger(d.logger),
		NowFunc: func() time.Time {
			return time.Now().UTC()
		},
		PrepareStmt:            true,
		SkipDefaultTransaction: true,
	}

	var dialector = postgres.Open(dsn)
	//switch d.config.Type {
	//case DatabaseTypePostgres:
	//	dialector = postgres.Open(dsn)
	//case DatabaseTypeSQLite:
	//	dialector = sqlite.Open(dsn)
	//default:
	//	return nil, fmt.Errorf("unsupported database type: %s", d.config.Type)
	//}

	db, err := gorm.Open(dialector, gormConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get SQL DB: %w", err)
	}

	sqlDB.SetMaxOpenConns(d.config.MaxOpenConns)
	sqlDB.SetMaxIdleConns(d.config.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(d.config.ConnMaxLifetime)

	return db, nil
}

// getDSN constructs the database connection string
func (s *Storage) getDSN(isAdmin bool) (string, error) {
	//if d.config.Type == DatabaseTypeSQLite {
	//	return "file::memory:?cache=shared", nil
	//}

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

func (s *Storage) createDatabaseAndUser(appUser, appPassword string) error {
	createDBSQL := fmt.Sprintf(
		`CREATE DATABASE %s WITH OWNER = %s ENCODING = 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8' CONNECTION LIMIT = -1`,
		s.config.Name, s.config.AdminUser)

	if err := s.db.Exec(createDBSQL).Error(); err != nil && !isDatabaseExistsError(err) {
		return fmt.Errorf("create database failed: %w", err)
	}

	// TODO : explore different authentication methods
	createUserSQL := fmt.Sprintf(
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '%s') THEN CREATE USER %s WITH PASSWORD '%s'; END IF; END $$`,
		s.config.User, s.config.User, s.config.Password)

	if err := s.db.Exec(createUserSQL).Error(); err != nil {
		return fmt.Errorf("create user failed: %w", err)
	}

	grantSQL := fmt.Sprintf(
		`GRANT ALL PRIVILEGES ON DATABASE %s TO %s; GRANT ALL PRIVILEGES ON SCHEMA public TO %s;`,
		s.config.Name, s.config.User, s.config.User)

	if err := s.db.Exec(grantSQL).Error(); err != nil {
		return fmt.Errorf("grant privileges failed: %w", err)
	}

	return nil
}

func (s *Storage) Close() error {
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

func (s *Storage) HealthCheck() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.db == nil {
		return fmt.Errorf("database connection is closed")
	}

	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Ping()
}

func (s *Storage) WithTransaction(ctx context.Context, fn func(database.Transaction) error) error {
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

func (s *Storage) DB() *gorm.DB {
	return s.db.GORM()
}

func isDatabaseNotExistError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgInvalidCatalogName
}

func isDatabaseExistsError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgDuplicateDatabase
}

// Implement Storage interface by delegating to repositories

func (s *Storage) CreatePool(ctx context.Context, pool *datamodel.Pool) error {
	return s.dataStore.CreatePool(ctx, pool)
}

func (s *Storage) GetPool(ctx context.Context, id string) (*datamodel.Pool, error) {
	return s.dataStore.GetPool(ctx, id)
}

func (s *Storage) UpdatePool(ctx context.Context, pool *datamodel.Pool) error {
	return s.dataStore.UpdatePool(ctx, pool)
}

func (s *Storage) DeletePool(ctx context.Context, id string) error {
	return s.dataStore.DeletePool(ctx, id)
}

func (s *Storage) ListPools(ctx context.Context) ([]*datamodel.Pool, error) {
	return s.dataStore.ListPools(ctx)
}

func (s *Storage) CreateVolume(ctx context.Context, volume *datamodel.Volume) error {
	return s.dataStore.CreateVolume(ctx, volume)
}

func (s *Storage) GetVolume(ctx context.Context, id string) (*datamodel.Volume, error) {
	return s.dataStore.GetVolume(ctx, id)
}

func (s *Storage) UpdateVolume(ctx context.Context, volume *datamodel.Volume) error {
	return s.dataStore.UpdateVolume(ctx, volume)
}

func (s *Storage) DeleteVolume(ctx context.Context, id string) error {
	return s.dataStore.DeleteVolume(ctx, id)
}

func (s *Storage) ListVolumes(ctx context.Context) ([]*datamodel.Volume, error) {
	return s.dataStore.ListVolumes(ctx)
}
