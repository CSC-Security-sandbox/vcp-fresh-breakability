package postgres

import (
	"context"
	"crypto/md5"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/util/middleware/log"
)

//go:embed migrations/core/*.sql
var migrationsFS embed.FS

type Migrator struct {
	db     *gormwrapper.Wrapper
	config database.DbConfig
	logger log.Logger
}

func (s *Storage) Migrate(ctx context.Context) error {
	m := &Migrator{
		db:     s.db,
		config: s.config,
		logger: s.logger,
	}
	return m.Migrate(ctx)
}

func (s *Storage) Rollback(ctx context.Context) error {
	m := &Migrator{
		db:     s.db,
		config: s.config,
		logger: s.logger,
	}
	return m.Rollback(ctx)
}

// getModels returns the list of models to be migrated.
func getModels() []interface{} {

	return []interface{}{
		datamodel.Pool{},
		datamodel.Volume{},
		datamodel.Svm{},
	}
}

func (m *Migrator) Migrate(ctx context.Context) error {
	// Step 1: Run SQL migrations
	sqlMig, err := m.createMigrator()
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}

	if err := m.runSQLMigrations(ctx, sqlMig); err != nil {
		return fmt.Errorf("SQL migrations failed: %w", err)
	}

	// Step 2: Run GORM AutoMigrate
	if err := m.runAutoMigrations(ctx, getModels()); err != nil {
		return fmt.Errorf("AutoMigrate failed: %w", err)
	}

	// Step 3: Run post-migration fixes
	if err := m.postMigrationFixes(ctx); err != nil {
		return fmt.Errorf("post-migration fixes failed: %w", err)
	}

	return nil
}

func (m *Migrator) createMigrator() (*migrate.Migrate, error) {
	sqlDB, err := m.db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	driver, err := postgres.WithInstance(sqlDB, &postgres.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to create migration driver: %w", err)
	}

	source, err := iofs.New(migrationsFS, m.config.MigrationPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create migration source: %w", err)
	}

	return migrate.NewWithInstance("iofs", source, "postgres", driver)
}

func (m *Migrator) runSQLMigrations(ctx context.Context, mig *migrate.Migrate) error {
	if err := mig.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to run migrations: %w", err)
	}
	return nil
}

func (m *Migrator) runAutoMigrations(ctx context.Context, models []interface{}) error {
	checksum, err := m.calculateChecksum(models)
	if err != nil {
		return fmt.Errorf("calculate checksum failed: %w", err)
	}

	if needs, err := m.needsMigration(checksum); err != nil {
		return err
	} else if !needs {
		m.logger.Info("Models unchanged, skipping AutoMigrate")
		return nil
	}

	m.logger.Info("Running AutoMigrate for model changes")
	if err := m.db.WithContext(ctx).AutoMigrate(models...); err != nil {
		return fmt.Errorf("automigrate failed: %w", err)
	}

	return m.recordMigration(checksum)
}

func (m *Migrator) postMigrationFixes(ctx context.Context) error {
	// Example: Add any foreign keys that GORM might not handle perfectly
	// This is optional and depends on your specific needs
	return nil
}

func (m *Migrator) Rollback(ctx context.Context) error {
	sqlMig, err := m.createMigrator()
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}
	// Check if there are any migrations to rollback
	version, dirty, err := sqlMig.Version()
	if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
		return fmt.Errorf("failed to get current migration version: %w", err)
	}

	if errors.Is(err, migrate.ErrNilVersion) {
		m.logger.Info("No migrations to rollback - database is at initial version")
		return nil
	}

	m.logger.Info(fmt.Sprintf("Current migration version: %d (dirty: %v)", version, dirty))

	// Rollback one step
	if err := sqlMig.Steps(-1); err != nil {
		return fmt.Errorf("failed to rollback migration: %w", err)
	}
	// Get new version after rollback
	newVersion, _, _ := sqlMig.Version()
	m.logger.Info(fmt.Sprintf("Successfully rolled back to version %d", newVersion))
	return nil
}

func (m *Migrator) calculateChecksum(models []interface{}) (string, error) {
	hash := md5.New()
	for _, model := range models {
		t := reflect.TypeOf(model)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		_, err := fmt.Fprintf(hash, "Type:%s\n", t.Name())
		if err != nil {
			return "", err
		}
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			_, err := fmt.Fprintf(hash, "Field:%s:%s\n", field.Name, field.Type)
			if err != nil {
				return "", err
			}
			if tag := field.Tag.Get("gorm"); tag != "" {
				_, err := fmt.Fprintf(hash, "GormTag:%s\n", tag)
				if err != nil {
					return "", err
				}
			}
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (m *Migrator) needsMigration(checksum string) (bool, error) {
	var lastChecksum string
	err := m.db.Raw(`
		SELECT checksum FROM schema_checksums 
		ORDER BY created_at DESC 
		LIMIT 1
	`).Scan(&lastChecksum).Error()

	if err != nil {
		// Table doesn't exist or empty
		return true, nil
	}
	return checksum != lastChecksum, nil
}

func (m *Migrator) recordMigration(checksum string) error {
	return m.db.Exec(`
		INSERT INTO schema_checksums (checksum) 
		VALUES (?)
	`, checksum).Error()
}
