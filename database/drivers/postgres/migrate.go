// Package postgres provides database migration functionality for PostgreSQL.
// It supports a two-phase migration approach:
// 1. Pre-migrations: Run before GORM AutoMigrate (schema changes)
// 2. Post-migrations: Run after GORM AutoMigrate (data changes)
package postgres

import (
	"context"
	"crypto/md5"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type Migrator struct {
	Models       []interface{}
	Logger       log.Logger
	MigrationsFS embed.FS
}

func (m *Migrator) Migrate(db *gormwrapper.Wrapper, ctx context.Context) error {
	// Step 1: Run pre-migration SQL files
	if err := m.runPreMigrations(db, ctx); err != nil {
		return fmt.Errorf("pre-migration SQL files failed: %w", err)
	}

	// Step 2: Run GORM AutoMigrate
	if err := m.runAutoMigrations(db, ctx, m.Models); err != nil {
		return fmt.Errorf("AutoMigrate failed: %w", err)
	}

	// Step 3: Run post-migration SQL files
	if err := m.runPostMigrations(db, ctx); err != nil {
		return fmt.Errorf("post-migration SQL files failed: %w", err)
	}

	// Step 4: Run post-migration fixes
	if err := m.postMigrationFixes(ctx); err != nil {
		return fmt.Errorf("post-migration fixes failed: %w", err)
	}

	return nil
}

func (m *Migrator) runPreMigrations(db *gormwrapper.Wrapper, ctx context.Context) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}

	driver, err := postgres.WithInstance(sqlDB, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	source, err := iofs.New(m.MigrationsFS, "migrations/pre")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	mig, err := migrate.NewWithInstance("iofs", source, "postgres", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}

	if err := mig.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to run pre-migrations: %w", err)
	}

	m.Logger.InfoContext(ctx, "Successfully executed pre-migrations")
	return nil
}

func (m *Migrator) runPostMigrations(db *gormwrapper.Wrapper, ctx context.Context) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}

	// Use a separate schema_migrations table for post-migrations to avoid conflicts
	driver, err := postgres.WithInstance(sqlDB, &postgres.Config{
		MigrationsTable: "schema_migrations_post",
	})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	source, err := iofs.New(m.MigrationsFS, "migrations/post")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	mig, err := migrate.NewWithInstance("iofs", source, "postgres", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}

	if err := mig.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to run post-migrations: %w", err)
	}

	m.Logger.InfoContext(ctx, "Successfully executed post-migrations")
	return nil
}

func (m *Migrator) runAutoMigrations(db *gormwrapper.Wrapper, ctx context.Context, models []interface{}) error {
	checksum, err := m.calculateChecksum(models)
	if err != nil {
		return fmt.Errorf("calculate checksum failed: %w", err)
	}

	if _, err := m.needsMigration(db, checksum); err != nil {
		return err
	} else if false {
		m.Logger.InfoContext(ctx, "Models unchanged, skipping AutoMigrate")
		return nil
	}

	m.Logger.InfoContext(ctx, "Running AutoMigrate for model changes")
	if err := db.WithContext(ctx).AutoMigrate(models...); err != nil {
		return fmt.Errorf("automigrate failed: %w", err)
	}

	return m.recordMigration(db, checksum)
}

func (m *Migrator) postMigrationFixes(ctx context.Context) error {
	// Example: Add any foreign keys that GORM might not handle perfectly
	// This is optional and depends on your specific needs
	return nil
}

func (m *Migrator) Rollback(db *gormwrapper.Wrapper, ctx context.Context) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}

	driver, err := postgres.WithInstance(sqlDB, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	source, err := iofs.New(m.MigrationsFS, "migrations/pre")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	sqlMig, err := migrate.NewWithInstance("iofs", source, "postgres", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}

	// Check if there are any migrations to rollback
	version, dirty, err := sqlMig.Version()
	if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
		return fmt.Errorf("failed to get current migration version: %w", err)
	}

	if errors.Is(err, migrate.ErrNilVersion) {
		m.Logger.InfoContext(ctx, "No migrations to rollback - database is at initial version")
		return nil
	}

	m.Logger.InfoContext(ctx, fmt.Sprintf("Current migration version: %d (dirty: %v)", version, dirty))

	// Rollback one step
	if err := sqlMig.Steps(-1); err != nil {
		return fmt.Errorf("failed to rollback migration: %w", err)
	}
	// Get new version after rollback
	newVersion, _, _ := sqlMig.Version()
	m.Logger.InfoContext(ctx, fmt.Sprintf("Successfully rolled back to version %d", newVersion))
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

func (m *Migrator) needsMigration(db *gormwrapper.Wrapper, checksum string) (bool, error) {
	var lastChecksum string
	err := db.Raw(`
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

func (m *Migrator) recordMigration(db *gormwrapper.Wrapper, checksum string) error {
	return db.Exec(`
		INSERT INTO schema_checksums (checksum) 
		VALUES (?)
	`, checksum).Error()
}

// CreateOrUpdateViews ensures all required views are created or updated after migrations.
func (m *Migrator) CreateOrUpdateViews(db *gormwrapper.Wrapper) error {
	if err := CreateOrUpdatePoolView(db); err != nil {
		return err
	}
	return nil
}

// CreateOrUpdatePoolView ensures the pool_view is always in sync with the pool table schema.
func CreateOrUpdatePoolView(db *gormwrapper.Wrapper) error {
	const viewSQL = `CREATE OR REPLACE VIEW pool_views AS
	SELECT
		p.*,
		coalesce(sum(v.throughput), 0.0) as throughput,
		coalesce(sum(v.size_in_bytes - v.clones_shared_bytes), 0) as quota_in_bytes,
		coalesce(count(v.id) filter (where v.clones_shared_bytes > 0), 0) as clone_volume_count,
		count(v.id) as volume_count
	FROM pools p
		LEFT JOIN volumes v on v.pool_id = p.id
		and v.account_id = p.account_id
		and v.deleted_at is null
	GROUP BY
		p.id,
		p.name;`

	err := db.Exec(viewSQL).Error()
	if err == nil {
		return nil
	}
	// SQLSTATE 42P16: column order/type mismatch, drop and recreate
	if strings.Contains(err.Error(), "42P16") {
		dropErr := db.Exec("DROP VIEW IF EXISTS pool_views;").Error()
		if dropErr != nil {
			return dropErr
		}
		return db.Exec(viewSQL).Error()
	}
	return err
}
