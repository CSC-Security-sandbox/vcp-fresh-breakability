package database

import (
	"context"
	"embed"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/drivers/postgres"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/drivers/sqllite"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var NewMigrator = newMigrator

//go:embed migrations/pre/*.sql migrations/post/*.sql
var migrationsFS embed.FS

type MigratorInterface interface {
	Migrate(db *gormwrapper.Wrapper, ctx context.Context) error
	Rollback(db *gormwrapper.Wrapper, ctx context.Context) error
	CreateOrUpdateViews(db *gormwrapper.Wrapper) error
}

// NewMigrator creates a new migrator instance.
func newMigrator(config dbutils.DbConfig, logger log.Logger) (MigratorInterface, error) {
	switch config.Type {
	case dbutils.SQLite:
		return &sqllite.Migrator{
			Logger: logger,
			Models: getMetricModels(),
		}, nil
	case dbutils.Postgres:
		return &postgres.Migrator{
			Logger:       logger,
			Models:       getMetricModels(),
			MigrationsFS: migrationsFS,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}
}
