package database

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/postgres"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/sqllite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var NewMigrator = newMigrator

type MigratorInterface interface {
	Migrate(db *gormwrapper.Wrapper, ctx context.Context) error
	Rollback(db *gormwrapper.Wrapper, ctx context.Context) error
}

func (s *PersistenceStore) Migrate(ctx context.Context) error {
	migrator, err := NewMigrator(s.config, s.logger)
	if err != nil {
		return err
	}
	return migrator.Migrate(s.db, ctx)
}

func (s *PersistenceStore) Rollback(ctx context.Context) error {
	migrator, err := NewMigrator(s.config, s.logger)
	if err != nil {
		return err
	}
	return migrator.Rollback(s.db, ctx)
}

// getModels returns the list of Models to be migrated.
func getModels() []interface{} {
	return []interface{}{
		datamodel.Pool{},
		datamodel.Volume{},
		datamodel.VolumeReplication{},
		datamodel.Account{},
		datamodel.Node{},
		datamodel.Lif{},
		datamodel.Svm{},
		datamodel.Job{},
		datamodel.Snapshot{},
		datamodel.HostGroup{},
		datamodel.ServiceAccount{},
		datamodel.KmsConfig{},
		datamodel.BackupVault{},
	}
}

// NewMigrator creates a new migrator instance.
func newMigrator(config DbConfig, logger log.Logger) (MigratorInterface, error) {
	switch config.Type {
	case DatabaseTypeSQLite:
		return &sqllite.Migrator{
			Logger: logger,
			Models: getModels(),
		}, nil
	case DatabaseTypePostgres:
		return &postgres.Migrator{
			Logger: logger,
			Models: getModels(),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}
}
