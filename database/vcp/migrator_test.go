package database

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestNewMigrator_SupportedTypes(t *testing.T) {
	logger := &log.MockLogger{}
	configs := []dbutils.DbConfig{
		{Type: dbutils.SQLite},
		{Type: dbutils.Postgres},
	}
	for _, cfg := range configs {
		migrator, err := NewMigrator(cfg, logger)
		assert.NoError(t, err)
		assert.NotNil(t, migrator)
	}
}

func TestNewMigrator_UnsupportedType(t *testing.T) {
	logger := &log.MockLogger{}
	cfg := dbutils.DbConfig{Type: "unknown"}
	migrator, err := NewMigrator(cfg, logger)
	assert.Error(t, err)
	assert.Nil(t, migrator)
}

func TestPersistenceStore_Migrate_Success(t *testing.T) {
	mockMigrator := new(MockMigratorInterface)
	store := &PersistenceStore{
		config: dbutils.DbConfig{Type: "mock"},
		logger: &log.MockLogger{},
		db:     &gormwrapper.Wrapper{},
	}
	origNewMigrator := NewMigrator
	NewMigrator = func(config dbutils.DbConfig, logger log.Logger) (MigratorInterface, error) {
		return mockMigrator, nil
	}
	defer func() { NewMigrator = origNewMigrator }()

	mockMigrator.On("Migrate", store.db, mock.Anything).Return(nil)
	mockMigrator.On("CreateOrUpdateViews", store.db).Return(nil)
	err := store.Migrate(context.Background())
	assert.NoError(t, err)
	mockMigrator.AssertExpectations(t)
}

func TestPersistenceStore_Migrate_Error(t *testing.T) {
	mockMigrator := new(MockMigratorInterface)
	store := &PersistenceStore{
		config: dbutils.DbConfig{Type: "mock"},
		logger: &log.MockLogger{},
		db:     &gormwrapper.Wrapper{},
	}
	origNewMigrator := NewMigrator
	NewMigrator = func(config dbutils.DbConfig, logger log.Logger) (MigratorInterface, error) {
		return mockMigrator, nil
	}
	defer func() { NewMigrator = origNewMigrator }()

	mockMigrator.On("Migrate", store.db, mock.Anything).Return(errors.New("migrate error"))
	err := store.Migrate(context.Background())
	assert.Error(t, err)
	mockMigrator.AssertExpectations(t)
}

func TestPersistenceStore_Rollback_Success(t *testing.T) {
	mockMigrator := new(MockMigratorInterface)
	store := &PersistenceStore{
		config: dbutils.DbConfig{Type: "mock"},
		logger: &log.MockLogger{},
		db:     &gormwrapper.Wrapper{},
	}
	origNewMigrator := NewMigrator
	NewMigrator = func(config dbutils.DbConfig, logger log.Logger) (MigratorInterface, error) {
		return mockMigrator, nil
	}
	defer func() { NewMigrator = origNewMigrator }()

	mockMigrator.On("Rollback", store.db, mock.Anything).Return(nil)
	err := store.Rollback(context.Background())
	assert.NoError(t, err)
	mockMigrator.AssertExpectations(t)
}

func TestPersistenceStore_Rollback_Error(t *testing.T) {
	mockMigrator := new(MockMigratorInterface)
	store := &PersistenceStore{
		config: dbutils.DbConfig{Type: "mock"},
		logger: &log.MockLogger{},
		db:     &gormwrapper.Wrapper{},
	}
	origNewMigrator := NewMigrator
	NewMigrator = func(config dbutils.DbConfig, logger log.Logger) (MigratorInterface, error) {
		return mockMigrator, nil
	}
	defer func() { NewMigrator = origNewMigrator }()

	mockMigrator.On("Rollback", store.db, mock.Anything).Return(errors.New("rollback error"))
	err := store.Rollback(context.Background())
	assert.Error(t, err)
	mockMigrator.AssertExpectations(t)
}

func TestPersistenceStore_Migrate_NewMigratorError(t *testing.T) {
	store := &PersistenceStore{
		config: dbutils.DbConfig{Type: "unknown"},
		logger: &log.MockLogger{},
		db:     &gormwrapper.Wrapper{},
	}
	// NewMigrator will return error for unsupported type
	err := store.Migrate(context.Background())
	assert.Error(t, err)
}

func TestPersistenceStore_Rollback_NewMigratorError(t *testing.T) {
	store := &PersistenceStore{
		config: dbutils.DbConfig{Type: "unknown"},
		logger: &log.MockLogger{},
		db:     &gormwrapper.Wrapper{},
	}
	// NewMigrator will return error for unsupported type
	err := store.Rollback(context.Background())
	assert.Error(t, err)
}
