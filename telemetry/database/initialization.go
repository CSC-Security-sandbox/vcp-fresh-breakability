package database

import (
	"context"
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/db"
)

// Database interface defines the methods required for a database connection
type Database interface {
	GetConnection(ctx context.Context, logger log.Logger) (database.Storage, error)
}

// VCPDatabase interface defines the methods required for a VCP database connection
type VCPDatabase interface {
	GetConnection(ctx context.Context, logger log.Logger) (database.Storage, error)
}

// vcpDatabaseImpl implements the VCPDatabase interface
// Use this concrete type where needed
type vcpDataRepository struct{}

func (vcp *vcpDataRepository) GetConnection(ctx context.Context, logger log.Logger) (database.Storage, error) {
	return db.GetDbConnection(ctx, logger)
}

type TelemetryDatabase interface {
	GetConnection(ctx context.Context, logger log.Logger) (database.Storage, error)
}

// telemetryDatabaseImpl implements the TelemetryDatabase interface
// Use this concrete type where needed
type telemetryDatabaseImpl struct{}

func (telemetry *telemetryDatabaseImpl) GetConnection(ctx context.Context, logger log.Logger) (database.Storage, error) {
	return db.GetTelemetryDbConnection(ctx, logger)
}

// Exported types for DI usage
var (
	VCPDatabaseImpl       = vcpDataRepository{}
	TelemetryDatabaseImpl = telemetryDatabaseImpl{}
)

// InitializeDatabase initializes the database connection based on the provided Database type
func InitializeDatabase(ctx context.Context, dbType Database, logger log.Logger) (database.Storage, error) {
	if dbType == nil {
		return nil, errors.New("database type is nil")
	}
	storage, err := dbType.GetConnection(ctx, logger)
	if err != nil {
		return nil, err
	}
	if storage == nil {
		return nil, errors.New("database storage is nil")
	}
	return storage, nil
}
