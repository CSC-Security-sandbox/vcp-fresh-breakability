package database

import (
	"context"
	"gorm.io/gorm"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
)

type (
	Storage interface {
		Connect() error
		Close() error
		HealthCheck() error
		WithTransaction(ctx context.Context, fn func(Transaction) error) error
		Migrate(ctx context.Context) error
		Rollback(ctx context.Context) error
		DB() *gorm.DB
		SetupDatabase(ctx context.Context) error

		DataStore
	}

	Transaction interface {
		GORM() *gorm.DB
		Commit() error
		Rollback() error
	}

	DbConfig struct {
		Type              string
		Host              string
		Port              string
		User              string
		Password          string
		Name              string
		SSLMode           string
		TimeZone          string
		MaxOpenConns      int
		MaxIdleConns      int
		ConnMaxLifetime   time.Duration
		ConnectionTimeout int
		AdminUser         string
		AdminPassword     string
		MigrationPath     string
	}
)

// DataStore defines all operations
type DataStore interface {
	CreatePool(ctx context.Context, pool *datamodel.Pool) error
	GetPool(ctx context.Context, id string) (*datamodel.Pool, error)
	UpdatePool(ctx context.Context, pool *datamodel.Pool) error
	DeletePool(ctx context.Context, id string) error
	ListPools(ctx context.Context) ([]*datamodel.Pool, error)

	CreateVolume(ctx context.Context, volume *datamodel.Volume) error
	GetVolume(ctx context.Context, id string) (*datamodel.Volume, error)
	UpdateVolume(ctx context.Context, volume *datamodel.Volume) error
	DeleteVolume(ctx context.Context, id string) error
	ListVolumes(ctx context.Context) ([]*datamodel.Volume, error)
}
