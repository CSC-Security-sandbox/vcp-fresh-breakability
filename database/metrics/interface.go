package database

import (
	"context"
	"database/sql"

	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormWrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"gorm.io/gorm"
)

type DataStoreRepository struct {
	db *gormWrapper.Wrapper
}

func NewDataStoreRepository(db *gormWrapper.Wrapper) *DataStoreRepository {
	return &DataStoreRepository{db: db}
}

// HydratedMetrics CRUD
func (r *DataStoreRepository) CreateHydratedMetrics(ctx context.Context, m *datamodel.HydratedMetrics) error {
	return r.db.GORM().WithContext(ctx).Create(m).Error
}

func (r *DataStoreRepository) GetHydratedMetrics(ctx context.Context, filter map[string]interface{}) ([]datamodel.HydratedMetrics, error) {
	var result []datamodel.HydratedMetrics
	tx := r.db.GORM().WithContext(ctx)
	if len(filter) > 0 {
		tx = tx.Where(filter)
	}
	err := tx.Find(&result).Error
	return result, err
}

func (r *DataStoreRepository) UpdateHydratedMetrics(ctx context.Context, id string, updates map[string]interface{}) error {
	return r.db.GORM().WithContext(ctx).Model(&datamodel.HydratedMetrics{}).Where("resource_uuid = ?", id).Updates(updates).Error
}

func (r *DataStoreRepository) DeleteHydratedMetrics(ctx context.Context, id string) error {
	return r.db.GORM().WithContext(ctx).Where("resource_uuid = ?", id).Delete(&datamodel.HydratedMetrics{}).Error
}

// AggregatedUsage CRUD
func (r *DataStoreRepository) CreateAggregatedUsage(ctx context.Context, a *datamodel.AggregatedUsage) error {
	return r.db.GORM().WithContext(ctx).Create(a).Error
}

func (r *DataStoreRepository) GetAggregatedUsage(ctx context.Context, filter map[string]interface{}) ([]datamodel.AggregatedUsage, error) {
	var result []datamodel.AggregatedUsage
	tx := r.db.GORM().WithContext(ctx)
	if len(filter) > 0 {
		tx = tx.Where(filter)
	}
	err := tx.Find(&result).Error
	return result, err
}

func (r *DataStoreRepository) UpdateAggregatedUsage(ctx context.Context, id int64, updates map[string]interface{}) error {
	return r.db.GORM().WithContext(ctx).Model(&datamodel.AggregatedUsage{}).Where("id = ?", id).Updates(updates).Error
}

func (r *DataStoreRepository) DeleteAggregatedUsage(ctx context.Context, id int64) error {
	return r.db.GORM().WithContext(ctx).Where("id = ?", id).Delete(&datamodel.AggregatedUsage{}).Error
}

// BillingGcpUsage CRUD
func (r *DataStoreRepository) CreateBillingGcpUsage(ctx context.Context, b *datamodel.BillingGcpUsage) error {
	return r.db.GORM().WithContext(ctx).Create(b).Error
}

func (r *DataStoreRepository) GetBillingGcpUsage(ctx context.Context, filter map[string]interface{}) ([]datamodel.BillingGcpUsage, error) {
	var result []datamodel.BillingGcpUsage
	tx := r.db.GORM().WithContext(ctx)
	if len(filter) > 0 {
		tx = tx.Where(filter)
	}
	err := tx.Find(&result).Error
	return result, err
}

func (r *DataStoreRepository) UpdateBillingGcpUsage(ctx context.Context, id int64, updates map[string]interface{}) error {
	return r.db.GORM().WithContext(ctx).Model(&datamodel.BillingGcpUsage{}).Where("id = ?", id).Updates(updates).Error
}

func (r *DataStoreRepository) DeleteBillingGcpUsage(ctx context.Context, id int64) error {
	return r.db.GORM().WithContext(ctx).Where("id = ?", id).Delete(&datamodel.BillingGcpUsage{}).Error
}

type (
	Storage interface {
		Connect(isAdmin bool) error
		Close() error
		HealthCheck() error
		WithTransaction(ctx context.Context, fn func(dbutils.Transaction) error) error
		Migrate(ctx context.Context) error
		Rollback(ctx context.Context) error
		DB() *gorm.DB
		SQLDB() *sql.DB
		SetupDatabase(ctx context.Context) error

		// Embed DataStore interface
		DataStore
	}

	// DataStore defines all operations
	DataStore interface {
		// HydratedMetrics CRUD
		CreateHydratedMetrics(ctx context.Context, m *datamodel.HydratedMetrics) error
		GetHydratedMetrics(ctx context.Context, filter map[string]interface{}) ([]datamodel.HydratedMetrics, error)
		UpdateHydratedMetrics(ctx context.Context, id string, updates map[string]interface{}) error
		DeleteHydratedMetrics(ctx context.Context, id string) error

		// AggregatedUsage CRUD
		CreateAggregatedUsage(ctx context.Context, a *datamodel.AggregatedUsage) error
		GetAggregatedUsage(ctx context.Context, filter map[string]interface{}) ([]datamodel.AggregatedUsage, error)
		UpdateAggregatedUsage(ctx context.Context, id int64, updates map[string]interface{}) error
		DeleteAggregatedUsage(ctx context.Context, id int64) error

		// BillingGcpUsage CRUD
		CreateBillingGcpUsage(ctx context.Context, b *datamodel.BillingGcpUsage) error
		GetBillingGcpUsage(ctx context.Context, filter map[string]interface{}) ([]datamodel.BillingGcpUsage, error)
		UpdateBillingGcpUsage(ctx context.Context, id int64, updates map[string]interface{}) error
		DeleteBillingGcpUsage(ctx context.Context, id int64) error
	}
)
