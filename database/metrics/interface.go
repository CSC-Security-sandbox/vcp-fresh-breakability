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

func (r *DataStoreRepository) CreateHydratedMetricsBatch(ctx context.Context, metrics []datamodel.HydratedMetrics, batchSize int) error {
	if len(metrics) == 0 {
		return nil
	}

	return r.db.GORM().WithContext(ctx).CreateInBatches(metrics, batchSize).Error
}
func (r *DataStoreRepository) GetHydratedMetrics(ctx context.Context, filter map[string]interface{}) ([]datamodel.HydratedMetrics, error) {
	var result []datamodel.HydratedMetrics
	db := r.db.GORM().WithContext(ctx)

	if len(filter) > 0 {
		// Check if we have complex conditions
		if conditions, ok := filter["conditions"]; ok {
			// Process each condition
			if condArr, ok := conditions.([][]interface{}); ok {
				for _, condition := range condArr {
					if len(condition) > 0 {
						// Apply each condition to the query
						db = db.Where(condition[0], condition[1:]...)
					}
				}
			}
			// Remove the conditions key from filter
			delete(filter, "conditions")
		}

		// Apply any remaining simple filters
		if len(filter) > 0 {
			db = db.Where(filter)
		}
	}
	err := db.Find(&result).Error
	return result, err
}

func (r *DataStoreRepository) UpdateHydratedMetrics(ctx context.Context, id string, updates map[string]interface{}) error {
	return r.db.GORM().WithContext(ctx).Model(&datamodel.HydratedMetrics{}).Where("id = ?", id).Updates(updates).Error
}

func (r *DataStoreRepository) DeleteHydratedMetrics(ctx context.Context, id string) error {
	return r.db.GORM().WithContext(ctx).Where("id = ?", id).Delete(&datamodel.HydratedMetrics{}).Error
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
		CreateHydratedMetricsBatch(ctx context.Context, metrics []datamodel.HydratedMetrics, batchSize int) error
		GetHydratedMetrics(ctx context.Context, filter map[string]interface{}) ([]datamodel.HydratedMetrics, error)
		UpdateHydratedMetrics(ctx context.Context, id string, updates map[string]interface{}) error
		DeleteHydratedMetrics(ctx context.Context, id string) error

		// AggregatedUsage CRUD
		CreateAggregatedUsage(ctx context.Context, a *datamodel.AggregatedUsage) error
		GetAggregatedUsage(ctx context.Context, filter map[string]interface{}) ([]datamodel.AggregatedUsage, error)
		UpdateAggregatedUsage(ctx context.Context, id int64, updates map[string]interface{}) error
		DeleteAggregatedUsage(ctx context.Context, id int64) error

		// BizOps
		AggregateUsageForBizOps(ctx context.Context, bizopsAggrParams *datamodel.BizOpsAggregateParams) error
	}
)
