package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormWrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

var (
	getPoolWithDetails  = _getPoolWithDetails
	listPoolWithDetails = _listPoolWithDetails
)

type DataStoreRepository struct {
	db *gormWrapper.Wrapper
}

func NewDataStoreRepository(db *gormWrapper.Wrapper) *DataStoreRepository {
	return &DataStoreRepository{db: db}
}

// CreatedPool converts created pool to available pool
func (d *DataStoreRepository) CreatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	// Fixme: The logger should be fetched from ctx
	defer commitOrRollbackOnError(slogger.NewLogger(), tx, &err)
	err = tx.Where("name = ?", pool.Name).Where("account_id = ?", pool.AccountID).First(&pool).Error
	if err != nil {
		return nil, err
	}
	pool.State = models.LifeCycleStateREADY
	pool.StateDetails = models.LifeCycleStateAvailableDetails
	pool.UpdatedAt = time.Now()
	err = tx.Updates(pool).Error
	if err != nil {
		return nil, err
	}

	dbPool, err := getPoolWithDetails(db, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
	if err != nil {
		return nil, err
	}
	return dbPool, nil
}

// CreatingPool creates a new pool in the database
func (d *DataStoreRepository) CreatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	// Fixme: The logger should be fetched from ctx
	logger := slogger.NewLogger()
	defer commitOrRollbackOnError(logger, tx, &err)

	var dbPool *datamodel.Pool
	err1 := tx.Where("name = ?", pool.Name).Where("account_id = ?", pool.AccountID).First(&dbPool).Error
	if errors.Is(err1, gorm.ErrRecordNotFound) {
		pool.UUID = utils.RandomUUID()
		pool.State = models.LifeCycleStateCreating
		pool.StateDetails = models.LifeCycleStateCreatingDetails
		pool.CreatedAt = time.Now()
		pool.UpdatedAt = pool.CreatedAt
		pool.Account.ID = pool.AccountID
		err = tx.Create(&pool).Error
		if err != nil {
			return nil, err
		}

		dbPool, err = getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
		if err != nil {
			return nil, err
		}
		return dbPool, nil
	} else if err1 != nil {
		logger.Errorf("Error while checking if pool exists: %v", err1)
		return nil, err1
	}
	return nil, customerrors.NewConflictErr("pool already exists")
}

// GetPool retrieves a pool by its UUID
func (d *DataStoreRepository) GetPool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.Pool, error) {
	return getPoolWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: poolUUID}, AccountID: accountID})
}

func (d *DataStoreRepository) UpdatePool(ctx context.Context, pool *datamodel.Pool) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	// Fixme: The logger should be fetched from ctx
	defer commitOrRollbackOnError(slogger.NewLogger(), tx, &err)

	dbPool, err := getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
	if err != nil {
		return err
	}
	dbPool.UpdatedAt = time.Now()
	dbPool.State = pool.State
	dbPool.StateDetails = pool.StateDetails

	if err = tx.Updates(dbPool).Error; err != nil {
		return err
	}
	return nil
}

// DeletePool deletes a pool from the database
func (d *DataStoreRepository) DeletePool(ctx context.Context, pool *datamodel.Pool) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	// Fixme: The logger should be fetched from ctx
	defer commitOrRollbackOnError(slogger.NewLogger(), tx, &err)
	pool.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	pool.State = models.LifeCycleStateDeleted
	pool.StateDetails = models.LifeCycleStateDeletedDetails
	err = tx.Updates(pool).Error
	if err != nil {
		return err
	}
	return nil
}

// DeletingPool updates the pool entry to deleting state
func (d *DataStoreRepository) DeletingPool(ctx context.Context, pool *datamodel.Pool) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	// Fixme: The logger should be fetched from ctx
	defer commitOrRollbackOnError(slogger.NewLogger(), tx, &err)
	pool.State = models.LifeCycleStateDeleting
	pool.StateDetails = models.LifeCycleStateDeletingDetails
	err = tx.Updates(pool).Error
	if err != nil {
		return err
	}
	return nil
}

func (d *DataStoreRepository) ListPools(ctx context.Context, conditions [][]interface{}) ([]*datamodel.Pool, error) {
	return listPoolWithDetails(d.db.ApplyFilter(conditions).GORM().WithContext(ctx))
}

func (d *DataStoreRepository) GetPoolByVendorID(ctx context.Context, vendorID string) (*datamodel.Pool, error) {
	return getPoolWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Pool{VendorID: vendorID})
}

func _getPoolWithDetails(db *gorm.DB, query *datamodel.Pool) (*datamodel.Pool, error) {
	pool := &datamodel.Pool{}
	err := db.Preload("Account").First(&pool, query).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("pool not found", nil)
		}
		return nil, err
	}
	return pool, nil
}

func _listPoolWithDetails(db *gorm.DB) ([]*datamodel.Pool, error) {
	var pools []*datamodel.Pool
	err := db.Preload("Account").Find(&pools).Error
	if err != nil {
		return nil, err
	}
	return pools, nil
}

func (d *DataStoreRepository) SavePoolWithVsaClusterDetails(ctx context.Context, pool *datamodel.Pool, cluster *datamodel.ClusterDetails) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	// Fixme: The logger should be fetched from ctx
	defer commitOrRollbackOnError(slogger.NewLogger(), tx, &err)
	pool.ClusterDetails = *cluster
	err = tx.Model(&pool).Updates(map[string]interface{}{
		"cluster_details": pool.ClusterDetails,
	}).Error
	if err != nil {
		return err
	}
	return nil
}
