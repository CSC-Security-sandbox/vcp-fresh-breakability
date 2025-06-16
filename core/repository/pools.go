package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gormWrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	getPoolWithDetails  = _getPoolWithDetails
	listPoolWithDetails = _listPoolWithDetails
	getPoolByName       = _getPoolByName
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
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	dbPool := &datamodel.Pool{}
	err = tx.Where("name = ?", pool.Name).Where("account_id = ?", pool.AccountID).First(&dbPool).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	dbPool.State = models.LifeCycleStateREADY
	dbPool.StateDetails = models.LifeCycleStateAvailableDetails
	dbPool.UpdatedAt = time.Now()
	err = tx.Updates(dbPool).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
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
	logger := util.GetLogger(ctx)
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
		pool.AutoTierBucketName = fmt.Sprintf("%s-%s", pool.AutoTierBucketName, pool.UUID)
		err = tx.Create(&pool).Error
		if err != nil {
			return nil, err
		}

		dbPoolView, err := getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
		if err != nil {
			return nil, err
		}
		return ConvertPoolViewToPool(dbPoolView), nil
	} else if err1 != nil {
		logger.Errorf("Error while checking if pool exists: %v", err1)
		return nil, err1
	}
	return nil, customerrors.NewConflictErr("pool already exists")
}

// GetPool retrieves a pool by its UUID
func (d *DataStoreRepository) GetPool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.PoolView, error) {
	return getPoolWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: poolUUID}, AccountID: accountID})
}

func (d *DataStoreRepository) UpdatePool(ctx context.Context, pool *datamodel.Pool) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	dbPoolView, err := getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
	if err != nil {
		return err
	}
	dbPool := ConvertPoolViewToPool(dbPoolView)
	dbPool.UpdatedAt = time.Now()
	dbPool.State = pool.State
	dbPool.StateDetails = pool.StateDetails

	if err = tx.Updates(dbPool).Error; err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
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
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	pool.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	pool.State = models.LifeCycleStateDeleted
	pool.StateDetails = models.LifeCycleStateDeletedDetails
	err = tx.Updates(pool).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
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
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	pool.State = models.LifeCycleStateDeleting
	pool.StateDetails = models.LifeCycleStateDeletingDetails
	err = tx.Updates(pool).Error
	if err != nil {
		return err
	}
	return nil
}

func (d *DataStoreRepository) ListPools(ctx context.Context, conditions [][]interface{}) ([]*datamodel.PoolView, error) {
	return listPoolWithDetails(d.db.ApplyFilter(conditions).GORM().WithContext(ctx))
}

func (d *DataStoreRepository) GetPoolByVendorID(ctx context.Context, vendorID string) (*datamodel.PoolView, error) {
	return getPoolWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Pool{VendorID: vendorID})
}

func _getPoolWithDetails(db *gorm.DB, query *datamodel.Pool) (*datamodel.PoolView, error) {
	pool := &datamodel.PoolView{}
	err := db.Preload("Account").First(&pool, query).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.NewNotFoundErr("pool", nil))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return pool, nil
}

func (d *DataStoreRepository) GetPoolByName(ctx context.Context, conditions [][]interface{}) (*datamodel.PoolView, error) {
	return getPoolByName(d.db.ApplyFilter(conditions).GORM().WithContext(ctx))
}

func _getPoolByName(db *gorm.DB) (*datamodel.PoolView, error) {
	pool := &datamodel.PoolView{}
	err := db.Preload("Account").First(&pool).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("pool", nil)
		}
		return nil, err
	}
	return pool, nil
}

func _listPoolWithDetails(db *gorm.DB) ([]*datamodel.PoolView, error) {
	var pools []*datamodel.PoolView
	err := db.Preload("Account").Find(&pools).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return pools, nil
}

func (d *DataStoreRepository) SavePoolWithVsaClusterDetails(ctx context.Context, pool *datamodel.Pool, cluster *datamodel.ClusterDetails) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	pool.ClusterDetails = *cluster
	err = tx.Model(&pool).Updates(map[string]interface{}{
		"cluster_details": pool.ClusterDetails,
	}).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

// ConvertPoolViewToPool converts a PoolView to a Pool for use in CRUD operations.
func ConvertPoolViewToPool(view *datamodel.PoolView) *datamodel.Pool {
	if view == nil {
		return nil
	}
	return &datamodel.Pool{
		BaseModel:               view.BaseModel,
		Name:                    view.Name,
		Description:             view.Description,
		State:                   view.State,
		StateDetails:            view.StateDetails,
		VendorID:                view.VendorID,
		ServiceLevel:            view.ServiceLevel,
		SizeInBytes:             view.SizeInBytes,
		UsedBytes:               view.UsedBytes,
		Network:                 view.Network,
		AllowAutoTiering:        view.AllowAutoTiering,
		HotTierSizeInBytes:      view.HotTierSizeInBytes,
		EnableHotTierAutoResize: view.EnableHotTierAutoResize,
		AccountID:               view.AccountID,
		Account:                 view.Account,
		PoolAttributes:          view.PoolAttributes,
		ClusterDetails:          view.ClusterDetails,
		QosType:                 view.QosType,
		Username:                view.Username,
		Password:                view.Password,
		AutoTierBucketName:      view.AutoTierBucketName,
		ServiceAccountId:        view.ServiceAccountId,
	}
}

// ConvertPoolToPoolView converts a datamodel.Pool to a datamodel.PoolView for use in read operations or when you want to expose enriched fields.
func ConvertPoolToPoolView(pool *datamodel.Pool) *datamodel.PoolView {
	if pool == nil {
		return nil
	}
	return &datamodel.PoolView{
		Pool:         *pool,
		Throughput:   0, // Set to 0 or fill in with actual value if available
		QuotaInBytes: 0, // Set to 0 or fill in with actual value if available
		VolumeCount:  0, // Set to 0 or fill in with actual value if available
	}
}
