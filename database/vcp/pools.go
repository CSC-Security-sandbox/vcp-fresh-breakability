package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormWrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	uniqueSerialSeqName = "cluster_serial_seq"
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
		if pool.UUID == "" {
			pool.UUID = utils.RandomUUID()
		}
		pool.State = models.LifeCycleStateCreating
		pool.StateDetails = models.LifeCycleStateCreatingDetails
		pool.CreatedAt = time.Now()
		pool.UpdatedAt = pool.CreatedAt
		pool.Account.ID = pool.AccountID
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

// DescribePool retrieves a pool by its UUID
func (d *DataStoreRepository) DescribePool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.PoolView, error) {
	return getPoolWithDetails(d.db.Unscoped().GORM().WithContext(ctx), &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: poolUUID}, AccountID: accountID})
}

// GetPool retrieves a pool by its UUID
func (d *DataStoreRepository) GetPool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.PoolView, error) {
	return getPoolWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: poolUUID}, AccountID: accountID})
}

func (d *DataStoreRepository) UpdatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	dbPoolView, err := getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
	if err != nil {
		return nil, err
	}
	dbPool := ConvertPoolViewToPool(dbPoolView)
	if dbPool.State == models.LifeCycleStateCreating ||
		dbPool.State == models.LifeCycleStateDeleting ||
		dbPool.State == models.LifeCycleStateUpdating {
		return nil, customerrors.NewConflictErr("Pool is already transitioning between states")
	}
	dbPool.State = models.LifeCycleStateUpdating
	dbPool.StateDetails = models.LifeCycleStateUpdatingDetails

	dbPool.SizeInBytes = pool.SizeInBytes
	if !nillable.IsNilOrEmpty(&pool.Description) {
		dbPool.Description = pool.Description
	}

	dbPool.UpdatedAt = time.Now()

	if err = tx.Updates(dbPool).Error; err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	updatedPoolView, err := getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
	if err != nil {
		return nil, err
	}
	return ConvertPoolViewToPool(updatedPoolView), nil
}

func (d *DataStoreRepository) UpdatedPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	pool.UpdatedAt = time.Now()
	pool.State = models.LifeCycleStateREADY
	pool.StateDetails = models.LifeCycleStateAvailableDetails

	// Ensure a WHERE clause by explicitly using the primary key
	if err = tx.Model(&datamodel.Pool{}).
		Where("uuid = ?", pool.UUID).
		Updates(pool).Error; err != nil {
		return nil, err
	}
	updatedPoolView, err := getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
	if err != nil {
		return nil, err
	}
	return ConvertPoolViewToPool(updatedPoolView), nil
}

func (d *DataStoreRepository) UpdatePoolSubnetNames(ctx context.Context, poolUUID, snHostProject string, subnetNames []string) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	dbPoolView, err := getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: poolUUID}})
	if err != nil {
		return err
	}
	dbPool := ConvertPoolViewToPool(dbPoolView)
	if subnetNames != nil {
		dbPool.ClusterDetails.SubnetNames = subnetNames
		dbPool.ClusterDetails.SnHostProject = snHostProject
		dbPool.SnHostProject = snHostProject
	}
	dbPool.UpdatedAt = time.Now()

	if err = tx.Updates(dbPool).Error; err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

func (d *DataStoreRepository) UpdatePoolState(ctx context.Context, pool *datamodel.Pool, state string, stateDetails string) (*datamodel.Pool, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	pool.UpdatedAt = time.Now()
	pool.State = state
	pool.StateDetails = stateDetails

	// Ensure a WHERE clause by explicitly using the primary key
	if err = tx.Model(&datamodel.Pool{}).
		Where("uuid = ?", pool.UUID).
		Updates(pool).Error; err != nil {
		return nil, err
	}
	updatedPoolView, err := getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
	if err != nil {
		return nil, err
	}
	return ConvertPoolViewToPool(updatedPoolView), nil
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

func (d *DataStoreRepository) ListPools(ctx context.Context, filter *utils2.Filter) ([]*datamodel.PoolView, error) {
	if filter != nil {
		if filter.ShouldIncludeDeleted() {
			return listPoolWithDetails(d.db.ApplyFilter(filter.Apply()).Unscoped().GORM().WithContext(ctx))
		}
		return listPoolWithDetails(d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx))
	}
	return listPoolWithDetails(d.db.GORM().WithContext(ctx))
}

func (d *DataStoreRepository) GetPoolByVendorID(ctx context.Context, vendorID string, accountID int64) (*datamodel.PoolView, error) {
	return getPoolWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Pool{VendorID: vendorID, AccountID: accountID})
}

func _getPoolWithDetails(db *gorm.DB, query *datamodel.Pool) (*datamodel.PoolView, error) {
	pool := &datamodel.PoolView{}
	err := db.Preload("Account").Preload("KmsConfig").Preload("KmsConfig.ServiceAccount").Preload("KmsConfig.Account").First(&pool, query).Error
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
	err := db.Preload("Account").Preload("KmsConfig").Find(&pools).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return pools, nil
}

func (d *DataStoreRepository) SavePoolWithVsaDetails(ctx context.Context, pool *datamodel.Pool, cluster *datamodel.ClusterDetails) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	pool.ClusterDetails = *cluster
	err = tx.Updates(pool).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

func (d *DataStoreRepository) GetPoolsByAccountName(ctx context.Context, accountName string) ([]*datamodel.Pool, error) {
	var pools []*datamodel.Pool

	err := d.db.GORM().WithContext(ctx).
		Preload("Account").
		Joins("JOIN accounts ON pools.account_id = accounts.id").
		Where("accounts.name = ?", accountName).
		Find(&pools).
		Error

	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return pools, nil
}

// ConvertPoolViewToPool converts a PoolView to a Pool for use in CRUD operations.
func ConvertPoolViewToPool(view *datamodel.PoolView) *datamodel.Pool {
	if view == nil {
		return nil
	}
	return &datamodel.Pool{
		BaseModel:         view.BaseModel,
		Name:              view.Name,
		Description:       view.Description,
		State:             view.State,
		StateDetails:      view.StateDetails,
		VendorID:          view.VendorID,
		ServiceLevel:      view.ServiceLevel,
		SizeInBytes:       view.SizeInBytes,
		UsedBytes:         view.UsedBytes,
		Network:           view.Network,
		AllowAutoTiering:  view.AllowAutoTiering,
		AccountID:         view.AccountID,
		Account:           view.Account,
		PoolAttributes:    view.PoolAttributes,
		ClusterDetails:    view.ClusterDetails,
		QosType:           view.QosType,
		AutoTieringConfig: view.AutoTieringConfig,
		ServiceAccountId:  view.ServiceAccountId,
		DeploymentName:    view.DeploymentName,
		PoolCredentials:   view.PoolCredentials,
		KmsConfigID:       view.KmsConfigID,
		KmsConfig:         view.KmsConfig,
		SnHostProject:     view.SnHostProject,
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

// UpdatePoolWithKmsConfigID updates the KMS configuration for a pool
func (d *DataStoreRepository) UpdatePoolWithKmsConfigID(ctx context.Context, pool *datamodel.Pool, kmsConfigUUID string) (*datamodel.Pool, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	kmsConfig, err := getKmsConfigByUUID(tx, kmsConfigUUID)
	if err != nil {
		return nil, err
	}

	dbPool := &datamodel.Pool{}
	err = tx.Where("name = ?", pool.Name).Where("account_id = ?", pool.AccountID).First(&dbPool).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	dbPool.KmsConfigID = sql.NullInt64{Int64: kmsConfig.ID, Valid: true}
	err = tx.Updates(dbPool).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	poolWithDetails, err := getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: dbPool.UUID}})
	return &poolWithDetails.Pool, err
}

// GetNextSerialNumberInRegion retrieves the next number from a regional db counter and returns the serial number suffix with the given prefix.
func (d *DataStoreRepository) GetNextSerialNumberInRegion(ctx context.Context, prefix string) (string, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return "", err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	var nextClusterSerialNumber int64

	query := fmt.Sprintf("SELECT nextval('%s')", uniqueSerialSeqName)
	err = tx.Raw(query).Scan(&nextClusterSerialNumber).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// If you get this error, it means the sequence does not exist.
		// In local setup, please run the migration script to create the sequence.
		return "", vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, err)
	} else if err != nil {
		return "", vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	// Example, in case seq returns 1: 935010000000000001
	// Example, in case seq returns 45555: 935010000000045555
	// 935: predefined prefix
	// 01: region code, e.g., 01 for us-central1
	// 00000000001: nextClusterSerialNumber padded to 13 digits
	return fmt.Sprintf("%s%013d", prefix, nextClusterSerialNumber), nil
}
