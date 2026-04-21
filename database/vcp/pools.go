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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	uniqueSerialSeqName           = "cluster_serial_seq"
	getPoolWithDetails            = _getPoolWithDetails
	listPoolWithDetails           = _listPoolWithDetails
	listPoolWithDetailsPagination = _listPoolWithDetailsPagination
	getPoolByName                 = _getPoolByName
	getPoolsByKmsConfigID         = _getPoolsByKmsConfigID
)

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
	err = tx.Where("vendor_id = ?", pool.VendorID).Where("account_id = ?", pool.AccountID).First(&dbPool).Error
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
	err1 := tx.Where("vendor_id = ?", pool.VendorID).Where("account_id = ?", pool.AccountID).First(&dbPool).Error
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
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
		}

		dbPoolView, err := getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
		if err != nil {
			return nil, err
		}
		return ConvertPoolViewToPool(dbPoolView), nil
	} else if err1 != nil {
		logger.Errorf("Error while checking if pool exists: %v", err1)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err1)
	}
	return nil, vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, customerrors.NewConflictErr("pool already exists"))
}

// DescribePool retrieves a pool by its UUID
func (d *DataStoreRepository) DescribePool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.PoolView, error) {
	return getPoolWithDetails(d.db.Unscoped().GORM().WithContext(ctx), &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: poolUUID}, AccountID: accountID})
}

// GetPool retrieves a pool by its UUID
func (d *DataStoreRepository) GetPool(ctx context.Context, poolUUID string, accountID int64) (*datamodel.PoolView, error) {
	return getPoolWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: poolUUID}, AccountID: accountID})
}

func (d *DataStoreRepository) GetPoolByUUID(ctx context.Context, poolUUID string) (*datamodel.Pool, error) {
	var pool datamodel.Pool
	err := d.db.GORM().WithContext(ctx).
		Model(&datamodel.Pool{}).
		Where("uuid = ?", poolUUID).
		Limit(1).
		Find(&pool).Error
	if err != nil {
		return nil, err
	}
	if pool.UUID == "" {
		return nil, customerrors.NewNotFoundErr("Pool", &poolUUID)
	}
	return &pool, nil
}

func (d *DataStoreRepository) GetPoolStateByUUID(ctx context.Context, poolUUID string) (string, error) {
	var result struct {
		State string `gorm:"column:state"`
	}
	err := d.db.GORM().WithContext(ctx).
		Model(&datamodel.Pool{}).
		Select("state").
		Where("uuid = ? AND deleted_at IS NULL", poolUUID).
		First(&result).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", customerrors.NewNotFoundErr("Pool", &poolUUID)
		}
		return "", err
	}
	return result.State, nil
}

func (d *DataStoreRepository) GetPoolByID(ctx context.Context, poolID int64) (*datamodel.Pool, error) {
	var pool datamodel.Pool
	err := d.db.GORM().WithContext(ctx).Where("id = ?", poolID).First(&pool).Error
	if err != nil {
		poolIDStr := fmt.Sprintf("%d", poolID)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("Pool", &poolIDStr)
		}
		return nil, err
	}
	return &pool, nil
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

	if pool.ActiveDirectoryID.Valid && pool.ActiveDirectoryID.Int64 > 0 {
		dbPool.ActiveDirectoryID = pool.ActiveDirectoryID
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
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
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
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
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
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
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

// PoolPreloadOptions controls which related entities are eagerly loaded alongside a pool query.
// Skipping unneeded preloads avoids extra DB round-trips for callers that only need a subset of fields.
type PoolPreloadOptions struct {
	KmsConfig       bool
	ActiveDirectory bool
}

// ListPoolsSelective fetches pools matching filter and only preloads the relations requested via opts.
// Account is always preloaded because it provides the account name used by the model converter.
func (d *DataStoreRepository) ListPoolsSelective(ctx context.Context, filter *utils2.Filter, opts PoolPreloadOptions) ([]*datamodel.PoolView, error) {
	var db *gorm.DB
	if filter != nil && filter.ShouldIncludeDeleted() {
		db = d.db.ApplyFilter(filter.Apply()).Unscoped().GORM().WithContext(ctx)
	} else if filter != nil {
		db = d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx)
	} else {
		db = d.db.GORM().WithContext(ctx)
	}

	db = db.Preload("Account")
	if opts.KmsConfig {
		db = db.Preload("KmsConfig")
	}
	if opts.ActiveDirectory {
		db = db.Preload("ActiveDirectory")
	}

	var pools []*datamodel.PoolView
	if err := db.Find(&pools).Error; err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return pools, nil
}

// ListPoolsWithFilterAndPaginationOrderedByUUID returns non-deleted pools matching the filter with limit/offset,
// ordered by uuid for stable pagination (no overlapping or missing pools across pages). Use for certificate and password rotation.
func (d *DataStoreRepository) ListPoolsWithFilterAndPaginationOrderedByUUID(ctx context.Context, filter *utils2.Filter, pagination *utils2.Pagination) ([]*datamodel.PoolView, error) {
	if filter != nil {
		return listPoolWithDetailsPaginationOrderedByUUID(d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx), pagination)
	}
	return listPoolWithDetailsPaginationOrderedByUUID(d.db.GORM().WithContext(ctx), pagination)
}

// ListPoolsWithPagination retrieves pools with pagination support including deleted pools
func (d *DataStoreRepository) ListPoolsWithPagination(ctx context.Context, conditions [][]interface{}, pagination *utils2.Pagination) ([]*datamodel.PoolView, error) {
	return listPoolWithDetailsPagination(d.db.ApplyFilter(conditions).Unscoped().GORM().WithContext(ctx), pagination)
}

// ListExpertModePools ListExpertModePool retrieves active pools that are running in ONTAP mode, optimized to only query necessary fields (uuid, build_info, state, api_access_mode) without preloads
func (d *DataStoreRepository) ListExpertModePools(ctx context.Context) ([]*datamodel.Pool, error) {
	var pools []*datamodel.Pool
	db := d.db.GORM().WithContext(ctx)

	err := db.Table("pools").
		Select("uuid", "build_info", "state", "api_access_mode").
		Where("api_access_mode = ?", "ONTAP").
		Find(&pools).Error

	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return pools, nil
}

func (d *DataStoreRepository) GetPoolByVendorID(ctx context.Context, vendorID string, accountID int64) (*datamodel.PoolView, error) {
	return getPoolWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Pool{VendorID: vendorID, AccountID: accountID})
}

func _getPoolWithDetails(db *gorm.DB, query *datamodel.Pool) (*datamodel.PoolView, error) {
	pool := &datamodel.PoolView{}
	err := db.Preload("Account").Preload("KmsConfig").Preload("KmsConfig.ServiceAccount").Preload("KmsConfig.Account").Preload("ActiveDirectory").First(&pool, query).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrPoolNotFound, customerrors.NewNotFoundErr("pool", nil))
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
	err := db.Preload("Account").Preload("KmsConfig").Preload("ActiveDirectory").Find(&pools).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return pools, nil
}

func _listPoolWithDetailsPagination(db *gorm.DB, pagination *utils2.Pagination) ([]*datamodel.PoolView, error) {
	var pools []*datamodel.PoolView
	err := db.Preload("Account").Scopes(utils2.Paginate(pagination)).Find(&pools).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return pools, nil
}

// listPoolWithDetailsPaginationOrderedByUUID paginates pools with deterministic order by uuid to avoid overlap across pages.
func listPoolWithDetailsPaginationOrderedByUUID(db *gorm.DB, pagination *utils2.Pagination) ([]*datamodel.PoolView, error) {
	var pools []*datamodel.PoolView
	err := db.Preload("Account").Order("uuid").Scopes(utils2.Paginate(pagination)).Find(&pools).Error
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

// CountActivePoolsByNetwork returns the number of non-deleted pools on the given network,
// optionally excluding a specific pool UUID (used during deletion to check remaining pools).
func (d *DataStoreRepository) CountActivePoolsByNetwork(ctx context.Context, network string, excludePoolUUID string) (int64, error) {
	db := d.db.GORM().WithContext(ctx).
		Model(&datamodel.Pool{}).
		Where("network = ? AND deleted_at IS NULL", network)
	if excludePoolUUID != "" {
		db = db.Where("uuid != ?", excludePoolUUID)
	}
	var count int64
	if err := db.Count(&count).Error; err != nil {
		return 0, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return count, nil
}

func (d *DataStoreRepository) GetPoolsByActiveDirectoryId(ctx context.Context, activeDirectoryId string) ([]*datamodel.Pool, error) {
	var pools []*datamodel.Pool

	err := d.db.GORM().WithContext(ctx).
		Preload("Account").
		Preload("ActiveDirectory").
		Where("active_directory_id = ?", activeDirectoryId).
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
		BaseModel:             view.BaseModel,
		Name:                  view.Name,
		Description:           view.Description,
		State:                 view.State,
		StateDetails:          view.StateDetails,
		VendorID:              view.VendorID,
		PoolOCID:              view.PoolOCID,
		ServiceLevel:          view.ServiceLevel,
		SizeInBytes:           view.SizeInBytes,
		UsedBytes:             view.UsedBytes,
		Network:               view.Network,
		AllowAutoTiering:      view.AllowAutoTiering,
		AccountID:             view.AccountID,
		Account:               view.Account,
		PoolAttributes:        view.PoolAttributes,
		ClusterDetails:        view.ClusterDetails,
		QosType:               view.QosType,
		AutoTieringConfig:     view.AutoTieringConfig,
		ServiceAccountId:      view.ServiceAccountId,
		DeploymentName:        view.DeploymentName,
		PoolCredentials:       view.PoolCredentials,
		KmsConfigID:           view.KmsConfigID,
		KmsConfig:             view.KmsConfig,
		SnHostProject:         view.SnHostProject,
		VLMConfig:             view.VLMConfig,
		LargeCapacity:         view.LargeCapacity,
		SatisfyZI:             view.SatisfyZI,
		SatisfyZS:             view.SatisfyZS,
		ActiveDirectory:       view.ActiveDirectory,
		ActiveDirectoryID:     view.ActiveDirectoryID,
		ExpertModeCredentials: view.ExpertModeCredentials,
		APIAccessMode:         view.APIAccessMode,
		BuildInfo:             view.BuildInfo,
	}
}

// ConvertPoolToPoolView converts a datamodel.Pool to a datamodel.PoolView for use in read operations or when you want to expose enriched fields.
func ConvertPoolToPoolView(pool *datamodel.Pool) *datamodel.PoolView {
	if pool == nil {
		return nil
	}
	return &datamodel.PoolView{
		Pool:                 *pool,
		Throughput:           0, // Set to 0 or fill in with actual value if available
		Iops:                 0, // Set to 0 or fill in with actual value if available
		QuotaInBytes:         0, // Set to 0 or fill in with actual value if available
		VolumeCount:          0, // Set to 0 or fill in with actual value if available
		ThinCloneVolumeCount: 0, // Set to 0 or fill in with actual value if available
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
	err = tx.Where("vendor_id = ?", pool.VendorID).Where("account_id = ?", pool.AccountID).First(&dbPool).Error
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

// UpdatePoolFields updates specific fields of a pool without changing its state
func (d *DataStoreRepository) UpdatePoolFields(ctx context.Context, poolUUID string, updates map[string]interface{}) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	// Add updated_at timestamp
	updates["updated_at"] = time.Now()

	// Update only the specified fields
	result := tx.Model(&datamodel.Pool{}).
		Where("uuid = ?", poolUUID).
		Updates(updates)
	if result.Error != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, result.Error)
	}
	if result.RowsAffected == 0 {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.New("pool not found"))
	}

	return nil
}

// UpdatePoolTieringConfig updates the auto tiering config
func (d *DataStoreRepository) UpdatePoolTieringConfig(ctx context.Context, poolUUID string, hotTierConsumption, coldTierConsumption, tieringThreshold *int64, tieringStatus *datamodel.TieringStatus) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	// Fetch the pool
	var pool datamodel.Pool
	if err := tx.Where("uuid = ?", poolUUID).First(&pool).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.New("Pool not found"))
		}
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	// Check if auto_tiering_config exists
	if pool.AutoTieringConfig == nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.New("auto_tiering_config is null"))
	}

	// Update only the fields that need update
	if hotTierConsumption != nil && *hotTierConsumption != pool.AutoTieringConfig.HotTierConsumption {
		pool.AutoTieringConfig.HotTierConsumption = *hotTierConsumption
	}
	if coldTierConsumption != nil && *coldTierConsumption != pool.AutoTieringConfig.ColdTierConsumption {
		pool.AutoTieringConfig.ColdTierConsumption = *coldTierConsumption
	}
	if tieringThreshold != nil && *tieringThreshold != pool.AutoTieringConfig.TieringFullnessThreshold {
		pool.AutoTieringConfig.TieringFullnessThreshold = *tieringThreshold
	}
	if tieringStatus != nil && *tieringStatus != pool.AutoTieringConfig.TieringStatus {
		pool.AutoTieringConfig.TieringStatus = *tieringStatus
	}

	// Save the entire pool back (GORM will update auto_tiering_config as a whole)
	if err := tx.Save(&pool).Error; err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return nil
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

	// Example, in case seq returns 1: 93501000000000000001
	// Example, in case seq returns 45555: 93501000000000045555
	// 935: predefined prefix
	// 01: region code, e.g., 01 for us-central1
	// 000000000000001: nextClusterSerialNumber padded to 15 digits
	return fmt.Sprintf("%s%015d", prefix, nextClusterSerialNumber), nil
}

func (d *DataStoreRepository) ListTpProjects(ctx context.Context) ([]string, error) {
	db := d.db.GORM().WithContext(ctx)
	var projects []string
	err := db.
		Model(&datamodel.Pool{}).
		Where("cluster_details->>'regional_tenant_project' <> ''").
		Where("cluster_details->>'regional_tenant_project' IS NOT NULL").
		Where("deleted_at IS NULL").
		Distinct().
		Pluck("cluster_details->>'regional_tenant_project'", &projects).Error
	if err != nil {
		return nil, err
	}
	return projects, nil
}

// PoolIdentifier contains pool identification information
type PoolIdentifier struct {
	UUID      string
	VendorID  string
	Name      string
	AccountID int64
}

// PoolMetricsData contains only the fields required for metrics collection
// This is an optimized structure that fetches minimal data from the database
type PoolMetricsData struct {
	ID                int64                        `gorm:"column:id"`
	UUID              string                       `gorm:"column:uuid"`
	Name              string                       `gorm:"column:name"`
	SizeInBytes       int64                        `gorm:"column:size_in_bytes"`
	DeploymentName    string                       `gorm:"column:deployment_name"`
	PoolAttributes    *datamodel.PoolAttributes    `gorm:"column:pool_attributes;type:jsonb"`
	AllowAutoTiering  bool                         `gorm:"column:allow_auto_tiering"`
	AutoTieringConfig *datamodel.AutoTieringConfig `gorm:"column:auto_tiering_config;type:jsonb"`
	// QuotaInBytes is fetched from pool_views via JOIN (sum of volume sizes minus clones_shared_bytes)
	QuotaInBytes uint64 `gorm:"column:quota_in_bytes"`
}

// GetAccountName returns the account name from PoolAttributes
func (p *PoolMetricsData) GetAccountName() string {
	if p.PoolAttributes != nil {
		return p.PoolAttributes.AccountName
	}
	return ""
}

// ListPoolUUIDs retrieves pool identifiers that match the provided filter
func (d *DataStoreRepository) ListPoolUUIDs(ctx context.Context, filter *utils2.Filter) ([]*PoolIdentifier, error) {
	var db *gorm.DB

	if filter != nil {
		if filter.ShouldIncludeDeleted() {
			db = d.db.ApplyFilter(filter.Apply()).Unscoped().GORM().WithContext(ctx)
		} else {
			db = d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx)
		}
	} else {
		db = d.db.GORM().WithContext(ctx)
	}

	var results []*PoolIdentifier
	err := db.Model(&datamodel.Pool{}).Select("uuid, vendor_id, name, account_id").Find(&results).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return results, nil
}

// ListPoolUUIDsPaginated retrieves pool identifiers with pagination support
func (d *DataStoreRepository) ListPoolUUIDsPaginated(ctx context.Context, filter *utils2.Filter, offset, limit int) ([]*PoolIdentifier, error) {
	var db *gorm.DB

	if filter != nil {
		if filter.ShouldIncludeDeleted() {
			db = d.db.ApplyFilter(filter.Apply()).Unscoped().GORM().WithContext(ctx)
		} else {
			db = d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx)
		}
	} else {
		db = d.db.GORM().WithContext(ctx)
	}

	// Apply pagination if limit > 0
	if limit > 0 {
		db = db.Limit(limit)
	}
	if offset > 0 {
		db = db.Offset(offset)
	}

	var results []*PoolIdentifier
	err := db.Model(&datamodel.Pool{}).Select("uuid, vendor_id, name, account_id").Find(&results).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return results, nil
}

// GetPoolsCount counts pools based on the provided filter
func (d *DataStoreRepository) GetPoolsCount(ctx context.Context, filter *utils2.Filter) (int64, error) {
	var db *gorm.DB

	if filter != nil {
		if filter.ShouldIncludeDeleted() {
			db = d.db.ApplyFilter(filter.Apply()).Unscoped().GORM().WithContext(ctx)
		} else {
			db = d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx)
		}
	} else {
		db = d.db.GORM().WithContext(ctx)
	}

	var count int64
	var err error

	// Only apply deleted_at filter when not including deleted records
	if filter == nil || !filter.ShouldIncludeDeleted() {
		err = db.Model(&datamodel.Pool{}).Where("deleted_at IS NULL").Count(&count).Error
	} else {
		err = db.Model(&datamodel.Pool{}).Count(&count).Error
	}

	if err != nil {
		return 0, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return count, nil
}

func _getPoolsByKmsConfigID(db *gorm.DB, kmsConfigID int64) ([]*datamodel.Pool, error) {
	var pools []*datamodel.Pool
	err := db.Where("kms_config_id = ?", kmsConfigID).Find(&pools).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return pools, nil
}

// ListPoolsForMetrics retrieves pools with only the fields required for metrics collection.
// This is an optimized query that selects only required columns directly from pool_views.
// Account name is extracted from pool_attributes JSONB column.
func (d *DataStoreRepository) ListPoolsForMetrics(ctx context.Context) ([]*PoolMetricsData, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("ListPoolsForMetrics: Starting optimized pool metrics query")

	db := d.db.GORM().WithContext(ctx)

	var results []*PoolMetricsData

	// Select required columns directly from pool_views
	err := db.Table("pool_views").
		Select(`
			id,
			uuid,
			name,
			size_in_bytes,
			deployment_name,
			pool_attributes,
			allow_auto_tiering,
			auto_tiering_config,
			quota_in_bytes
		`).
		Where("deleted_at IS NULL").
		Find(&results).Error

	if err != nil {
		logger.Error("ListPoolsForMetrics: Query failed", "error", err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	logger.Infof(fmt.Sprintf("ListPoolsForMetrics: Successfully fetched %d pools with only required fields for metrics.", len(results)))
	return results, nil
}

// PoolResourceData contains only the fields required for aggregator resource data collection.
// This is an optimized structure for fetchPoolData in telemetry aggregator.
type PoolResourceData struct {
	ID               int64                     `gorm:"column:id"`
	UUID             string                    `gorm:"column:uuid"`
	Name             string                    `gorm:"column:name"`
	AccountID        int64                     `gorm:"column:account_id"`
	DeploymentName   string                    `gorm:"column:deployment_name"`
	PoolAttributes   *datamodel.PoolAttributes `gorm:"column:pool_attributes;type:jsonb"`
	AllowAutoTiering bool                      `gorm:"column:allow_auto_tiering"`
	LargeCapacity    bool                      `gorm:"column:large_capacity"`
	APIAccessMode    string                    `gorm:"column:api_access_mode"`
	CreatedAt        time.Time                 `gorm:"column:created_at"`
}

// GetAccountName returns the account name from PoolAttributes
func (p *PoolResourceData) GetAccountName() string {
	if p.PoolAttributes != nil {
		return p.PoolAttributes.AccountName
	}
	return ""
}

// GetLabels returns labels from PoolAttributes
func (p *PoolResourceData) GetLabels() *datamodel.JSONB {
	if p.PoolAttributes != nil {
		return p.PoolAttributes.Labels
	}
	return nil
}

// IsRegionalHA returns whether pool is regional HA
func (p *PoolResourceData) IsRegionalHA() bool {
	if p.PoolAttributes != nil {
		return p.PoolAttributes.IsRegionalHA
	}
	return false
}

// ListPoolsForResourceData retrieves pools with only the fields required for aggregator resource data collection.
// This is an optimized query with pagination support for fetchPoolData in telemetry aggregator.
// Includes support for deleted_at filter to include recently deleted pools.
func (d *DataStoreRepository) ListPoolsForResourceData(ctx context.Context, startTime, endTime time.Time, pagination *utils2.Pagination) ([]*PoolResourceData, error) {
	db := d.db.GORM().WithContext(ctx)

	var results []*PoolResourceData

	// Select only the required columns from pools table
	// Account name and labels are available in pool_attributes JSONB, no JOIN needed
	query := db.Table("pools").
		Select(`
			id,
			uuid,
			name,
			account_id,
			deployment_name,
			pool_attributes,
			allow_auto_tiering,
			large_capacity,
			api_access_mode,
			created_at
		`).
		Where("(deleted_at IS NULL OR (deleted_at >= ? AND deleted_at <= ?))", startTime, endTime)

	// Apply pagination
	if pagination != nil {
		if pagination.Limit > 0 {
			query = query.Limit(pagination.Limit)
		}
		if pagination.Offset > 0 {
			query = query.Offset(pagination.Offset)
		}
	}

	err := query.Find(&results).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return results, nil
}

// ListOntapModePoolsForResourceData retrieves ONTAP mode pools with only the fields required for aggregator resource data collection.
// This is an optimized query with pagination support for fetchPoolData in telemetry aggregator.
// Includes support for deleted_at filter to include recently deleted pools.
func (d *DataStoreRepository) ListOntapModePoolsForResourceData(ctx context.Context, startTime, endTime time.Time, pagination *utils2.Pagination) ([]*PoolResourceData, error) {
	db := d.db.GORM().WithContext(ctx)

	var results []*PoolResourceData

	query := db.Table("pools").
		Select(`
			id,
			uuid,
			name,
			account_id,
			deployment_name,	
			pool_attributes,
			api_access_mode
		`).
		Where("api_access_mode = ?", "ONTAP").
		Where("(deleted_at IS NULL OR (deleted_at >= ? AND deleted_at <= ?))", startTime, endTime).
		Order("id")

	if pagination != nil {
		if pagination.Limit > 0 {
			query = query.Limit(pagination.Limit)
		}
		if pagination.Offset > 0 {
			query = query.Offset(pagination.Offset)
		}
	}

	err := query.Find(&results).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return results, nil
}

// GetBlockOnlyPoolIDs returns a map of pool IDs that contain ONLY block volumes (ISCSI)
// and have AllowAutoTiering enabled.
// Returns map[poolID]true for pools that have at least one ISCSI volume and NO NAS volumes.
// Used for auto-tiering billing guardrails to identify pure block pools eligible for billing.
func (d *DataStoreRepository) GetBlockOnlyPoolIDs(ctx context.Context) (map[int64]bool, error) {
	var poolIDs []int64

	err := d.db.GORM().WithContext(ctx).Raw(`
		SELECT DISTINCT pools.id
		FROM pools
		WHERE pools.deleted_at IS NULL
		  AND pools.allow_auto_tiering = true
		  AND EXISTS (
		    SELECT 1
		    FROM volumes
		    WHERE volumes.pool_id = pools.id
		      AND volumes.deleted_at IS NULL
		      AND volumes.volume_attributes->'protocols' ? 'ISCSI'
		  )
		  AND NOT EXISTS (
		    SELECT 1
		    FROM volumes
		    WHERE volumes.pool_id = pools.id
		      AND volumes.deleted_at IS NULL
		      AND (
		        volumes.volume_attributes->'protocols' ?| ARRAY['NFS','NFSV3','NFSV4','SMB']
		      )
		  )
	`).Scan(&poolIDs).Error

	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	// Convert slice to map for O(1) lookups
	result := make(map[int64]bool, len(poolIDs))
	for _, id := range poolIDs {
		result[id] = true
	}

	return result, nil
}
