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
	"gorm.io/gorm"
)

var (
	getPoolWithDetails = _getPoolWithDetails
)

type DataStoreRepository struct {
	db *gormWrapper.Wrapper
}

func NewDataStoreRepository(db *gormWrapper.Wrapper) *DataStoreRepository {
	return &DataStoreRepository{db: db}
}

func (d *DataStoreRepository) CreatePool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	db := d.db.GORM().WithContext(ctx)
	var dbpool datamodel.Pool
	err := db.Where("name = ?", pool.Name).Where("account_id = ?", pool.AccountID).First(&dbpool).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		pool.UUID = utils.RandomUUID()
		pool.State = models.LifeCycleStateCreating
		pool.StateDetails = models.LifeCycleStateCreatingDetails
		pool.CreatedAt = time.Now()
		pool.UpdatedAt = pool.CreatedAt
		pool.Account.ID = pool.AccountID
		err := db.Create(&pool).Error
		if err != nil {
			return nil, err
		}

		dbPool, err := getPoolWithDetails(db, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
		if err != nil {
			return nil, err
		}
		return dbPool, nil
	}
	return nil, errors.New("pool already exists")
}

func (d *DataStoreRepository) GetPool(ctx context.Context, poolUUID string) (*datamodel.Pool, error) {
	return getPoolWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: poolUUID}})
}

func (d *DataStoreRepository) UpdatePool(ctx context.Context, pool *datamodel.Pool) error {
	return d.db.GORM().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		dbPool, err := getPoolWithDetails(tx, &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: pool.UUID}})
		if err != nil {
			return err
		}
		dbPool.UpdatedAt = time.Now()
		dbPool.State = pool.State
		dbPool.StateDetails = pool.StateDetails
		if err := tx.Updates(dbPool).Error; err != nil {
			return err
		}
		return nil
	})
}

func (d *DataStoreRepository) DeletePool(ctx context.Context, id string) error {
	// TODO implement me
	panic("implement me")
}

func (d *DataStoreRepository) ListPools(ctx context.Context) ([]*datamodel.Pool, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DataStoreRepository) GetPoolByVendorID(ctx context.Context, vendorID string) (*datamodel.Pool, error) {
	return getPoolWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Pool{VendorID: vendorID})
}

func _getPoolWithDetails(db *gorm.DB, query *datamodel.Pool) (*datamodel.Pool, error) {
	pool := &datamodel.Pool{}
	err := db.Preload("Account").First(&pool, query).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "pool", nil)
	}
	return pool, nil
}

func (d *DataStoreRepository) SavePoolWithVsaClusterDetails(ctx context.Context, poolName string, accountName string, cluster *datamodel.ClusterDetails) error {
	db := d.db.GORM().WithContext(ctx)
	account, err := getAccount(db, &datamodel.Account{Name: accountName})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errors.New("pool not found")
	}
	if err != nil {
		return err
	}
	pool, err := getPoolWithDetails(db, &datamodel.Pool{Name: poolName, AccountID: account.ID})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errors.New("pool not found")
	}
	if err != nil {
		return err
	}
	pool.ClusterDetails = *cluster
	err = db.Model(&pool).Updates(map[string]interface{}{
		"cluster_details": pool.ClusterDetails,
	}).Error
	if err != nil {
		return err
	}
	return nil
}
