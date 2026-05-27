package database

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	getBackupPolicyWithDetails = _getBackupPolicyWithDetails
)

func (d *DataStoreRepository) GetBackupPolicyByNameAndOwnerID(ctx context.Context, backupPolicyName string, accountID int64) (*datamodel.BackupPolicy, error) {
	db := d.db.GORM().WithContext(ctx)
	dbBackupPolicy, err := getBackupPolicyWithDetails(db, &datamodel.BackupPolicy{Name: backupPolicyName, AccountID: accountID})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("backup policy", &backupPolicyName)
		}
		return nil, err
	}
	return dbBackupPolicy, nil
}

func (d *DataStoreRepository) GetBackupPolicyByUUIDAndOwnerID(ctx context.Context, backupPolicyUUID string, accountID int64) (*datamodel.BackupPolicy, error) {
	db := d.db.GORM().WithContext(ctx)
	dbBackupPolicy, err := getBackupPolicyWithDetails(db, &datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: backupPolicyUUID}, AccountID: accountID})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("backup policy", &backupPolicyUUID)
		}
		return nil, err
	}
	return dbBackupPolicy, nil
}

func (d *DataStoreRepository) GetVolumeCountByBackupPolicyID(ctx context.Context, backupPolicyUUID string) (int64, error) {
	db := d.db.GORM().WithContext(ctx)
	var volumeCount int64
	err := db.Model(&datamodel.Volume{}).
		Where("data_protection->>'backup_policy_id' = ?", backupPolicyUUID).
		Count(&volumeCount).Error
	if err != nil {
		return 0, err
	}
	return volumeCount, nil
}

func (d *DataStoreRepository) GetBackupPolicyUUIDsFromBackupVaultUUID(ctx context.Context, backupVaultUUID string, accountID int64) ([]string, error) {
	var backupPolicyUUIDs []string

	db := d.db.GORM().WithContext(ctx)
	err := db.Model(&datamodel.Volume{}).
		Distinct("data_protection->>'backup_policy_id'").
		Where("data_protection->>'backup_vault_id' = ?", backupVaultUUID).
		Where("data_protection->>'backup_policy_id' != ''").
		Where("data_protection->>'backup_policy_id' IS NOT NULL").
		Pluck("data_protection->>'backup_policy_id'", &backupPolicyUUIDs).Error

	if err != nil {
		return nil, err
	}

	return backupPolicyUUIDs, nil
}

func (d *DataStoreRepository) ListBackupPolicyVolumeCount(ctx context.Context, conditions [][]interface{}) (map[string]int64, error) {
	var backupPolicies []struct {
		BackupPolicyID string `json:"backup_policy_id"`
		Count          int64  `json:"count"`
	}
	db := d.db.ApplyFilter(conditions).GORM().WithContext(ctx)
	err := db.Model(&datamodel.Volume{}).
		Select("data_protection->>'backup_policy_id' as backup_policy_id, count(*) as count").
		Where("data_protection->>'backup_policy_id' != ''").
		Group("data_protection->>'backup_policy_id'").
		Scan(&backupPolicies).Error
	if err != nil {
		return nil, err
	}
	backupPoliciesMap := make(map[string]int64)
	for _, bp := range backupPolicies {
		if bp.BackupPolicyID != "" {
			backupPoliciesMap[bp.BackupPolicyID] = bp.Count
		}
	}
	return backupPoliciesMap, nil
}

func (d *DataStoreRepository) ListBackupPolicies(ctx context.Context, conditions [][]interface{}) ([]*datamodel.BackupPolicy, error) {
	db := d.db.ApplyFilter(conditions).GORM().WithContext(ctx)
	var backupPolicies []*datamodel.BackupPolicy
	err := db.Find(&backupPolicies).Error
	if err != nil {
		return nil, err
	}
	return backupPolicies, nil
}

// ListBackupPoliciesWithPagination retrieves backup policies with pagination support.
func (d *DataStoreRepository) ListBackupPoliciesWithPagination(ctx context.Context, conditions [][]interface{}, pagination *dbutils.Pagination) ([]*datamodel.BackupPolicy, error) {
	return _listBackupPoliciesWithPagination(d.db.ApplyFilter(conditions).GORM().WithContext(ctx), pagination)
}

func _listBackupPoliciesWithPagination(db *gorm.DB, pagination *dbutils.Pagination) ([]*datamodel.BackupPolicy, error) {
	var backupPolicies []*datamodel.BackupPolicy
	err := db.Scopes(dbutils.Paginate(pagination)).Find(&backupPolicies).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return backupPolicies, nil
}

func (d *DataStoreRepository) CreateBackupPolicyEntryInVCP(ctx context.Context, backupPolicy *datamodel.BackupPolicy) (*datamodel.BackupPolicy, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	var dbBackupPolicy datamodel.BackupPolicy
	err = tx.Where("name = ? AND account_id = ?", backupPolicy.Name, backupPolicy.AccountID).First(&dbBackupPolicy).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		backupPolicy.CreatedAt = time.Now()
		backupPolicy.UpdatedAt = backupPolicy.CreatedAt

		err = tx.Create(backupPolicy).Error
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
		}

		dbBackupPolicyDetail, err := getBackupPolicyWithDetails(tx, backupPolicy)
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
		return dbBackupPolicyDetail, nil
	} else if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	logger.Warnf("Backup policy with name %s already exists for account ID %d", backupPolicy.Name, backupPolicy.AccountID)
	dbBackupPolicyDetail, err := getBackupPolicyWithDetails(db, &datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: dbBackupPolicy.UUID}, AccountID: dbBackupPolicy.AccountID})
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return dbBackupPolicyDetail, nil
}

func (d *DataStoreRepository) UpdateBackupPolicy(ctx context.Context, uuid string, updates map[string]interface{}) (*datamodel.BackupPolicy, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	var updated datamodel.BackupPolicy
	err = tx.Model(&updated).Clauses(clause.Returning{}).Where("uuid = ?", uuid).Updates(updates).Error
	if err != nil {
		return nil, err
	}
	return &updated, nil
}

func (d *DataStoreRepository) DeleteBackupPolicy(ctx context.Context, backupPolicyUUID string) (*datamodel.BackupPolicy, error) {
	db := d.db.GORM().WithContext(ctx)
	logger := util.GetLogger(ctx)

	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(logger, tx, &err)

	dbBackupPolicy, err := getBackupPolicyWithDetails(tx, &datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: backupPolicyUUID}})
	if err != nil {
		return nil, err
	}

	dbBackupPolicy.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	dbBackupPolicy.LifeCycleState = datamodel.LifeCycleStateDeleted
	dbBackupPolicy.LifeCycleStateDetails = datamodel.LifeCycleStateDeletedDetails
	err = tx.Save(dbBackupPolicy).Error
	if err != nil {
		return nil, err
	}

	return dbBackupPolicy, nil
}

func _getBackupPolicyWithDetails(db *gorm.DB, query *datamodel.BackupPolicy) (*datamodel.BackupPolicy, error) {
	backupPolicy := &datamodel.BackupPolicy{}
	err := db.Preload("Account").First(&backupPolicy, query).Error
	if err != nil {
		return nil, err
	}
	return backupPolicy, nil
}
