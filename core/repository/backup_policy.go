package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
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
		backupPolicy.UpdatedAt = time.Now()

		err = tx.Create(backupPolicy).Error
		if err != nil {
			return nil, err
		}

		dbBackupPolicyDetail, err := getBackupPolicyWithDetails(tx, backupPolicy)
		if err != nil {
			return nil, err
		}
		return dbBackupPolicyDetail, nil
	} else if err != nil {
		return nil, err
	}
	logger.Warnf("Backup policy with name %s already exists for account ID %d", backupPolicy.Name, backupPolicy.AccountID)
	dbBackupPolicyDetail, err := getBackupPolicyWithDetails(tx, &dbBackupPolicy)
	if err != nil {
		return nil, err
	}
	return dbBackupPolicyDetail, nil
}

func _getBackupPolicyWithDetails(db *gorm.DB, query *datamodel.BackupPolicy) (*datamodel.BackupPolicy, error) {
	backupPolicy := &datamodel.BackupPolicy{}
	err := db.Preload("Account").First(&backupPolicy, query).Error
	if err != nil {
		return nil, err
	}
	return backupPolicy, nil
}
