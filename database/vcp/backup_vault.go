package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	getBackupVaultWithDetails = _getBackupVaultWithDetails
	checkBVExists             = _checkBVExists
)

func (d *DataStoreRepository) DeleteBackupVaultInVCP(ctx context.Context, backupVaultId string) (*datamodel.BackupVault, error) {
	db := d.db.GORM().WithContext(ctx)
	logger := util.GetLogger(ctx)

	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(logger, tx, &err)

	dbBackupVault, err := getBackupVaultWithDetails(tx, &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: backupVaultId}})
	if err != nil {
		return nil, err
	}

	dbBackupVault.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	dbBackupVault.LifeCycleState = models.LifeCycleStateDeleted
	dbBackupVault.LifeCycleStateDetails = models.LifeCycleStateDeletedDetails
	err = tx.Save(dbBackupVault).Error
	if err != nil {
		return nil, err
	}

	return dbBackupVault, nil
}

func (d *DataStoreRepository) UpdateBackupVaultInVCP(ctx context.Context, sdeBackupVault *datamodel.BackupVault, dbBackupVault *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}

	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	dbBackupVault, err = getBackupVaultWithDetails(tx, &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: dbBackupVault.UUID}})
	if err != nil {
		return nil, err
	}

	dbBackupVault.Description = sdeBackupVault.Description
	dbBackupVault.ImmutableAttributes = sdeBackupVault.ImmutableAttributes
	dbBackupVault.CmekAttributes = sdeBackupVault.CmekAttributes
	dbBackupVault.LifeCycleState = sdeBackupVault.LifeCycleState
	dbBackupVault.LifeCycleStateDetails = sdeBackupVault.LifeCycleStateDetails
	dbBackupVault.UpdatedAt = time.Now()

	err = tx.Updates(dbBackupVault).Error
	if err != nil {
		return nil, err
	}

	return dbBackupVault, nil
}
func _getBackupVaultWithDetails(db *gorm.DB, query *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	bv := &datamodel.BackupVault{}
	err := db.Preload("Account").First(&bv, query).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("backup vault", &query.UUID)
		}
		return nil, err
	}
	return bv, nil
}

func _checkBVExists(tx *gorm.DB, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	bvDetails := &datamodel.BackupVault{}
	err1 := tx.Where("uuid = ? and account_id = ?", bv.UUID, bv.AccountID).First(&bvDetails).Error
	return bvDetails, err1
}

func (d *DataStoreRepository) GetBackupVault(ctx context.Context, backupVaultId string) (*datamodel.BackupVault, error) {
	var bv datamodel.BackupVault
	err := d.db.GORM().WithContext(ctx).Preload("Account").Where("uuid = ?", backupVaultId).First(&bv).Error
	if err != nil {
		return nil, err
	}
	return &bv, nil
}

func (d *DataStoreRepository) GetBackupVaultById(ctx context.Context, backupVaultId int64) (*datamodel.BackupVault, error) {
	var bv datamodel.BackupVault
	err := d.db.GORM().WithContext(ctx).Preload("Account").Where("id = ?", backupVaultId).First(&bv).Error
	if err != nil {
		return nil, err
	}
	return &bv, nil
}

func (d *DataStoreRepository) GetBackupVaultByNameAndOwnerID(ctx context.Context, backupVaultName, ownerID string) (*datamodel.BackupVault, error) {
	var bv datamodel.BackupVault
	err := d.db.GORM().WithContext(ctx).Preload("Account").Where("name = ?", backupVaultName).Where("account_id = ?", ownerID).First(&bv).Error
	if err != nil {
		return nil, err
	}
	return &bv, nil
}

func (d *DataStoreRepository) GetBackupVaultByCrossRegionBackupVaultName(ctx context.Context, crossRegionBackupVaultName string, accountID int64) (*datamodel.BackupVault, error) {
	var bv datamodel.BackupVault
	err := d.db.GORM().WithContext(ctx).Preload("Account").Where("cross_region_backup_vault_name = ?", crossRegionBackupVaultName).Where("account_id = ?", accountID).First(&bv).Error
	if err != nil {
		return nil, err
	}
	return &bv, nil
}

func (d *DataStoreRepository) GetBackupVaultByUUIDndOwnerID(ctx context.Context, backupVaultID string, accountID int64) (*datamodel.BackupVault, error) {
	db := d.db.GORM().WithContext(ctx)
	dbBackupVault, err := getBackupVaultWithDetails(db, &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: backupVaultID}, AccountID: accountID})
	if err != nil {
		return nil, err
	}
	return dbBackupVault, nil
}

// GetBackupVaultByExternalUUIDAndOwnerID gets a BackupVault by external UUID and owner ID (account ID)
func (d *DataStoreRepository) GetBackupVaultByExternalUUIDAndOwnerID(ctx context.Context, externalUUID string, accountID int64) (*datamodel.BackupVault, error) {
	var bv datamodel.BackupVault
	db := d.db.GORM().WithContext(ctx)
	err := db.Preload("Account").Where("external_uuid = ? AND account_id = ?", externalUUID, accountID).First(&bv).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("backup vault", &externalUUID)
		}
		return nil, err
	}
	return &bv, nil
}

func (d *DataStoreRepository) CreateBackupVaultEntryInVCP(ctx context.Context, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}

	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	bvDetails, err1 := checkBVExists(tx, bv)
	if errors.Is(err1, gorm.ErrRecordNotFound) {
		err = tx.Create(bv).Error
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
		}
		err := tx.Where("uuid = ? and account_id = ?", bv.UUID, bv.AccountID).First(&bvDetails).Error
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
		return bvDetails, nil
	} else if err1 != nil {
		logger.Errorf("Error while Attaching BackupVault: %v", err1)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err1)
	}

	backupVaultDetails, err := getBackupVaultWithDetails(db, &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: bv.UUID}})
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return backupVaultDetails, nil
}

func (d *DataStoreRepository) UpdateBackupVault(ctx context.Context, bv *datamodel.BackupVault) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	dbBv, err := getBackupVaultWithDetails(tx, &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: bv.UUID}})
	if err != nil {
		return err
	}

	dbBv.UpdatedAt = time.Now()
	dbBv.BucketDetails = bv.BucketDetails

	if err = tx.Save(dbBv).Error; err != nil {
		return err
	}
	return nil
}

func (d *DataStoreRepository) UpdateBackupVaultState(ctx context.Context, bv *datamodel.BackupVault, state, stateDetails string) (*datamodel.BackupVault, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	bv.LifeCycleState = state
	bv.LifeCycleStateDetails = stateDetails
	err = tx.Save(bv).Error
	if err != nil {
		return nil, err
	}

	dbBackupVault, err := getBackupVaultWithDetails(tx, bv)
	if err != nil {
		return nil, err
	}
	return dbBackupVault, nil
}
func (d *DataStoreRepository) ListBackupVaults(ctx context.Context, accountID int64) ([]*datamodel.BackupVault, error) {
	db := d.db.GORM().WithContext(ctx)
	var backupVaults []*datamodel.BackupVault
	err := db.Preload("Account").Where("account_id = ?", accountID).Find(&backupVaults).Error
	if err != nil {
		return nil, err
	}
	return backupVaults, nil
}

// CreatingBackupVault creates a new backup vault in the database
func (d *DataStoreRepository) CreatingBackupVault(ctx context.Context, bv *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}

	logger := util.GetLogger(ctx)
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		} else if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	var dbBackupVault *datamodel.BackupVault
	err1 := tx.Where("name = ?", bv.Name).Where("account_id = ?", bv.AccountID).First(&dbBackupVault).Error
	if errors.Is(err1, gorm.ErrRecordNotFound) {
		bv.UUID = utils.RandomUUID()
		bv.LifeCycleState = models.LifeCycleStateCreating
		bv.LifeCycleStateDetails = models.LifeCycleStateCreatingDetails
		bv.CreatedAt = time.Now()
		bv.UpdatedAt = bv.CreatedAt
		err = tx.Create(&bv).Error
		if err != nil {
			return nil, err
		}

		dbBackupVault, err = getBackupVaultWithDetails(tx, bv)
		if err != nil {
			return nil, err
		}
		return dbBackupVault, nil
	} else if err1 != nil {
		logger.Errorf("Error while checking if backup vault exists: %v", err1)
		return nil, err1
	}
	return nil, customerrors.NewConflictErr("backup vault already exists")
}

// GetMultipleBackupVaults retrieves multiple BackupVaults based on the provided conditions
func (d *DataStoreRepository) GetMultipleBackupVaults(ctx context.Context, conditions [][]interface{}) ([]*datamodel.BackupVault, error) {
	return getMultipleBackupVaults(d.db.ApplyFilter(conditions).GORM().WithContext(ctx))
}

func getMultipleBackupVaults(db *gorm.DB) ([]*datamodel.BackupVault, error) {
	var backupVaults []*datamodel.BackupVault
	err := db.Preload("Account").Find(&backupVaults).Error
	if err != nil {
		return nil, err
	}
	return backupVaults, nil
}

// GetBackupVaultUUIDsFromBackupPolicyUUID retrieves all backup vault UUIDs associated with a backup policy
func (d *DataStoreRepository) GetBackupVaultUUIDsFromBackupPolicyUUID(ctx context.Context, backupPolicyUUID string, accountID int64) ([]string, error) {
	var backupVaultUUIDs []string
	db := d.db.GORM().WithContext(ctx)
	err := db.Model(&datamodel.Volume{}).
		Distinct("data_protection->>'backup_vault_id'").
		Where("data_protection->>'backup_policy_id' = ?", backupPolicyUUID).
		Where("data_protection->>'backup_vault_id' IS NOT NULL").
		Where("data_protection->>'backup_vault_id' != ''").
		Pluck("data_protection->>'backup_vault_id'", &backupVaultUUIDs).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get backup vault UUIDs from backup policy: %v", err)
	}

	return backupVaultUUIDs, nil
}
