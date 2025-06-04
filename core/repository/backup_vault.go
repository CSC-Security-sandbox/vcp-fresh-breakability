package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	getBackupVaultWithDetails = _getBackupVaultWithDetails
)

func (d *DataStoreRepository) CreateBackupVault(ctx context.Context, bv *datamodel.BackupVault, vcpBvParams *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}

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

	vcpBvParams, err = getBackupVaultWithDetails(db, &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: vcpBvParams.UUID}})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, errors.New("entry not found")
	} else if err != nil {
		return nil, err
	}
	if bv.LifeCycleState == models.LifeCycleStateREADY {
		bv.LifeCycleState = models.LifeCycleStateAvailable
	}

	// Update vcpBvParams with bv details
	vcpBvParams.Name = bv.Name
	vcpBvParams.Description = bv.Description
	vcpBvParams.BackupRegionName = bv.BackupRegionName
	vcpBvParams.ImmutableAttributes = bv.ImmutableAttributes
	vcpBvParams.BackupVaultType = bv.BackupVaultType
	vcpBvParams.SourceRegionName = bv.SourceRegionName
	vcpBvParams.CrossRegionBackupVaultName = bv.CrossRegionBackupVaultName
	vcpBvParams.AccountVendorID = bv.AccountVendorID
	vcpBvParams.LifeCycleState = bv.LifeCycleState
	vcpBvParams.LifeCycleStateDetails = bv.LifeCycleStateDetails
	vcpBvParams.UpdatedAt = time.Now()
	vcpBvParams.UUID = bv.UUID

	// Save the updated entry back to the database
	if err := tx.Save(&vcpBvParams).Error; err != nil {
		return nil, err
	}

	return vcpBvParams, nil
}

func _getBackupVaultWithDetails(db *gorm.DB, query *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	bv := &datamodel.BackupVault{}
	err := db.Preload("Account").First(&bv, query).Error
	if err != nil {
		return nil, err
	}
	return bv, nil
}

func (d *DataStoreRepository) GetBackupVault(ctx context.Context, backupVaultId string) (*datamodel.BackupVault, error) {
	var bv datamodel.BackupVault
	err := d.db.GORM().WithContext(ctx).Preload("Account").Where("uuid = ?", backupVaultId).First(&bv).Error
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

func (d *DataStoreRepository) GetBackupVaultByUUID(ctx context.Context, backupVaultID string) (*datamodel.BackupVault, error) {
	db := d.db.GORM().WithContext(ctx)
	dbBackupVault, err := getBackupVaultWithDetails(db, &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: backupVaultID}})
	if err != nil {
		return nil, err
	}
	return dbBackupVault, nil
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
		bv.Account.ID = bv.AccountID
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
