package database

import (
	"context"
	"database/sql"
	"errors"
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
	getSvmsByKmsConfigID  = _getSvmsByKmsConfigID
	listSvmsWithAccountId = _listSvmsWithAccountId
)

// GetSvmsByPoolID retrieves SVMs by its corresponding pool ID
func (d *DataStoreRepository) GetSvmsByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Svm, error) {
	var svms []*datamodel.Svm
	err := d.db.GORM().Unscoped().WithContext(ctx).Where("pool_id = ?", poolID).Find(&svms).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return svms, nil
}

// GetNextSVMIndexByPoolID retrieves the next SVM index (count + 1) by pool ID
func (d *DataStoreRepository) GetNextSVMIndexByPoolID(ctx context.Context, poolID int64) (int64, error) {
	var count int64
	err := d.db.GORM().Unscoped().WithContext(ctx).Model(&datamodel.Svm{}).Where("pool_id = ?", poolID).Count(&count).Error
	if err != nil {
		return 0, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return count + 1, nil
}

// GetSvmsByKmsConfigID retrieves SVMs by kms config id
func (d *DataStoreRepository) GetSvmsByKmsConfigID(ctx context.Context, kmsConfigID int64) ([]*datamodel.Svm, error) {
	return getSvmsByKmsConfigID(d.db.GORM().WithContext(ctx), kmsConfigID)
}

func _getSvmsByKmsConfigID(db *gorm.DB, kmsConfigID int64) ([]*datamodel.Svm, error) {
	var svms []*datamodel.Svm
	err := db.Where("kms_config_id = ?", kmsConfigID).Find(&svms).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return svms, nil
}

// CreateSVM creates a new SVM in the database
func (d *DataStoreRepository) CreateSVM(ctx context.Context, svm *datamodel.Svm) (*datamodel.Svm, error) {
	var dbSvm datamodel.Svm
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	err1 := tx.Where("account_id = ?", svm.AccountID).Where("name = ?", svm.Name).Where("pool_id = ?", svm.PoolID).First(&dbSvm).Error
	if errors.Is(err1, gorm.ErrRecordNotFound) {
		svm.UUID = utils.RandomUUID()
		svm.CreatedAt = time.Now()
		svm.UpdatedAt = svm.CreatedAt
		svm.State = models.LifeCycleStateREADY
		svm.StateDetails = models.LifeCycleStateAvailableDetails

		err = tx.Create(svm).Error
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
		}
		err = tx.Where("account_id = ?", svm.AccountID).Where("name = ?", svm.Name).Where("pool_id = ?", svm.PoolID).First(&dbSvm).Error
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
		return &dbSvm, nil
	} else if err1 != nil {
		logger.Errorf("Error while checking if svm exists: %v", err1)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err1)
	}
	return nil, vsaerrors.NewVCPError(vsaerrors.ErrIncorrectVSAClusterState, customerrors.NewConflictErr("svm already exists"))
}

func (d *DataStoreRepository) GetSvmForPoolID(ctx context.Context, poolID int64) (*datamodel.Svm, error) {
	return getSvmWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Svm{PoolID: poolID})
}

func getSvmWithDetails(db *gorm.DB, query *datamodel.Svm) (*datamodel.Svm, error) {
	svm := &datamodel.Svm{}
	err := db.Preload("Account").First(&svm, query).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "svm", nil)
	}
	return svm, nil
}

// DeleteSVM deletes an SVM from the database
func (d *DataStoreRepository) DeleteSVM(ctx context.Context, svm *datamodel.Svm) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	svm.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	svm.State = models.LifeCycleStateDeleted
	svm.StateDetails = models.LifeCycleStateDeletedDetails
	err = tx.Updates(svm).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

// ErroredSVM marks an SVM with error state the database
func (d *DataStoreRepository) ErroredSVM(ctx context.Context, svm *datamodel.Svm, errMsg string) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	svm.UpdatedAt = time.Now()
	svm.State = models.LifeCycleStateError
	svm.StateDetails = errMsg
	err = tx.Updates(svm).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

// DeletingSVM deletes an SVM from the database
func (d *DataStoreRepository) DeletingSVM(ctx context.Context, svm *datamodel.Svm) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	svm.State = models.LifeCycleStateDeleting
	svm.StateDetails = models.LifeCycleStateDeletingDetails
	err = tx.Updates(svm).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

func (d *DataStoreRepository) UpdateSvmWithKmsConfigIDs(ctx context.Context, svm *datamodel.Svm, gcpKmsConfigUUID, externalKmsConfigUUID string) (*datamodel.Svm, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}

	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	kmsConfig, err := d.GetKmsConfig(ctx, gcpKmsConfigUUID)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	svm.KmsConfigID = sql.NullInt64{Int64: kmsConfig.ID, Valid: true}
	svm.KmsConfig = kmsConfig
	svm.UpdatedAt = time.Now()
	svm.SvmDetails.ExternalKmsConfigUUID = externalKmsConfigUUID

	err = tx.Save(svm).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return svm, nil
}

func (d *DataStoreRepository) UpdateSvmActiveDirectoryID(ctx context.Context, svm *datamodel.Svm, activeDirectoryID int64) (*datamodel.Svm, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}

	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	svm.ActiveDirectoryID = sql.NullInt64{Int64: activeDirectoryID, Valid: true}
	svm.UpdatedAt = time.Now()

	err = tx.Save(svm).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return svm, nil
}

func (d *DataStoreRepository) UnsetSvmActiveDirectoryID(ctx context.Context, svm *datamodel.Svm) (*datamodel.Svm, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}

	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	svm.ActiveDirectoryID = sql.NullInt64{Valid: false}
	svm.UpdatedAt = time.Now()

	err = tx.Save(svm).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return svm, nil
}

// UpdateSvmCurrentKmsKeyID updates the current KMS key ID in SvmDetails
// This tracks which service account key the SVM is currently using during rotation
func (d *DataStoreRepository) UpdateSvmCurrentKmsKeyID(ctx context.Context, svmUUID string, keyID string) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}

	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	svm := &datamodel.Svm{}
	err = tx.Where("uuid = ?", svmUUID).First(svm).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	// Initialize SvmDetails if nil
	if svm.SvmDetails == nil {
		svm.SvmDetails = &datamodel.SvmDetails{}
	}

	svm.SvmDetails.CurrentKmsKeyID = keyID
	svm.UpdatedAt = time.Now()

	err = tx.Save(svm).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return nil
}

func (d *DataStoreRepository) ListSvmsWithAccountId(ctx context.Context, accountId int64) ([]*datamodel.Svm, error) {
	return listSvmsWithAccountId(d.db.GORM().WithContext(ctx), accountId)
}

func _listSvmsWithAccountId(db *gorm.DB, accountId int64) ([]*datamodel.Svm, error) {
	var svms []*datamodel.Svm
	err := db.Where("account_id = ?", accountId).Find(&svms).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return svms, nil
}

// GetSvmByNameAndPoolID retrieves an SVM by name and pool ID
func (d *DataStoreRepository) GetSvmByNameAndPoolID(ctx context.Context, name string, poolID int64) (*datamodel.Svm, error) {
	return getSvmWithDetails(d.db.GORM().WithContext(ctx), &datamodel.Svm{Name: name, PoolID: poolID})
}

// GetSvmByExternalUUID retrieves an SVM by external UUID from svm_details JSONB field and validates pool ownership
func (d *DataStoreRepository) GetSvmByExternalUUID(ctx context.Context, externalUUID string, poolID int64) (*datamodel.Svm, error) {
	db := d.db.GORM().WithContext(ctx)
	svm := &datamodel.Svm{}
	err := db.Where("pool_id = ? AND svm_details ->> 'external_uuid' = ?", poolID, externalUUID).
		First(&svm).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "svm", nil)
	}
	return svm, nil
}
