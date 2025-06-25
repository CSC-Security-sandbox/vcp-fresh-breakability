package repository

import (
	"context"
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
	getSvmsByKmsConfigID = _getSvmsByKmsConfigID
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

// GetSvmsByKmsConfigID retrieves SVMs by kms config id
func (d *DataStoreRepository) GetSvmsByKmsConfigID(ctx context.Context, kmsConfigID int64) ([]*datamodel.Svm, error) {
	return getSvmsByKmsConfigID(d.db.GORM().WithContext(ctx), kmsConfigID)
}

func _getSvmsByKmsConfigID(db *gorm.DB, kmsConfigID int64) ([]*datamodel.Svm, error) {
	var svms []*datamodel.Svm
	err := db.Where("cmek_config_id = ?", kmsConfigID).Find(&svms).Error
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
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
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
