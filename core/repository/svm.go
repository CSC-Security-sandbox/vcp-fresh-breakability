package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

func (d *DataStoreRepository) CreateSVM(ctx context.Context, svm *datamodel.Svm) (*datamodel.Svm, error) {
	var dbSvm datamodel.Svm
	err := d.db.GORM().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Where("account_id = ?", svm.AccountID).Where("name = ?", svm.Name).Where("pool_id = ?", svm.PoolID).First(&dbSvm).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			svm.UUID = utils.RandomUUID()
			svm.CreatedAt = time.Now()
			svm.UpdatedAt = svm.CreatedAt
			svm.State = models.LifeCycleStateAvailable
			svm.StateDetails = models.LifeCycleStateAvailableDetails

			err = tx.Create(svm).Error
			if err != nil {
				return err
			}
			err = tx.Where("account_id = ?", svm.AccountID).Where("name = ?", svm.Name).Where("pool_id = ?", svm.PoolID).First(&dbSvm).Error
			if err != nil {
				return err
			}
			return nil
		}
		return errors.New("svm already exists")
	})
	if err != nil {
		return nil, err
	}
	return &dbSvm, nil
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
