package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"gorm.io/gorm"
)

func (d *DataStoreRepository) CreateSVM(ctx context.Context, svm *datamodel.Svm) (*datamodel.Svm, error) {
	var dbSvm datamodel.Svm
	db := d.db.GORM().WithContext(ctx)
	err := db.Where("name = ?", svm.Name).First(&dbSvm).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		svm.UUID = utils.RandomUUID()
		svm.CreatedAt = time.Now()
		svm.UpdatedAt = svm.CreatedAt
		svm.State = models.LifeCycleStateAvailable
		svm.StateDetails = models.LifeCycleStateAvailableDetails

		err = db.Create(svm).Error
		if err != nil {
			return nil, err
		}
		err = db.Where("name = ?", svm.Name).First(&dbSvm).Error
		if err != nil {
			return nil, err
		}
		return &dbSvm, nil
	}
	return nil, errors.New("svm already exists")
}
