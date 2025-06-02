package repository

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func (d *DataStoreRepository) GetKmsConfig(ctx context.Context, kmsConfigUUID string) (*datamodel.KmsConfig, error) {
	db := d.db.GORM().WithContext(ctx)
	return getKmsConfig(db, &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: kmsConfigUUID}})
}

func (d *DataStoreRepository) UpdateKmsConfigState(ctx context.Context, kmsConfigUUID string, state string, stateDetails string) (*datamodel.KmsConfig, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	kmsConfig, err := _getKmsConfig(tx, &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: kmsConfigUUID}})
	if err != nil {
		return nil, err
	}

	kmsConfig.State = state
	kmsConfig.StateDetails = stateDetails
	err = tx.Updates(kmsConfig).Error
	if err != nil {
		return nil, err
	}

	return kmsConfig, nil
}

func (d *DataStoreRepository) UpdateKmsConfig(ctx context.Context, kmsConfig *datamodel.KmsConfig) (*datamodel.KmsConfig, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(slogger.NewLogger(), tx, &err)

	kmsConfig.State = models.LifeCycleStateREADY
	kmsConfig.StateDetails = models.LifeCycleStateAvailableDetails

	err = tx.Updates(kmsConfig).Error
	if err != nil {
		return nil, err
	}

	return kmsConfig, nil
}
