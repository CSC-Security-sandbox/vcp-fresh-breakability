package repository

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"gorm.io/gorm"
)

func (d *DataStoreRepository) GetMultipleKmsConfigs(ctx context.Context, conditions [][]interface{}) ([]*datamodel.KmsConfig, error) {
	return getMultipleKmsConfigs(d.db.ApplyFilter(conditions).GORM().WithContext(ctx))
}

func getMultipleKmsConfigs(db *gorm.DB) ([]*datamodel.KmsConfig, error) {
	var kmsConfigs []*datamodel.KmsConfig
	err := db.Preload("ServiceAccount").Find(&kmsConfigs).Error
	if err != nil {
		return nil, err
	}
	return kmsConfigs, nil
}
