package database

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (d *DataStoreRepository) GetAppConfig(ctx context.Context, key string) (*datamodel.AppConfig, error) {
	db := d.db.GORM().WithContext(ctx)
	var cfg datamodel.AppConfig
	err := db.Where("\"key\" = ?", key).First(&cfg).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, err)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return &cfg, nil
}

func (d *DataStoreRepository) UpsertAppConfig(ctx context.Context, key, value string) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}

	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	cfg := &datamodel.AppConfig{
		BaseModel: datamodel.BaseModel{UUID: uuid.NewString()},
		Key:       key,
		Value:     value,
	}

	err = tx.Clauses(
		clause.OnConflict{
			Columns: []clause.Column{{Name: "key"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"value":      value,
				"updated_at": time.Now(),
			}),
		},
	).Create(cfg).Error
	if err != nil {
		logger.Errorf("Failed to upsert app config for key: %s, error: %v", key, err)
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
	}

	return nil
}
