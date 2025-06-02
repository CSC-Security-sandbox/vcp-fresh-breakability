package repository

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func (d *DataStoreRepository) UpdateServiceAccountEmailAndKey(ctx context.Context, uuid string, email string, key string) (*datamodel.ServiceAccount, error) {
	db := d.db.GORM().WithContext(ctx)
	dbServiceAccount := &datamodel.ServiceAccount{}
	err := db.Where("uuid = ?", uuid).First(dbServiceAccount).Error
	if err != nil {
		return nil, err
	}
	encKey, err := utils.EncryptPassword(log.Secret(key))
	if err != nil {
		return nil, err
	}
	dbServiceAccount.ServiceAccountPasswordLocation = *encKey
	dbServiceAccount.ServiceAccountEmail = email
	dbServiceAccount.UpdatedAt = utils.GetTimeNow()
	return dbServiceAccount, db.Where("uuid = ?", uuid).Updates(dbServiceAccount).Error
}

func (d *DataStoreRepository) UpdateServiceAccountState(ctx context.Context, uuid string, state string, stateDetails string) (*datamodel.ServiceAccount, error) {
	db := d.db.GORM().WithContext(ctx)
	dbServiceAccount := &datamodel.ServiceAccount{}
	err := db.Where("uuid = ?", uuid).First(dbServiceAccount).Error
	if err != nil {
		return nil, err
	}
	dbServiceAccount.State = state
	dbServiceAccount.StateDetails = stateDetails
	dbServiceAccount.UpdatedAt = utils.GetTimeNow()
	return dbServiceAccount, db.Where("uuid = ?", uuid).Updates(dbServiceAccount).Error
}
