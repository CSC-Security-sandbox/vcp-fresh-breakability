package database

import (
	"context"
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

var (
	listKmsServiceAccounts = _listKmsServiceAccounts
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

// GetServiceAccountFromEmail gets the Kms Service Account based on SA email
func (d *DataStoreRepository) GetServiceAccountFromEmail(ctx context.Context, email string) (*datamodel.ServiceAccount, error) {
	db := d.db.GORM().WithContext(ctx)
	sa := &datamodel.ServiceAccount{}
	err := db.First(&sa, &datamodel.ServiceAccount{ServiceAccountEmail: email}).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.NewNotFoundErr("service account", nil))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return sa, nil
}

func (d *DataStoreRepository) ListKmsServiceAccounts(ctx context.Context, filter *dbutils.Filter) ([]*datamodel.ServiceAccount, error) {
	if filter != nil {
		return listKmsServiceAccounts(d.db.ApplyFilter(filter.Apply()).GORM().WithContext(ctx))
	}
	return listKmsServiceAccounts(d.db.GORM().WithContext(ctx))
}

func _listKmsServiceAccounts(db *gorm.DB) ([]*datamodel.ServiceAccount, error) {
	var sa []*datamodel.ServiceAccount
	err := db.Find(&sa).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return sa, nil
}
