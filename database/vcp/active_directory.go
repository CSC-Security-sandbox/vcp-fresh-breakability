package database

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"gorm.io/gorm"
)

func (d *DataStoreRepository) CreateActiveDirectory(ctx context.Context, ad *datamodel.ActiveDirectory) (*datamodel.ActiveDirectory, error) {
	return createActiveDirectory(d.db.GORM().WithContext(ctx), ad)
}

func createActiveDirectory(db *gorm.DB, ad *datamodel.ActiveDirectory) (*datamodel.ActiveDirectory, error) {
	query := &datamodel.ActiveDirectory{AdName: ad.AdName, AccountId: ad.AccountId, BaseModel: datamodel.BaseModel{DeletedAt: nil}}
	existingAd, _ := getActiveDirectoryWithDetails(db, query)
	if existingAd != nil {
		return nil, errors.New("Active Directory with the given name already exists")
	}
	err := db.Create(ad).Error
	if err != nil {
		return nil, err
	}
	return ad, nil
}

func (d *DataStoreRepository) GetActiveDirectoryByNameAndAccountID(ctx context.Context, name string, accountID int64) (*datamodel.ActiveDirectory, error) {
	ad, err := getActiveDirectoryWithDetails(d.db.GORM().WithContext(ctx), &datamodel.ActiveDirectory{AdName: name, AccountId: accountID, BaseModel: datamodel.BaseModel{DeletedAt: nil}})
	if err != nil {
		return nil, err
	}
	return ad, nil
}

func getActiveDirectoryWithDetails(db *gorm.DB, query *datamodel.ActiveDirectory) (*datamodel.ActiveDirectory, error) {
	var ad datamodel.ActiveDirectory
	err := db.First(&ad, query).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &ad, nil
}
