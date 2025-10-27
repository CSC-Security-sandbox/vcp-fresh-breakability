package database

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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

func (d *DataStoreRepository) GetActiveDirectoryByUuidAndAccountId(ctx context.Context, uuid string, accountID int64) (*datamodel.ActiveDirectory, error) {
	db := d.db.GORM().WithContext(ctx)
	query := &datamodel.ActiveDirectory{AccountId: accountID, BaseModel: datamodel.BaseModel{DeletedAt: nil, UUID: uuid}}
	var ad datamodel.ActiveDirectory
	err := db.First(&ad, query).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "Active Directory", nil)
		}
		return nil, err
	}
	return &ad, nil
}

func (d *DataStoreRepository) GetActiveDirectoryByUUID(ctx context.Context, uuid string) (*datamodel.ActiveDirectory, error) {
	ad, err := getActiveDirectoryWithDetails(d.db.GORM().Unscoped().WithContext(ctx), &datamodel.ActiveDirectory{BaseModel: datamodel.BaseModel{UUID: uuid}})
	if err != nil {
		return nil, err
	}
	return ad, nil
}

func (d *DataStoreRepository) ListActiveDirectories(ctx context.Context, accountID int64) ([]*datamodel.ActiveDirectory, error) {
	return listActiveDirectories(d.db.GORM().WithContext(ctx), accountID)
}

func (d *DataStoreRepository) GetMultipleActiveDirectoriesByUUIDs(ctx context.Context, uuids []string) ([]*datamodel.ActiveDirectory, error) {
	return getMultipleActiveDirectoriesByUUIDs(d.db.GORM().Unscoped().WithContext(ctx), uuids)
}

func listActiveDirectories(db *gorm.DB, accountID int64) ([]*datamodel.ActiveDirectory, error) {
	var ads []*datamodel.ActiveDirectory
	err := db.Where("account_id = ? AND deleted_at IS NULL", accountID).Find(&ads).Error
	if err != nil {
		return nil, err
	}
	return ads, nil
}

func getMultipleActiveDirectoriesByUUIDs(db *gorm.DB, uuids []string) ([]*datamodel.ActiveDirectory, error) {
	var ads []*datamodel.ActiveDirectory
	err := db.Where("uuid IN ?", uuids).Find(&ads).Error
	if err != nil {
		return nil, err
	}
	return ads, nil
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
