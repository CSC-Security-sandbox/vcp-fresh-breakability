package database

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
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

func (d *DataStoreRepository) DeleteActiveDirectory(ctx context.Context, uuid string) error {
	return deleteActiveDirectory(d.db.GORM().WithContext(ctx), uuid)
}

func deleteActiveDirectory(db *gorm.DB, uuid string) error {
	// First, get the Active Directory to update its state
	var ad datamodel.ActiveDirectory
	err := db.Where("uuid = ?", uuid).First(&ad).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Already deleted, consider it a success
			return nil
		}
		return err
	}

	// Update the state to Deleted before soft deleting
	ad.State = models.LifeCycleStateDeleted
	ad.StateDetails = models.LifeCycleStateDeletedDetails
	ad.Username = ""
	ad.CredentialPath = ""
	ad.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}

	// Update the fields after deletion
	err = db.Model(&datamodel.ActiveDirectory{}).Where("uuid = ?", uuid).Updates(map[string]interface{}{
		"deleted_at":      ad.DeletedAt,
		"state":           ad.State,
		"state_details":   ad.StateDetails,
		"username":        ad.Username,
		"credential_path": ad.CredentialPath, // Clear sensitive info
	}).Error
	if err != nil {
		return err
	}

	return nil
}

func (d *DataStoreRepository) GetSVMsUsingActiveDirectory(ctx context.Context, adId int64) ([]*datamodel.Svm, error) {
	return getSVMsUsingActiveDirectory(d.db.GORM().WithContext(ctx), adId)
}

func getSVMsUsingActiveDirectory(db *gorm.DB, adId int64) ([]*datamodel.Svm, error) {
	var svms []*datamodel.Svm
	// Query SVMs where the active_directory_id field matches the given adId
	err := db.Where("active_directory_id = ?", adId).Find(&svms).Error
	if err != nil {
		return nil, err
	}
	return svms, nil
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

func (d *DataStoreRepository) UpdateActiveDirectory(ctx context.Context, ad *datamodel.ActiveDirectory) (*datamodel.ActiveDirectory, error) {
	return updateActiveDirectory(d.db.GORM().WithContext(ctx), ad)
}

func updateActiveDirectory(db *gorm.DB, ad *datamodel.ActiveDirectory) (*datamodel.ActiveDirectory, error) {
	if ad == nil {
		return nil, errors.New("Active Directory is nil")
	}
	ad.UpdatedAt = time.Now()
	result := db.Model(&datamodel.ActiveDirectory{}).Where("id = ?", ad.ID).Updates(ad)
	if result.Error != nil {
		return nil, errors.New(result.Error.Error())
	}
	if result.RowsAffected == 0 {
		return nil, customerrors.NewNotFoundErr("active_directory", nil)
	}
	return ad, nil
}
