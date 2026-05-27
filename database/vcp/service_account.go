package database

import (
	"context"
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

var (
	listKmsServiceAccounts = _listKmsServiceAccounts
	deleteServiceAccount   = _deleteServiceAccount
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

func (d *DataStoreRepository) DeleteServiceAccount(ctx context.Context, serviceAccount *datamodel.ServiceAccount) error {
	return deleteServiceAccount(d.db.GORM().WithContext(ctx), serviceAccount)
}

func _deleteServiceAccount(db *gorm.DB, serviceAccount *datamodel.ServiceAccount) error {
	serviceAccount.UpdatedAt = utils.GetTimeNow()
	serviceAccount.State = datamodel.LifeCycleStateDisabled
	serviceAccount.StateDetails = datamodel.LifeCycleStateDisabledDetails

	return db.Save(serviceAccount).Error
}

// AddKeyToServiceAccount adds a new key to the service account's keys array
func (d *DataStoreRepository) AddKeyToServiceAccount(ctx context.Context, serviceAccountUUID string, key datamodel.ServiceAccountKey) error {
	db := d.db.GORM().WithContext(ctx)
	serviceAccount := &datamodel.ServiceAccount{}
	err := db.Where("uuid = ?", serviceAccountUUID).First(serviceAccount).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	// Initialize attributes if nil
	if serviceAccount.ServiceAccountAttributes == nil {
		serviceAccount.ServiceAccountAttributes = &datamodel.ServiceAccountAttributes{
			Keys: []datamodel.ServiceAccountKey{},
		}
	}

	// Add the new key
	serviceAccount.AddKey(key)
	serviceAccount.UpdatedAt = utils.GetTimeNow()

	return db.Save(serviceAccount).Error
}

// RemoveKeyFromServiceAccount removes a key from the service account's keys array
func (d *DataStoreRepository) RemoveKeyFromServiceAccount(ctx context.Context, serviceAccountUUID string, keyID string) error {
	db := d.db.GORM().WithContext(ctx)
	serviceAccount := &datamodel.ServiceAccount{}
	err := db.Where("uuid = ?", serviceAccountUUID).First(serviceAccount).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	if !serviceAccount.RemoveKey(keyID) {
		return vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("key not found"))
	}

	serviceAccount.UpdatedAt = utils.GetTimeNow()
	return db.Save(serviceAccount).Error
}

// MarkKeyForDeletion marks a key for deletion by setting IsPrimary=false and IsActive=false
// This soft-deletes the key so that DeleteOldSAKeyFromGCPActivity can find and delete it from GCP
func (d *DataStoreRepository) MarkKeyForDeletion(ctx context.Context, serviceAccountUUID string, keyID string) error {
	db := d.db.GORM().WithContext(ctx)
	serviceAccount := &datamodel.ServiceAccount{}
	err := db.Where("uuid = ?", serviceAccountUUID).First(serviceAccount).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	if serviceAccount.ServiceAccountAttributes == nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("key not found"))
	}

	// Find and mark the key for deletion
	found := false
	for i := range serviceAccount.ServiceAccountAttributes.Keys {
		if serviceAccount.ServiceAccountAttributes.Keys[i].KeyID == keyID {
			serviceAccount.ServiceAccountAttributes.Keys[i].IsPrimary = false
			serviceAccount.ServiceAccountAttributes.Keys[i].IsActive = false
			found = true
			break
		}
	}

	if !found {
		return vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("key not found"))
	}

	serviceAccount.UpdatedAt = utils.GetTimeNow()
	return db.Save(serviceAccount).Error
}

// UpdateServiceAccountPasswordLocation sets the service account's password location to already-encrypted key data (e.g. KMS-encrypted).
// Use this when the key data is already encrypted; do not use UpdateServiceAccountEmailAndKey which re-encrypts plaintext.
func (d *DataStoreRepository) UpdateServiceAccountPasswordLocation(ctx context.Context, serviceAccountUUID string, encryptedKeyData string) error {
	db := d.db.GORM().WithContext(ctx)
	dbServiceAccount := &datamodel.ServiceAccount{}
	err := db.Where("uuid = ?", serviceAccountUUID).First(dbServiceAccount).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	dbServiceAccount.ServiceAccountPasswordLocation = encryptedKeyData
	dbServiceAccount.UpdatedAt = utils.GetTimeNow()
	return db.Where("uuid = ?", serviceAccountUUID).Updates(dbServiceAccount).Error
}

// SetPrimaryKeyForServiceAccount sets a key as primary and updates ServiceAccountPasswordLocation
func (d *DataStoreRepository) SetPrimaryKeyForServiceAccount(ctx context.Context, serviceAccountUUID string, keyID string) error {
	db := d.db.GORM().WithContext(ctx)
	serviceAccount := &datamodel.ServiceAccount{}
	err := db.Where("uuid = ?", serviceAccountUUID).First(serviceAccount).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	key := serviceAccount.GetKeyByID(keyID)
	if key == nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("key not found"))
	}

	// Set as primary in the keys array
	serviceAccount.SetPrimaryKey(keyID)

	// Update ServiceAccountPasswordLocation to point to the new primary key
	serviceAccount.ServiceAccountPasswordLocation = key.KeyData
	serviceAccount.UpdatedAt = utils.GetTimeNow()

	return db.Save(serviceAccount).Error
}

// GetServiceAccountWithKeys retrieves service account with all keys loaded
func (d *DataStoreRepository) GetServiceAccountWithKeys(ctx context.Context, serviceAccountUUID string) (*datamodel.ServiceAccount, error) {
	db := d.db.GORM().WithContext(ctx)
	serviceAccount := &datamodel.ServiceAccount{}
	err := db.Where("uuid = ?", serviceAccountUUID).First(serviceAccount).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return serviceAccount, nil
}
