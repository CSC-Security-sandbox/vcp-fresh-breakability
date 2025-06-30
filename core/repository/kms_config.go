package repository

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

const (
	KmsSaStateDisabled           = "disable"
	KmsSaStateEnable             = "enable"
	RetryTimeOutForGetCryptoKey  = 1 * time.Minute
	RetryIntervalForGetCryptoKey = 5 * time.Second
)

var (
	getKmsConfig              = _getKmsConfig
	listKmsConfigByAccountID  = _listKmsConfigByAccountID
	getTimeNow                = utils.GetTimeNow
	createKmsServiceAccount   = _createKmsServiceAccount
	updateKmsConfigAttributes = _updateKmsConfigAttributes
	updateKmsConfigDetails    = _updateKmsConfigDetails
	getKmsConfigByUUID        = _getKmsConfigByUUID
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

// GetMultipleKmsConfigs retrieves multiple KMS configurations based on the provided conditions
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

// CreateKmsConfig creates the KMS configuration along with the service account
func (d *DataStoreRepository) CreateKmsConfig(ctx context.Context, kmsConfig *datamodel.KmsConfig) (*datamodel.KmsConfig, error) {
	db := d.db.GORM().WithContext(ctx)
	dbKmsConfigs, err := listKmsConfigByAccountID(db, kmsConfig.AccountID)
	if err != nil {
		return nil, err
	}
	for _, dbKmsConfig := range dbKmsConfigs {
		switch dbKmsConfig.State {
		case models.LifeCycleStateCreating:
			return nil, errors.NewConflictErr("another config create operation is in progress for this region and project")
		case models.LifeCycleStateDeleting:
			return nil, errors.NewConflictErr("another config delete operation is in progress for this region and project")
		case models.LifeCycleStateAvailable:
			return dbKmsConfig, nil
		}
	}
	dbServiceAccount, err := createKmsServiceAccount(db, kmsConfig)
	if err != nil {
		return nil, err
	}
	kmsConfig.ServiceAccountID = dbServiceAccount.ID
	err = db.Save(kmsConfig).Error
	if err != nil {
		return nil, err
	}

	err = db.Preload("ServiceAccount").Preload("Account").Where("uuid = ?", kmsConfig.UUID).First(kmsConfig).Error
	return kmsConfig, err
}

func _createKmsServiceAccount(db *gorm.DB, kmsConfig *datamodel.KmsConfig) (*datamodel.ServiceAccount, error) {
	serviceAccount := &datamodel.ServiceAccount{}
	err := db.First(serviceAccount, &datamodel.ServiceAccount{AccountID: kmsConfig.AccountID,
		State: KmsSaStateEnable}).Error
	if err != nil {
		if err.Error() != "record not found" {
			return nil, err
		}
		serviceAccount.UUID = utils.RandomUUID()
		serviceAccount.CreatedAt = getTimeNow()
		serviceAccount.UpdatedAt = getTimeNow()
		serviceAccount.Name = kmsConfig.Name
		serviceAccount.Description = kmsConfig.Description
		serviceAccount.State = kmsConfig.State
		serviceAccount.StateDetails = kmsConfig.StateDetails
		serviceAccount.AccountID = kmsConfig.AccountID
		if err = db.Create(serviceAccount).Error; err != nil {
			return nil, err
		}
		err = db.First(serviceAccount).Error
		return serviceAccount, err
	}
	return serviceAccount, err
}

// ListKmsConfigByAccountID retrieves all KMS configurations for a given account ID
func (d *DataStoreRepository) ListKmsConfigByAccountID(ctx context.Context, accountID int64) ([]*datamodel.KmsConfig, error) {
	return listKmsConfigByAccountID(d.db.GORM().WithContext(ctx), accountID)
}

func _listKmsConfigByAccountID(db *gorm.DB, accountID int64) ([]*datamodel.KmsConfig, error) {
	var kmsConfigs []*datamodel.KmsConfig
	err := db.Preload("ServiceAccount").Preload("Account").Where("account_id = ?", accountID).Find(&kmsConfigs).Error
	if err != nil {
		return nil, err
	}
	return kmsConfigs, nil
}

// GetKmsConfigByUUID retrieves a KMS configuration by its UUID
func (d *DataStoreRepository) GetKmsConfigByUUID(ctx context.Context, uuid string) (*datamodel.KmsConfig, error) {
	return getKmsConfigByUUID(d.db.GORM().WithContext(ctx), uuid)
}

func _getKmsConfigByUUID(db *gorm.DB, uuid string) (*datamodel.KmsConfig, error) {
	kmsConfig := &datamodel.KmsConfig{}
	err := db.Preload("ServiceAccount").Preload("Account").First(kmsConfig, &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: uuid}}).Error
	if err != nil {
		if err.Error() == "record not found" {
			return nil, errors.NewNotFoundErr(err.Error(), nil)
		}
		return nil, err
	}
	return kmsConfig, nil
}

func _getKmsConfig(db *gorm.DB, query *datamodel.KmsConfig) (*datamodel.KmsConfig, error) {
	kmsConfig := &datamodel.KmsConfig{}
	err := db.Preload("Account").Preload("ServiceAccount").First(&kmsConfig, query).Error
	if err != nil {
		return nil, errors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "KMS Configuration", nil)
	}
	return kmsConfig, nil
}

// UpdateKmsConfigAttributes updates the attributes of a KMS configuration in the database
func (d *DataStoreRepository) UpdateKmsConfigAttributes(ctx context.Context, uuid string, attributes *datamodel.KmsAttributes) (*datamodel.KmsConfig, error) {
	var updatedKmsConfig *datamodel.KmsConfig
	err := d.db.GORM().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error

		updatedKmsConfig, err = updateKmsConfigAttributes(tx, uuid, attributes)
		return err
	})
	if err != nil {
		return nil, err
	}
	return updatedKmsConfig, nil
}

func _updateKmsConfigAttributes(db *gorm.DB, uuid string, attributes *datamodel.KmsAttributes) (*datamodel.KmsConfig, error) {
	dbKmsConfig := &datamodel.KmsConfig{}
	err := db.Preload("ServiceAccount").Preload("Account").Where("uuid = ?", uuid).First(dbKmsConfig).Error
	if err != nil {
		return nil, err
	}
	dbKmsConfig.UpdatedAt = time.Now()
	dbKmsConfig.KmsAttributes = attributes
	err = db.Where("uuid = ?", uuid).Updates(dbKmsConfig).Error
	if err != nil {
		return nil, err
	}
	return dbKmsConfig, nil
}

// GetJobByResourceUUID retrieves the job associated with a KMS configuration by its UUID
func (d *DataStoreRepository) GetJobByResourceUUID(ctx context.Context, resourceUUID string) (*datamodel.Job, error) {
	job := &datamodel.Job{}
	err := d.db.GORM().WithContext(ctx).Where("job_attributes ->> 'resource_uuid' = ?", resourceUUID).First(job).Error
	if err != nil {
		return nil, err
	}
	return job, nil
}

// UpdateKmsConfigDetails updates the KMS configuration details such as key full path and resource ID
func (d *DataStoreRepository) UpdateKmsConfigDetails(ctx context.Context, uuid string, keyFullPath string, resourceID string) (*datamodel.KmsConfig, error) {
	var updatedKmsConfig *datamodel.KmsConfig
	err := d.db.GORM().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error

		updatedKmsConfig, err = updateKmsConfigDetails(tx, uuid, keyFullPath, resourceID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return updatedKmsConfig, nil
}

func _updateKmsConfigDetails(db *gorm.DB, uuid string, keyFullPath string, resourceID string) (*datamodel.KmsConfig, error) {
	parsedKeyFullPath, err := utils.ParseKeyFullPathResource(keyFullPath)
	if err != nil {
		return nil, err
	}
	dbKmsConfig := &datamodel.KmsConfig{}
	err = db.Preload("ServiceAccount").Preload("Account").Where("uuid = ?", uuid).First(dbKmsConfig).Error
	if err != nil {
		return nil, err
	}
	dbKmsConfig.UpdatedAt = time.Now()
	dbKmsConfig.KeyRingLocation = parsedKeyFullPath.Location
	dbKmsConfig.KeyRing = parsedKeyFullPath.KeyRing
	dbKmsConfig.KeyName = parsedKeyFullPath.CryptoKey
	dbKmsConfig.CustomerProjectID = parsedKeyFullPath.ProjectID
	dbKmsConfig.ResourceID = resourceID
	err = db.Where("uuid = ?", uuid).Updates(dbKmsConfig).Error
	if err != nil {
		return nil, err
	}
	return dbKmsConfig, nil
}

// GetKmsConfigByKeyFullPath retrieves a KMS configuration by its full key path
func (d *DataStoreRepository) GetKmsConfigByKeyFullPath(ctx context.Context, keyFullPath string) (*datamodel.KmsConfig, error) {
	parsedKeyFullPath, err := utils.ParseKeyFullPathResource(keyFullPath)
	if err != nil {
		return nil, err
	}
	kmsConfig := &datamodel.KmsConfig{}
	err = d.db.GORM().WithContext(ctx).Preload("ServiceAccount").Preload("Account").First(kmsConfig, &datamodel.KmsConfig{
		KeyRingLocation:   parsedKeyFullPath.Location,
		KeyRing:           parsedKeyFullPath.KeyRing,
		KeyName:           parsedKeyFullPath.CryptoKey,
		CustomerProjectID: parsedKeyFullPath.ProjectID,
	}).Error
	if err != nil {
		return nil, errors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "KMS Configuration", nil)
	}
	return kmsConfig, nil
}
