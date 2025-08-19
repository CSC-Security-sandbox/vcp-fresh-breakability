package database

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coreerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

const (
	KmsSaStateDisabled      = "disable"
	KmsSaStateEnable        = "enable"
	GcpKmsConfigHealthError = "specified key <key_name> in <key_ring> does not exist or service permissions are incorrect"
)

var (
	getKmsConfig              = _getKmsConfig
	listKmsConfigByAccountID  = _listKmsConfigByAccountID
	getTimeNow                = utils.GetTimeNow
	updateKmsConfigAttributes = _updateKmsConfigAttributes
	updateKmsConfigDetails    = _updateKmsConfigDetails
	getKmsConfigByUUID        = _getKmsConfigByUUID
	isKmsConfigInUse          = _isKmsConfigInUse
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
	kmsConfig.ServiceAccountID = nil
	err = db.Save(kmsConfig).Error
	if err != nil {
		return nil, err
	}

	err = db.Preload("ServiceAccount").Preload("Account").Where("uuid = ?", kmsConfig.UUID).First(kmsConfig).Error
	return kmsConfig, err
}

func (d *DataStoreRepository) CreateKmsServiceAccount(ctx context.Context, serviceAccount *datamodel.ServiceAccount) (*datamodel.ServiceAccount, error) {
	db := d.db.GORM().WithContext(ctx)
	err := db.First(serviceAccount, &datamodel.ServiceAccount{AccountID: serviceAccount.AccountID,
		State: KmsSaStateEnable}).Error
	if err != nil {
		if err.Error() != "record not found" {
			return nil, err
		}
		serviceAccount.UUID = utils.RandomUUID()
		serviceAccount.CreatedAt = getTimeNow()
		serviceAccount.UpdatedAt = getTimeNow()
		encKey, err := utils.EncryptPassword(log.Secret(serviceAccount.ServiceAccountPasswordLocation))
		if err != nil {
			return nil, err
		}
		serviceAccount.ServiceAccountPasswordLocation = *encKey
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
		if coreerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NewNotFoundErr("KMS Configuration", nil)
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
func (d *DataStoreRepository) GetJobByResourceUUID(ctx context.Context, resourceUUID string, jobType string) (*datamodel.Job, error) {
	job := &datamodel.Job{}
	query := d.db.GORM().WithContext(ctx).Where("job_attributes ->> 'resource_uuid' = ?", resourceUUID)

	// Add job type filter if provided
	if jobType != "" {
		query = query.Where("type = ?", jobType)
	}

	err := query.First(job).Error
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
	dbKmsConfig.KeyProjectID = parsedKeyFullPath.ProjectID
	dbKmsConfig.ResourceID = resourceID
	err = db.Where("uuid = ?", uuid).Updates(dbKmsConfig).Error
	if err != nil {
		return nil, err
	}
	return dbKmsConfig, nil
}

// GetKmsConfigByKeyFullPath retrieves a KMS configuration by its full key path
func (d *DataStoreRepository) GetKmsConfigByKeyFullPath(ctx context.Context, keyFullPath string, accountID int64) (*datamodel.KmsConfig, error) {
	parsedKeyFullPath, err := utils.ParseKeyFullPathResource(keyFullPath)
	if err != nil {
		return nil, err
	}
	kmsConfig := &datamodel.KmsConfig{}
	err = d.db.GORM().WithContext(ctx).Preload("ServiceAccount").Preload("Account").First(kmsConfig, &datamodel.KmsConfig{
		KeyRingLocation: parsedKeyFullPath.Location,
		KeyRing:         parsedKeyFullPath.KeyRing,
		KeyName:         parsedKeyFullPath.CryptoKey,
		KeyProjectID:    parsedKeyFullPath.ProjectID,
		AccountID:       accountID,
	}).Error
	if err != nil {
		return nil, errors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "KMS Configuration", nil)
	}
	return kmsConfig, nil
}

// DeleteKmsConfig deletes kms config based on UUID
func (d *DataStoreRepository) DeleteKmsConfig(ctx context.Context, kmsConfigUUID, state, stateDetails string) (*datamodel.KmsConfig, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	kmsConfig, err := getKmsConfig(tx, &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: kmsConfigUUID}})
	if err != nil {
		return nil, err
	}
	kmsConfig.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	kmsConfig.State = state
	kmsConfig.StateDetails = stateDetails
	err = tx.Save(kmsConfig).Error
	if err != nil {
		return nil, err
	}

	return kmsConfig, nil
}

func (d *DataStoreRepository) UpdateKmsConfig(ctx context.Context, kmsUUID string, updates map[string]interface{}) error {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)

	dbKmsConfig, err := _getKmsConfig(tx, &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: kmsUUID}})
	if err != nil {
		return err
	}

	err = tx.Model(&dbKmsConfig).Updates(updates).Error
	if err != nil {
		return err
	}
	return nil
}

func (d *DataStoreRepository) IsKmsConfigInUse(ctx context.Context, kmsConfigUUID string) (bool, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return false, err
	}
	kmsConfig, err := getKmsConfigByUUID(tx, kmsConfigUUID)
	if err != nil {
		return false, err
	}
	return isKmsConfigInUse(tx, kmsConfig)
}

func _isKmsConfigInUse(db *gorm.DB, kmsConfig *datamodel.KmsConfig) (bool, error) {
	svms, err := getSvmsByKmsConfigID(db, kmsConfig.ID)
	if err != nil && !errors.IsNotFoundErr(err) {
		return false, err
	}
	if len(svms) > 0 {
		return true, nil
	}
	return false, nil
}
