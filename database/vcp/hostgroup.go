package database

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	deleteHostGroup           = _deleteHostGroup
	getMultipleHostGroups     = _getMultipleHostGroups
	getHostGroupsByUUIDs      = _getHostGroupsByUUIDs
	updateHostGroupsState     = _updateHostGroupsState
	updateHostGroup           = _updateHostGroup
	getHostGroupWithDetails   = _getHostGroupWithDetails
	isHostGroupInUse          = _isHostGroupInUse
	listHostGroupsByAccountID = _ListHostGroupsByAccountID
)

func (d *DataStoreRepository) GetHostGroup(ctx context.Context, hostGroupUUID string, accountID int64) (*datamodel.HostGroup, error) {
	return getHostGroupWithDetails(d.db.GORM().WithContext(ctx), &datamodel.HostGroup{BaseModel: datamodel.BaseModel{UUID: hostGroupUUID}, AccountID: accountID})
}

func _getHostGroupWithDetails(db *gorm.DB, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error) {
	var dbHostGroup datamodel.HostGroup
	err := db.Where("uuid = ? AND account_id = ?", hostGroup.UUID, hostGroup.AccountID).Preload("Account").First(&dbHostGroup).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "host group", nil))
	}
	return &dbHostGroup, nil
}

func (d *DataStoreRepository) CreateHostGroup(ctx context.Context, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error) {
	db := d.db.GORM().WithContext(ctx)
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	logger := util.GetLogger(ctx)
	defer commitOrRollbackOnError(logger, tx, &err)
	var dbHostGroup datamodel.HostGroup
	err1 := tx.Where("name = ?", hostGroup.Name).Where("account_id = ?", hostGroup.AccountID).First(&dbHostGroup).Error
	if errors.Is(err1, gorm.ErrRecordNotFound) {
		hostGroup.UUID = utils.RandomUUID()
		hostGroup.CreatedAt = time.Now()
		hostGroup.UpdatedAt = hostGroup.CreatedAt

		hostGroup.State = datamodel.LifeCycleStateREADY
		hostGroup.StateDetails = datamodel.LifeCycleStateAvailableDetails
		err := tx.Create(&hostGroup).Error
		if err != nil {
			return nil, err
		}

		dbHostGroup, err := getHostGroupWithDetails(tx, &datamodel.HostGroup{BaseModel: datamodel.BaseModel{UUID: hostGroup.UUID}, AccountID: hostGroup.AccountID})
		if err != nil {
			return nil, err
		}
		return dbHostGroup, nil
	} else if err1 != nil {
		logger.Errorf("Error while checking if hostgroup exists: %v", err1)
		return nil, err1
	}
	return nil, customerrors.NewConflictErr("hostgroup already exists")
}

func (d *DataStoreRepository) GetMultipleHostGroups(ctx context.Context, hostGroupUUID []string, accountID int64) ([]*datamodel.HostGroup, error) {
	return getMultipleHostGroups(d.db.GORM().WithContext(ctx), hostGroupUUID, accountID)
}

func _getMultipleHostGroups(db *gorm.DB, hostGroupUUID []string, accountID int64) ([]*datamodel.HostGroup, error) {
	var dbHostGroups []*datamodel.HostGroup
	err := db.Where("uuid IN (?)", hostGroupUUID).Where("account_id = ?", accountID).Find(&dbHostGroups).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "host group", nil))
	}
	return dbHostGroups, nil
}

func (d *DataStoreRepository) GetHostGroupsByUUIDs(ctx context.Context, hostGroupUUIDs []string) ([]*datamodel.HostGroup, error) {
	return getHostGroupsByUUIDs(d.db.GORM().WithContext(ctx), hostGroupUUIDs)
}

func _getHostGroupsByUUIDs(db *gorm.DB, hostGroupUUIDs []string) ([]*datamodel.HostGroup, error) {
	var dbHostGroups []*datamodel.HostGroup
	err := db.Where("uuid IN (?)", hostGroupUUIDs).Preload("Account").Find(&dbHostGroups).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "host group", nil))
	}
	return dbHostGroups, nil
}

func (d *DataStoreRepository) DeleteHostGroup(ctx context.Context, hostGroupUUID string, accountID int64) (*datamodel.HostGroup, error) {
	return deleteHostGroup(ctx, d.db.GORM().WithContext(ctx), hostGroupUUID, accountID)
}

func _deleteHostGroup(ctx context.Context, db *gorm.DB, hostGroupUUID string, accountID int64) (*datamodel.HostGroup, error) {
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)
	hostGroup, err := getHostGroupWithDetails(db, &datamodel.HostGroup{BaseModel: datamodel.BaseModel{UUID: hostGroupUUID}, AccountID: accountID})
	if err != nil {
		return nil, err
	}

	inUse, err := isHostGroupInUse(tx, hostGroupUUID, accountID)
	if err != nil {
		return nil, err
	}

	if inUse {
		return nil, customerrors.NewUserInputValidationErr("host group is in use by one or more volumes")
	}

	err = tx.Model(&hostGroup).Updates(datamodel.HostGroup{
		State:        datamodel.LifeCycleStateDeleted,
		StateDetails: "",
		BaseModel: datamodel.BaseModel{
			DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true},
		},
	}).Error
	if err != nil {
		return nil, err
	}

	return hostGroup, nil
}

func (d *DataStoreRepository) UpdateHostGroupsState(ctx context.Context, hostGroupUUIDs []string, accountID int64, state, stateDetails string) error {
	return updateHostGroupsState(ctx, d.db.GORM().WithContext(ctx), hostGroupUUIDs, accountID, state, stateDetails)
}

func _updateHostGroupsState(ctx context.Context, db *gorm.DB, hostGroupUUIDs []string, accountID int64, state, stateDetails string) error {
	tx, err := startTransaction(db)
	if err != nil {
		return err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	dbHostGroup, err := getMultipleHostGroups(tx, hostGroupUUIDs, accountID)
	if err != nil {
		return err
	}

	for _, hostGroup := range dbHostGroup {
		err = tx.Model(&hostGroup).Updates(datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				UpdatedAt: time.Now(),
			},
			State:        state,
			StateDetails: stateDetails,
		}).Error
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DataStoreRepository) UpdateHostGroupsStateForHandleResource(ctx context.Context, hostGroupUUID string, accountID int64, state, stateDetails string) error {
	tx, err := startTransaction(d.db.GORM())
	if err != nil {
		return err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	dbHostGroup, err := getHostGroupWithDetails(tx, &datamodel.HostGroup{BaseModel: datamodel.BaseModel{UUID: hostGroupUUID}, AccountID: accountID})
	if err != nil {
		return err
	}

	err = tx.Model(&dbHostGroup).Updates(datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{
			UpdatedAt: time.Now(),
		},
		State:        state,
		StateDetails: stateDetails,
	}).Error
	if err != nil {
		return err
	}
	return nil
}

func _isHostGroupInUse(db *gorm.DB, hostGroupUUID string, accountID int64) (bool, error) {
	volumes, err := volumesWithHG(db, hostGroupUUID, accountID)
	if err != nil {
		return true, err
	}
	if len(volumes) == 0 {
		return false, nil
	}
	return true, nil
}

func (d *DataStoreRepository) UpdateHostGroup(ctx context.Context, hostGroupUUID string, accountID int64, description *string, Hosts *[]string) (*datamodel.HostGroup, error) {
	return updateHostGroup(ctx, d.db.GORM().WithContext(ctx), hostGroupUUID, accountID, description, Hosts)
}

func _updateHostGroup(ctx context.Context, db *gorm.DB, hostGroupUUID string, accountID int64, description *string, Hosts *[]string) (*datamodel.HostGroup, error) {
	tx, err := startTransaction(db)
	if err != nil {
		return nil, err
	}
	defer commitOrRollbackOnError(util.GetLogger(ctx), tx, &err)

	hostGroup, err := getHostGroupWithDetails(tx, &datamodel.HostGroup{BaseModel: datamodel.BaseModel{UUID: hostGroupUUID}, AccountID: accountID})
	if err != nil {
		return nil, err
	}

	if description != nil {
		hostGroup.Description = *description
	}

	if Hosts != nil {
		hostGroup.Hosts = datamodel.Hosts{Hosts: *Hosts}
	}

	hostGroup.UpdatedAt = time.Now()

	err = tx.Save(hostGroup).Error
	if err != nil {
		return nil, err
	}

	return hostGroup, nil
}

func (d *DataStoreRepository) ListHostGroupsByAccountID(ctx context.Context, accountID int64) ([]*datamodel.HostGroup, error) {
	return listHostGroupsByAccountID(d.db.GORM().WithContext(ctx), accountID)
}

func _ListHostGroupsByAccountID(db *gorm.DB, accountID int64) ([]*datamodel.HostGroup, error) {
	var dbHostGroups []*datamodel.HostGroup
	err := db.Where("account_id = ?", accountID).Find(&dbHostGroups).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "host group", nil))
	}
	return dbHostGroups, nil
}
