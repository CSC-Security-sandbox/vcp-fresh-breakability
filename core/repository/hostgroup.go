package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

func (d *DataStoreRepository) GetHostGroup(ctx context.Context, hostGroupUUID string, accountID int64) (*datamodel.HostGroup, error) {
	return getHostGroupWithDetails(d.db.GORM().WithContext(ctx), &datamodel.HostGroup{BaseModel: datamodel.BaseModel{UUID: hostGroupUUID}, AccountID: accountID})
}

func getHostGroupWithDetails(db *gorm.DB, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error) {
	var dbHostGroup datamodel.HostGroup
	err := db.Where("uuid = ?", hostGroup.UUID).Preload("Account").First(&dbHostGroup).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "host group", &hostGroup.UUID)
	}
	return &dbHostGroup, nil
}

func (d *DataStoreRepository) CreateHostGroup(ctx context.Context, hostGroup *datamodel.HostGroup) (*datamodel.HostGroup, error) {
	db := d.db.GORM().WithContext(ctx)
	var dbHostGroup datamodel.HostGroup
	err := db.Where("name = ?", hostGroup.Name).Where("account_id = ?", hostGroup.AccountID).First(&dbHostGroup).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		hostGroup.UUID = utils.RandomUUID()
		hostGroup.CreatedAt = time.Now()
		hostGroup.UpdatedAt = hostGroup.CreatedAt

		hostGroup.State = models.LifeCycleStateREADY
		hostGroup.StateDetails = models.LifeCycleStateAvailableDetails
		err := db.Create(&hostGroup).Error
		if err != nil {
			return nil, err
		}

		dbHostGroup, err := getHostGroupWithDetails(db, &datamodel.HostGroup{BaseModel: datamodel.BaseModel{UUID: hostGroup.UUID}})
		if err != nil {
			return nil, err
		}
		return dbHostGroup, nil
	}
	return nil, customerrors.NewConflictErr("hostgroup already exists")
}

func (d *DataStoreRepository) GetMultipleHostGroups(ctx context.Context, hostGroupUUID []string, accountID int64) ([]*datamodel.HostGroup, error) {
	var hostGroup []*datamodel.HostGroup
	err := d.db.GORM().WithContext(ctx).Where("account_id = ? AND uuid in ?", accountID, hostGroupUUID).Find(&hostGroup).Error
	if err != nil {
		return nil, customerrors.ConvertToNotFoundErrIfContainsMessage(err, "record not found", "hostgroup", nil)
	}
	return hostGroup, nil
}
