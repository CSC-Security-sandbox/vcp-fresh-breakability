package database

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"gorm.io/gorm"
)

func (d *DataStoreRepository) DeleteAccount(ctx context.Context, accountID int64) error {
	db := d.db.GORM().WithContext(ctx)
	var accountInfo datamodel.Account

	err := db.First(&accountInfo, accountID).Error
	if err != nil {
		return err
	}

	accountInfo.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	accountInfo.UpdatedAt = accountInfo.DeletedAt.Time
	accountInfo.State = datamodel.AccountStateDeleted
	accountInfo.StateDetails = datamodel.LifeCycleStateDeletedDetails

	err = db.Save(&accountInfo).Error
	if err != nil {
		return err
	}
	return nil
}

func (d *DataStoreRepository) HardDeleteResourceByTable(ctx context.Context, table string, query string, id int64) error {
	db := d.db.GORM().WithContext(ctx)

	var numEntries int64
	err := db.Unscoped().Table(table).Where(query, id).Count(&numEntries).Error
	if err != nil {
		return err
	}

	var ids []int64
	for numEntries > 0 {
		tx, err := startTransaction(db)
		if err != nil {
			return err
		}
		err = tx.Select("id").Table(table).Limit(100).Find(&ids, query, id).Error
		if err != nil {
			tx.Rollback()
			return err
		}

		if len(ids) > 0 {
			err = tx.Exec("DELETE FROM "+table+" WHERE id IN (?)", ids).Error
			if err != nil {
				tx.Rollback()
				return err
			}
		}

		err = tx.Commit().Error
		if err != nil {
			return err
		}

		numEntries -= int64(len(ids))
	}
	return err
}

func (d *DataStoreRepository) RollBackDeletedAccount(ctx context.Context, accountID int64) error {
	db := d.db.GORM().WithContext(ctx)
	var accountInfo datamodel.Account

	err := db.Unscoped().First(&accountInfo, accountID).Error
	if err != nil {
		return err
	}

	accountInfo.DeletedAt = nil
	accountInfo.UpdatedAt = time.Now()
	// TODO: State might be hyperscalerDisabled or something else
	accountInfo.State = datamodel.AccountStateHyperscalerDisabled
	accountInfo.StateDetails = datamodel.LifeCycleStateHyperscalerDisabledDetails

	err = db.Save(&accountInfo).Error
	if err != nil {
		return err
	}
	return nil
}
