package repository

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"gorm.io/gorm"
)

func (d *DataStoreRepository) GetAccount(ctx context.Context, name string) (*datamodel.Account, error) {
	return getAccount(d.db.GORM().WithContext(ctx), &datamodel.Account{Name: name})
}

func (d *DataStoreRepository) CreateAccount(ctx context.Context, account *datamodel.Account) (*datamodel.Account, error) {
	return createAccount(d.db.GORM().WithContext(ctx), account)
}

func getAccount(db *gorm.DB, query *datamodel.Account) (*datamodel.Account, error) {
	account := &datamodel.Account{}
	err := db.First(&account, query).Error
	if err != nil {
		return nil, err
	}

	return account, nil
}

func createAccount(db *gorm.DB, account *datamodel.Account) (*datamodel.Account, error) {
	err := db.Where("name = ?", account.Name).First(&account).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		account.CreatedAt = time.Now()
		account.UpdatedAt = account.CreatedAt
		err := db.Create(account).Error
		if err != nil {
			return nil, err
		}
		return account, nil
	}
	return nil, errors.New("account already exists")
}
