package database

import (
	"context"
	"errors"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

// GetAccount retrieves an account by its name
func (d *DataStoreRepository) GetAccount(ctx context.Context, name string) (*datamodel.Account, error) {
	return getAccount(d.db.GORM().WithContext(ctx), &datamodel.Account{Name: name})
}

// GetAccount retrieves an account by its uuid
func (d *DataStoreRepository) GetAccountByUUID(ctx context.Context, uuid string) (*datamodel.Account, error) {
	return getAccount(d.db.GORM().WithContext(ctx), &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: uuid}})
}

// CreateAccount creates a new account in the database
func (d *DataStoreRepository) CreateAccount(ctx context.Context, account *datamodel.Account) (*datamodel.Account, error) {
	return createAccount(d.db.GORM().WithContext(ctx), account)
}

// GetAccounts retrieves a list of accounts with pagination support
func (d *DataStoreRepository) GetAccounts(ctx context.Context, includeDelete bool, pagination *dbutils.Pagination) ([]*datamodel.Account, error) {
	db := d.db.GORM().WithContext(ctx)
	if includeDelete {
		db = d.db.GORM().Unscoped().WithContext(ctx)
	}
	return getAccounts(db, pagination)
}

// getAccount retrieves an account by the query
func getAccount(db *gorm.DB, query *datamodel.Account) (*datamodel.Account, error) {
	account := &datamodel.Account{}
	err := db.First(&account, query).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrAccountNotFound, customerrors.NewNotFoundErr("account", nil))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return account, nil
}

// createAccount creates a new account in the database
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

func getAccounts(db *gorm.DB, pagination *dbutils.Pagination) ([]*datamodel.Account, error) {
	var accounts []*datamodel.Account
	err := db.Scopes(dbutils.Paginate(pagination)).Find(&accounts).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return accounts, nil
}
