package database

import (
	"context"
	"errors"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

func (d *DataStoreRepository) GetSoftDeleteAccount(ctx context.Context, name string) (*datamodel.Account, error) {
	return getSoftDeleteAccount(d.db.GORM().WithContext(ctx), name)
}

func (d *DataStoreRepository) GetDeletedAccounts(ctx context.Context) ([]*datamodel.Account, error) {
	return getDeletedAccounts(d.db.GORM().WithContext(ctx))
}

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

func getSoftDeleteAccount(db *gorm.DB, name string) (*datamodel.Account, error) {
	accountSoftDelete := datamodel.Account{}
	err := db.Unscoped().Where("name = ?", name).First(&accountSoftDelete).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrAccountNotFound, customerrors.NewNotFoundErr("account", nil))
	}
	return &accountSoftDelete, nil
}

func getDeletedAccounts(db *gorm.DB) ([]*datamodel.Account, error) {
	accounts := []datamodel.Account{}
	err := db.Unscoped().Where("state = ?", models.AccountStateDeleted).Find(&accounts).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	result := make([]*datamodel.Account, 0, len(accounts))
	for i := range accounts {
		result = append(result, &accounts[i])
	}
	return result, nil
}

func getAccounts(db *gorm.DB, pagination *dbutils.Pagination) ([]*datamodel.Account, error) {
	var accounts []*datamodel.Account
	err := db.Scopes(dbutils.Paginate(pagination)).Find(&accounts).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return accounts, nil
}

// UpdateAccountStateForHandleResource updates the account state for handleResource flow
func (d *DataStoreRepository) UpdateAccountStateForHandleResource(ctx context.Context, accountUUID string, newState string) error {
	db := d.db.GORM().WithContext(ctx)
	err := db.Model(&datamodel.Account{}).Where("uuid = ?", accountUUID).Update("state", newState).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseUpdateAccountState, err)
	}
	return nil
}

// UpdateAccountVolumeRefreshTimestamp updates the VolumeRefreshWorkflowLastCompletionAt timestamp in AccountMetadata
func (d *DataStoreRepository) UpdateAccountVolumeRefreshTimestamp(ctx context.Context, accountUUID string, completionTime time.Time) error {
	db := d.db.GORM().WithContext(ctx)

	// Fetch the account first to get or initialize AccountMetadata
	account, err := d.GetAccountByUUID(ctx, accountUUID)
	if err != nil {
		return err
	}

	// Initialize AccountMetadata if it doesn't exist
	if account.AccountMetadata == nil {
		account.AccountMetadata = &datamodel.AccountMetadata{}
	}

	// Update the timestamp
	account.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt = completionTime

	// Save the updated metadata
	err = db.Model(&datamodel.Account{}).Where("uuid = ?", accountUUID).Update("account_metadata", account.AccountMetadata).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return nil
}
