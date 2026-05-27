package database

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

// AccountTelemetryData contains only the fields needed for telemetry/bizops operations.
// This is an optimized struct that avoids fetching unnecessary columns from the accounts table.
type AccountTelemetryData struct {
	ID    int64  `gorm:"column:id"`
	Name  string `gorm:"column:name"`
	State string `gorm:"column:state"`
}

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

// ListAccountsForTelemetry retrieves accounts with only the fields required for telemetry/bizops operations.
// This is an optimized query that selects only id, name, and state columns.
// Parameters:
//   - ctx: context for the operation
//   - pagination: pagination parameters (offset and limit)
//
// Returns only active (non-deleted) accounts.
func (d *DataStoreRepository) ListAccountsForTelemetry(ctx context.Context, pagination *dbutils.Pagination) ([]*AccountTelemetryData, error) {
	db := d.db.GORM().WithContext(ctx)

	var accounts []*AccountTelemetryData
	query := db.Model(&datamodel.Account{}).
		Select("id, name, state")

	// Apply pagination if provided
	if pagination != nil {
		query = query.Offset(pagination.Offset).Limit(pagination.Limit)
	}

	err := query.Find(&accounts).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	return accounts, nil
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
	err := db.Unscoped().Where("state = ?", datamodel.AccountStateDeleted).Find(&accounts).Error
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

// UpdateAccountTrialMetadata updates trial-related fields in account_metadata.
func (d *DataStoreRepository) UpdateAccountTrialMetadata(ctx context.Context, account *datamodel.Account, trial *datamodel.AccountTrialMode) error {
	if trial == nil {
		return nil
	}
	if account == nil || account.UUID == "" {
		return vsaerrors.NewVCPError(vsaerrors.ErrAccountNotFound, customerrors.NewNotFoundErr("account", nil))
	}

	db := d.db.GORM().WithContext(ctx)

	if account.AccountMetadata == nil {
		account.AccountMetadata = &datamodel.AccountMetadata{}
	}

	trial.ApplyTo(account.AccountMetadata)

	err := db.Model(&datamodel.Account{}).Where("uuid = ?", account.UUID).Update("account_metadata", account.AccountMetadata).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}
