package orchestrator

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

var (
	createAccount      = _createAccount
	getOrCreateAccount = _getOrCreateAccount
	getAccountWithName = _getAccountWithName
)

// Returns an account if exists. Creates a new account if it does not exist.
func _getOrCreateAccount(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
	account, err := getAccountWithName(ctx, se, accountName)
	if err == nil {
		if account.DeletedAt != nil || account.State == models.AccountStateDisabled {
			// Resurrect account
			return nil, errors.New("account is disabled")
		}
		return account, nil
	}

	account, err = createAccount(ctx, se, accountName)
	if err != nil {
		// Get account in create account race scenario
		return getAccountWithName(ctx, se, accountName)
	}
	return account, nil
}

// Creates a new account.
func _createAccount(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
	var tags string
	createdDate := time.Now()
	dbAccount := &datamodel.Account{
		BaseModel: datamodel.BaseModel{
			UUID:      utils.RandomUUID(),
			CreatedAt: createdDate,
			UpdatedAt: createdDate,
		},
		Name:  accountName,
		State: models.AccountStateEnabled,
		Tags:  tags,
	}

	createdAccount, err := se.CreateAccount(ctx, dbAccount)
	if err != nil {
		return nil, err
	}
	return createdAccount, nil
}

// Return an account with filtering of account Name.
func _getAccountWithName(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
	account, err := se.GetAccount(ctx, accountName)
	if err != nil {
		return nil, err
	}
	return account, nil
}

func (o *Orchestrator) GetAccount(ctx context.Context, accountName string) (*datamodel.Account, error) {
	se := o.storage
	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}
	if account.DeletedAt != nil || account.State == models.AccountStateDisabled {
		return nil, customerrors.NewNotFoundErr("account not found or disabled", nil)
	}
	return account, nil
}
