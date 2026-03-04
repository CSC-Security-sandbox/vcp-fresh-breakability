package common

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
)

// GetAccount returns an account with filtering by account name
func GetAccount(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
	account, err := se.GetAccount(ctx, accountName)
	if err != nil {
		return nil, err
	}
	return account, nil
}

// CreateAccount creates a new account
func CreateAccount(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
	createdDate := time.Now()
	dbAccount := &datamodel.Account{
		BaseModel: datamodel.BaseModel{
			UUID:      utils.RandomUUID(),
			CreatedAt: createdDate,
			UpdatedAt: createdDate,
		},
		Name:  accountName,
		State: models.AccountStateEnabled,
	}

	createdAccount, err := se.CreateAccount(ctx, dbAccount)
	if err != nil {
		return nil, err
	}
	return createdAccount, nil
}

// GetOrCreateAccount returns an account if exists, creates a new account if it does not exist
func GetOrCreateAccount(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
	account, err := GetAccount(ctx, se, accountName)
	if err == nil {
		if account.DeletedAt != nil || account.State == models.AccountStateDisabled {
			return nil, errors.New("account is disabled")
		}
		return account, nil
	}

	account, err = CreateAccount(ctx, se, accountName)
	if err != nil {
		// Get account in create account race scenario
		return GetAccount(ctx, se, accountName)
	}
	return account, nil
}
