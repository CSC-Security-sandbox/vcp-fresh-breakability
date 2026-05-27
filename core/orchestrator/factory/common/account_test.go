package common

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"gorm.io/gorm"
)

func TestGetAccount(t *testing.T) {
	tests := []struct {
		name          string
		accountName   string
		mockAccount   *datamodel.Account
		mockError     error
		expectedError bool
	}{
		{
			name:        "Successfully retrieves account",
			accountName: "test-account",
			mockAccount: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-uuid",
				},
				Name:  "test-account",
				State: models.AccountStateEnabled,
			},
			mockError:     nil,
			expectedError: false,
		},
		{
			name:          "Returns error when account not found",
			accountName:   "non-existent-account",
			mockAccount:   nil,
			mockError:     errors.New("account not found"),
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockStorage := database.NewMockStorage(t)
			mockStorage.EXPECT().
				GetAccount(ctx, tt.accountName).
				Return(tt.mockAccount, tt.mockError)

			result, err := GetAccount(ctx, mockStorage, tt.accountName)

			if tt.expectedError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, tt.mockAccount.Name, result.Name)
				assert.Equal(t, tt.mockAccount.UUID, result.UUID)
			}
		})
	}
}

func TestCreateAccount(t *testing.T) {
	tests := []struct {
		name          string
		accountName   string
		mockAccount   *datamodel.Account
		mockError     error
		expectedError bool
	}{
		{
			name:        "Successfully creates account",
			accountName: "new-account",
			mockAccount: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "new-uuid",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				Name:  "new-account",
				State: models.AccountStateEnabled,
			},
			mockError:     nil,
			expectedError: false,
		},
		{
			name:          "Returns error when account creation fails",
			accountName:   "new-account",
			mockAccount:   nil,
			mockError:     errors.New("database error"),
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockStorage := database.NewMockStorage(t)

			mockStorage.EXPECT().
				CreateAccount(ctx, mock.MatchedBy(func(account *datamodel.Account) bool {
					return account.Name == tt.accountName &&
						account.State == models.AccountStateEnabled &&
						account.UUID != ""
				})).
				Return(tt.mockAccount, tt.mockError)

			result, err := CreateAccount(ctx, mockStorage, tt.accountName)

			if tt.expectedError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, tt.accountName, result.Name)
				assert.Equal(t, models.AccountStateEnabled, result.State)
				assert.NotEmpty(t, result.UUID)
			}
		})
	}
}

func TestGetOrCreateAccount(t *testing.T) {
	tests := []struct {
		name                   string
		accountName            string
		getAccountResult       *datamodel.Account
		getAccountError        error
		createAccountResult    *datamodel.Account
		createAccountError     error
		secondGetAccountResult *datamodel.Account
		secondGetAccountError  error
		expectedError          bool
		expectedAccount        *datamodel.Account
		scenario               string
	}{
		{
			name:        "Account exists and is enabled",
			accountName: "existing-account",
			getAccountResult: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "existing-uuid"},
				Name:      "existing-account",
				State:     models.AccountStateEnabled,
			},
			getAccountError: nil,
			expectedError:   false,
			expectedAccount: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "existing-uuid"},
				Name:      "existing-account",
				State:     models.AccountStateEnabled,
			},
			scenario: "exists_enabled",
		},
		{
			name:        "Account exists but is disabled",
			accountName: "disabled-account",
			getAccountResult: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "disabled-uuid"},
				Name:      "disabled-account",
				State:     models.AccountStateDisabled,
			},
			getAccountError: nil,
			expectedError:   true,
			expectedAccount: nil,
			scenario:        "exists_disabled",
		},
		{
			name:        "Account exists but is deleted",
			accountName: "deleted-account",
			getAccountResult: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "deleted-uuid",
					DeletedAt: &gorm.DeletedAt{Time: time.Now(), Valid: true},
				},
				Name:  "deleted-account",
				State: models.AccountStateEnabled,
			},
			getAccountError: nil,
			expectedError:   true,
			expectedAccount: nil,
			scenario:        "exists_deleted",
		},
		{
			name:             "Account does not exist, create succeeds",
			accountName:      "new-account",
			getAccountResult: nil,
			getAccountError:  errors.New("account not found"),
			createAccountResult: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 2, UUID: "new-uuid"},
				Name:      "new-account",
				State:     models.AccountStateEnabled,
			},
			createAccountError: nil,
			expectedError:      false,
			expectedAccount: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 2, UUID: "new-uuid"},
				Name:      "new-account",
				State:     models.AccountStateEnabled,
			},
			scenario: "create_success",
		},
		{
			name:                "Account does not exist, create fails but get succeeds (race condition)",
			accountName:         "race-account",
			getAccountResult:    nil,
			getAccountError:     errors.New("account not found"),
			createAccountResult: nil,
			createAccountError:  errors.New("duplicate key"),
			secondGetAccountResult: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 3, UUID: "race-uuid"},
				Name:      "race-account",
				State:     models.AccountStateEnabled,
			},
			secondGetAccountError: nil,
			expectedError:         false,
			expectedAccount: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 3, UUID: "race-uuid"},
				Name:      "race-account",
				State:     models.AccountStateEnabled,
			},
			scenario: "create_race_condition",
		},
		{
			name:                   "Account does not exist, create fails and get also fails",
			accountName:            "fail-account",
			getAccountResult:       nil,
			getAccountError:        errors.New("account not found"),
			createAccountResult:    nil,
			createAccountError:     errors.New("database error"),
			secondGetAccountResult: nil,
			secondGetAccountError:  errors.New("account not found"),
			expectedError:          true,
			expectedAccount:        nil,
			scenario:               "create_and_get_fail",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockStorage := database.NewMockStorage(t)

			// Setup first GetAccount mock (always called first in GetOrCreateAccount)
			mockStorage.EXPECT().
				GetAccount(ctx, tt.accountName).
				Return(tt.getAccountResult, tt.getAccountError).
				Once()

			// Setup CreateAccount mock if first GetAccount fails
			if tt.getAccountError != nil {
				mockStorage.EXPECT().
					CreateAccount(ctx, mock.MatchedBy(func(account *datamodel.Account) bool {
						return account.Name == tt.accountName &&
							account.State == models.AccountStateEnabled
					})).
					Return(tt.createAccountResult, tt.createAccountError).
					Once()

				// Setup second GetAccount mock if create fails (race condition)
				if tt.createAccountError != nil {
					mockStorage.EXPECT().
						GetAccount(ctx, tt.accountName).
						Return(tt.secondGetAccountResult, tt.secondGetAccountError).
						Once()
				}
			}

			result, err := GetOrCreateAccount(ctx, mockStorage, tt.accountName)

			if tt.expectedError {
				assert.Error(t, err)
				assert.Nil(t, result)
				if tt.scenario == "exists_disabled" || tt.scenario == "exists_deleted" {
					assert.Contains(t, err.Error(), "account is disabled")
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				if tt.expectedAccount != nil {
					assert.Equal(t, tt.expectedAccount.Name, result.Name)
					assert.Equal(t, tt.expectedAccount.State, result.State)
				}
			}
		})
	}
}
