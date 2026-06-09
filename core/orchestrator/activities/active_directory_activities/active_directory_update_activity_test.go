package active_directory_activities

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/active_directories"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/temporal"
)

var mockUpdatePasswordSecret = func(ctx context.Context, password string, secretID string) error {
	return nil
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_Success(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
		Username:          nillable.GetStringPtr("new-admin@test.local"),
		Password:          nillable.GetStringPtr("NewSecurePass123!"),
		DNS:               nillable.GetStringPtr("10.0.0.2"),
		SecurityOperators: []string{"new-security-user"},
		BackupOperators:   []string{"new-backup-user"},
		Administrators:    []string{"new-admin-user"},
	}

	ad := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName:                    "test-ad",
		Username:                  "admin@test.local",
		Domain:                    "test.local",
		DNS:                       "10.0.0.1",
		NetBIOS:                   "TEST",
		State:                     datamodel.LifeCycleStateREADY,
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{},
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		Username:  "admin@test.local",
		Domain:    "test.local",
		DNS:       "10.0.0.1",
		NetBIOS:   "TEST",
		State:     datamodel.LifeCycleStateREADY,
		AccountId: 123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         []string{"old-security-user"},
				utils.ActiveDirectoryGroupBuiltInBackupOperators: []string{"old-backup-user"},
				utils.ActiveDirectoryGroupBuiltInAdministrators:  []string{"old-admin-user"},
			},
		},
	}

	updatedRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:       "test-ad",
		Username:     "new-admin@test.local",
		Domain:       "test.local",
		DNS:          "10.0.0.2",
		NetBIOS:      "TEST",
		State:        datamodel.LifeCycleStateREADY,
		StateDetails: datamodel.LifeCycleStateReadyDetails,
		AccountId:    123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         []string{"new-security-user"},
				utils.ActiveDirectoryGroupBuiltInBackupOperators: []string{"new-backup-user"},
				utils.ActiveDirectoryGroupBuiltInAdministrators:  []string{"new-admin-user"},
			},
		},
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()

	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)

	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return ad.UUID == "test-ad-uuid" &&
			ad.Username == "new-admin@test.local" &&
			ad.DNS == "10.0.0.2" &&
			ad.State == datamodel.LifeCycleStateREADY &&
			ad.StateDetails == datamodel.LifeCycleStateReadyDetails
	})).Return(updatedRecord, nil)

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	// Mock password storage
	originalUpdatePassword := updatePasswordSecret
	updatePasswordSecret = mockUpdatePasswordSecret
	defer func() { updatePasswordSecret = originalUpdatePassword }()

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_Success_SetsInUseWhenSvmsExist(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
		DNS:               nillable.GetStringPtr("10.0.0.2"),
	}

	ad := &models.ActiveDirectory{
		BaseModel:                 models.BaseModel{UUID: "test-ad-uuid"},
		AdName:                    "test-ad",
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{},
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-ad-uuid"},
		AdName:    "test-ad",
		AccountId: 123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{},
		},
	}

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 123},
		Name:      "test-account",
	}

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)
	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return ad.State == datamodel.LifeCycleStateInUse &&
			ad.StateDetails == datamodel.LifeCycleStateInUseDetails
	})).Return(existingRecord, nil)

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id", datamodel.LifeCycleStateInUse, datamodel.LifeCycleStateInUseDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_PersistsPassedStateAndStateDetails(t *testing.T) {
	// Verifies that state and stateDetails passed into UpdateVcpActiveDirectory are the ones persisted to DB
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
		DNS:               nillable.GetStringPtr("10.0.0.2"),
	}

	ad := &models.ActiveDirectory{
		BaseModel:                 models.BaseModel{UUID: "test-ad-uuid"},
		AdName:                    "test-ad",
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{},
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-ad-uuid"},
		AdName:    "test-ad",
		AccountId: 123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{},
		},
	}

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 123},
		Name:      "test-account",
	}

	customState := "READY"
	customStateDetails := "No SVMs using this AD"

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil)
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)
	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(updated *datamodel.ActiveDirectory) bool {
		return updated.State == customState && updated.StateDetails == customStateDetails &&
			updated.ChangeId == "change-123"
	})).Return(existingRecord, nil)

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "change-123", customState, customStateDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_ADNotFound(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "non-existent-uuid",
		AccountId:         "123",
		Username:          nillable.GetStringPtr("new-admin@test.local"),
	}

	ad := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "non-existent-uuid",
		},
		AdName: "test-ad",
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()

	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(nil, nil)

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_PasswordStoreFailed(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
		Username:          nillable.GetStringPtr("new-admin@test.local"),
		Password:          nillable.GetStringPtr("NewSecurePass123!"),
	}

	ad := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName:                    "test-ad",
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{},
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:         "test-ad",
		AccountId:      123,
		CredentialPath: "secret-id-123",
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{},
		},
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()

	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil).Maybe()

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	// Mock password storage failure
	originalUpdatePassword := updatePasswordSecret
	updatePasswordSecret = func(ctx context.Context, password string, secretID string) error {
		return errors.New("failed to store password in secret manager")
	}
	defer func() { updatePasswordSecret = originalUpdatePassword }()

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to store password")
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_UpdateDBFailed(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
		DNS:               nillable.GetStringPtr("10.0.0.2"),
	}

	ad := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName:                    "test-ad",
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{},
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		AccountId: 123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{},
		},
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()

	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)

	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.Anything).
		Return(nil, vsaerrors.New("database update failed"))

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database update failed")
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_PartialUpdate(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
		DNS:               nillable.GetStringPtr("10.0.0.3"), // Only updating DNS
	}

	ad := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName:                    "test-ad",
		Username:                  "admin@test.local",
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{},
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		Username:  "admin@test.local",
		DNS:       "10.0.0.1",
		AccountId: 123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{},
		},
	}

	updatedRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:       "test-ad",
		Username:     "admin@test.local",
		DNS:          "10.0.0.3",
		State:        datamodel.LifeCycleStateREADY,
		StateDetails: datamodel.LifeCycleStateReadyDetails,
		AccountId:    123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{},
		},
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()

	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)

	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return ad.UUID == "test-ad-uuid" &&
			ad.Username == "admin@test.local" && // Username unchanged
			ad.DNS == "10.0.0.3" // DNS updated
	})).Return(updatedRecord, nil)

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_UpdateSecurityGroups(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
		SecurityOperators: []string{"security-user-1", "security-user-2"},
		BackupOperators:   []string{"backup-user-1"},
		Administrators:    []string{},
	}

	ad := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
			SecurityOperators: []string{"old-security"},
			BackupOperators:   []string{"old-backup"},
			Administrators:    []string{"old-admin"},
		},
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		AccountId: 123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         []string{"old-security"},
				utils.ActiveDirectoryGroupBuiltInBackupOperators: []string{"old-backup"},
				utils.ActiveDirectoryGroupBuiltInAdministrators:  []string{"old-admin"},
			},
		},
	}

	updatedRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:       "test-ad",
		State:        datamodel.LifeCycleStateREADY,
		StateDetails: datamodel.LifeCycleStateReadyDetails,
		AccountId:    123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         []string{"security-user-1", "security-user-2"},
				utils.ActiveDirectoryGroupBuiltInBackupOperators: []string{"backup-user-1"},
				utils.ActiveDirectoryGroupBuiltInAdministrators:  []string{},
			},
		},
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()

	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)

	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return len(ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectorySeSecurityPrivilege]) == 2 &&
			len(ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInBackupOperators]) == 1 &&
			ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInAdministrators] == nil
	})).Return(updatedRecord, nil)

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_UpdateToEmptyLists(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	// Test updating all three user groups to empty lists
	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
		SecurityOperators: []string{}, // Empty list
		BackupOperators:   []string{}, // Empty list
		Administrators:    []string{}, // Empty list
	}

	ad := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
			SecurityOperators: []string{"old-security"},
			BackupOperators:   []string{"old-backup"},
			Administrators:    []string{"old-admin"},
		},
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		AccountId: 123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         []string{"old-security"},
				utils.ActiveDirectoryGroupBuiltInBackupOperators: []string{"old-backup"},
				utils.ActiveDirectoryGroupBuiltInAdministrators:  []string{"old-admin"},
			},
		},
	}

	updatedRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:       "test-ad",
		State:        datamodel.LifeCycleStateREADY,
		StateDetails: datamodel.LifeCycleStateReadyDetails,
		AccountId:    123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         nil,
				utils.ActiveDirectoryGroupBuiltInBackupOperators: nil,
				utils.ActiveDirectoryGroupBuiltInAdministrators:  nil,
			},
		},
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()

	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)

	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		// Verify all three lists are nil (empty lists converted to nil)
		return ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectorySeSecurityPrivilege] == nil &&
			ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInBackupOperators] == nil &&
			ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInAdministrators] == nil
	})).Return(updatedRecord, nil)

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_UpdateOnlyOneToEmptyList(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	// Test updating only SecurityOperators to empty list, leave others unchanged
	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
		SecurityOperators: []string{}, // Empty list
		// BackupOperators and Administrators are nil (not being updated)
	}

	ad := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
			SecurityOperators: []string{"old-security"},
			BackupOperators:   []string{"old-backup"},
			Administrators:    []string{"old-admin"},
		},
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		AccountId: 123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         []string{"old-security"},
				utils.ActiveDirectoryGroupBuiltInBackupOperators: []string{"old-backup"},
				utils.ActiveDirectoryGroupBuiltInAdministrators:  []string{"old-admin"},
			},
		},
	}

	updatedRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:       "test-ad",
		State:        datamodel.LifeCycleStateREADY,
		StateDetails: datamodel.LifeCycleStateReadyDetails,
		AccountId:    123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         nil,
				utils.ActiveDirectoryGroupBuiltInBackupOperators: []string{"old-backup"},
				utils.ActiveDirectoryGroupBuiltInAdministrators:  []string{"old-admin"},
			},
		},
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()

	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)

	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		// Verify SecurityOperators is nil (empty list converted to nil), others remain unchanged
		return ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectorySeSecurityPrivilege] == nil &&
			len(ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInBackupOperators]) == 1 &&
			len(ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInAdministrators]) == 1
	})).Return(updatedRecord, nil)

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_MixedEmptyAndNonEmptyLists(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	// Test mixed scenario: empty list for SecurityOperators, non-empty for BackupOperators, empty for Administrators
	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
		SecurityOperators: []string{},                // Empty list -> should become nil
		BackupOperators:   []string{"backup-user-1"}, // Non-empty list -> should remain as array
		Administrators:    []string{},                // Empty list -> should become nil
	}

	ad := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
			SecurityOperators: []string{"old-security"},
			BackupOperators:   []string{"old-backup"},
			Administrators:    []string{"old-admin"},
		},
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		AccountId: 123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         []string{"old-security"},
				utils.ActiveDirectoryGroupBuiltInBackupOperators: []string{"old-backup"},
				utils.ActiveDirectoryGroupBuiltInAdministrators:  []string{"old-admin"},
			},
		},
	}

	updatedRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:       "test-ad",
		State:        datamodel.LifeCycleStateREADY,
		StateDetails: datamodel.LifeCycleStateReadyDetails,
		AccountId:    123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         nil,
				utils.ActiveDirectoryGroupBuiltInBackupOperators: []string{"backup-user-1"},
				utils.ActiveDirectoryGroupBuiltInAdministrators:  nil,
			},
		},
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()

	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)

	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		// Verify:
		// - SecurityOperators is nil (empty list converted to nil)
		// - BackupOperators has 1 element (non-empty list stays as array)
		// - Administrators is nil (empty list converted to nil)
		return ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectorySeSecurityPrivilege] == nil &&
			len(ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInBackupOperators]) == 1 &&
			ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInBackupOperators][0] == "backup-user-1" &&
			ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInAdministrators] == nil
	})).Return(updatedRecord, nil)

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_NilAttributes(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
		DNS:               nillable.GetStringPtr("10.0.0.2"),
	}

	ad := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName:                    "test-ad",
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{},
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		AccountId: 123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{},
		},
	}

	updatedRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-ad-uuid",
		},
		AdName:       "test-ad",
		DNS:          "10.0.0.2",
		State:        datamodel.LifeCycleStateREADY,
		StateDetails: datamodel.LifeCycleStateReadyDetails,
		AccountId:    123,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			AdUsers: map[string][]string{},
		},
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()

	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)

	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.Anything).
		Return(updatedRecord, nil)

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateSdeActiveDirectory_Success(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId:          "test-ad-uuid",
		AccountId:                  "123456789",
		LocationId:                 "us-central1",
		XCorrelationId:             "test-correlation-id",
		Username:                   nillable.GetStringPtr("new-admin@test.local"),
		Password:                   nillable.GetStringPtr("encrypted-pass"),
		DNS:                        nillable.GetStringPtr("10.0.0.2"),
		Domain:                     nillable.GetStringPtr("test.local"),
		NetBIOS:                    nillable.GetStringPtr("TEST"),
		KdcIP:                      nillable.GetStringPtr("10.0.0.3"),
		KdcHostname:                nillable.GetStringPtr("kdc.test.local"),
		OrganizationalUnit:         nillable.GetStringPtr("OU=Computers"),
		Site:                       nillable.GetStringPtr("Default-First-Site"),
		LdapSigning:                nillable.GetBoolPtr(true),
		AllowLocalNFSUsersWithLdap: nillable.GetBoolPtr(false),
		EncryptDCConnections:       nillable.GetBoolPtr(true),
		AesEncryption:              nillable.GetBoolPtr(true),
		SecurityOperators:          []string{"security-user-1"},
		BackupOperators:            []string{"backup-user-1"},
		Administrators:             []string{"admin-user-1"},
		Description:                nillable.GetStringPtr("Updated test AD"),
	}

	originalDecryptPassword := decryptPassword
	decryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-pass"
		return &decrypted, nil
	}
	defer func() { decryptPassword = originalDecryptPassword }()

	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	expectedDone := false
	expectedResponse := &active_directories.V1betaUpdateActiveDirectoryAccepted{
		Payload: &cvpModels.OperationV1beta{
			Done: &expectedDone,
			Name: "operations/test-operation-123",
		},
	}

	mockActiveDirectoriesClient := active_directories.NewMockClientService(t)
	mockActiveDirectoriesClient.On("V1betaUpdateActiveDirectory", mock.MatchedBy(func(p *active_directories.V1betaUpdateActiveDirectoryParams) bool {
		return p.Body.Password == "decrypted-pass"
	})).Return(expectedResponse, nil)

	cvpClient := &cvpapi.Cvp{ActiveDirectories: mockActiveDirectoriesClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	result, err := activity.UpdateSdeActiveDirectory(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockActiveDirectoriesClient.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateSdeActiveDirectory_PartialUpdate(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123456789",
		LocationId:        "us-central1",
		XCorrelationId:    "test-correlation-id",
		DNS:               nillable.GetStringPtr("10.0.0.2"), // Only updating DNS
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	expectedDone := false
	expectedResponse := &active_directories.V1betaUpdateActiveDirectoryAccepted{
		Payload: &cvpModels.OperationV1beta{
			Done: &expectedDone,
			Name: "operations/test-operation-123",
		},
	}

	mockActiveDirectoriesClient := active_directories.NewMockClientService(t)
	mockActiveDirectoriesClient.On("V1betaUpdateActiveDirectory", mock.Anything).
		Return(expectedResponse, nil)

	cvpClient := &cvpapi.Cvp{ActiveDirectories: mockActiveDirectoriesClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	result, err := activity.UpdateSdeActiveDirectory(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestActiveDirectoryUpdateActivity_UpdateSdeActiveDirectory_SecurityGroupsUpdate(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123456789",
		LocationId:        "us-central1",
		XCorrelationId:    "test-correlation-id",
		SecurityOperators: []string{"security-user-1", "security-user-2"},
		BackupOperators:   []string{"backup-user-1"},
		Administrators:    []string{"admin-user-1", "admin-user-2"},
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	expectedDone := false
	expectedResponse := &active_directories.V1betaUpdateActiveDirectoryAccepted{
		Payload: &cvpModels.OperationV1beta{
			Done: &expectedDone,
			Name: "operations/test-operation-123",
		},
	}

	mockActiveDirectoriesClient := active_directories.NewMockClientService(t)
	mockActiveDirectoriesClient.On("V1betaUpdateActiveDirectory", mock.Anything).
		Return(expectedResponse, nil)

	cvpClient := &cvpapi.Cvp{ActiveDirectories: mockActiveDirectoriesClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	result, err := activity.UpdateSdeActiveDirectory(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestActiveDirectoryUpdateActivity_UpdateSdeActiveDirectory_UpdateToEmptyLists(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	// Test updating all three user groups to empty lists for SDE
	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123456789",
		LocationId:        "us-central1",
		XCorrelationId:    "test-correlation-id",
		SecurityOperators: []string{}, // Empty list - should be sent to SDE
		BackupOperators:   []string{}, // Empty list - should be sent to SDE
		Administrators:    []string{}, // Empty list - should be sent to SDE
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	expectedDone := false
	expectedResponse := &active_directories.V1betaUpdateActiveDirectoryAccepted{
		Payload: &cvpModels.OperationV1beta{
			Done: &expectedDone,
			Name: "operations/test-operation-123",
		},
	}

	mockActiveDirectoriesClient := active_directories.NewMockClientService(t)
	// Verify that the request includes empty lists (not nil)
	mockActiveDirectoriesClient.On("V1betaUpdateActiveDirectory", mock.MatchedBy(func(req *active_directories.V1betaUpdateActiveDirectoryParams) bool {
		// Verify empty lists are sent (not nil)
		return req.Body != nil &&
			req.Body.SecurityOperators != nil && len(req.Body.SecurityOperators) == 0 &&
			req.Body.BackupOperators != nil && len(req.Body.BackupOperators) == 0 &&
			req.Body.Administrators != nil && len(req.Body.Administrators) == 0
	})).Return(expectedResponse, nil)

	cvpClient := &cvpapi.Cvp{ActiveDirectories: mockActiveDirectoriesClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	result, err := activity.UpdateSdeActiveDirectory(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockActiveDirectoriesClient.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateSdeActiveDirectory_UpdateOnlyOneToEmptyList(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	// Test updating only SecurityOperators to empty list, others not provided
	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123456789",
		LocationId:        "us-central1",
		XCorrelationId:    "test-correlation-id",
		SecurityOperators: []string{}, // Empty list - should be sent
		// BackupOperators and Administrators are nil (not being updated)
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	expectedDone := false
	expectedResponse := &active_directories.V1betaUpdateActiveDirectoryAccepted{
		Payload: &cvpModels.OperationV1beta{
			Done: &expectedDone,
			Name: "operations/test-operation-123",
		},
	}

	mockActiveDirectoriesClient := active_directories.NewMockClientService(t)
	// Verify that only SecurityOperators is set (empty), others are nil
	mockActiveDirectoriesClient.On("V1betaUpdateActiveDirectory", mock.MatchedBy(func(req *active_directories.V1betaUpdateActiveDirectoryParams) bool {
		return req.Body != nil &&
			req.Body.SecurityOperators != nil && len(req.Body.SecurityOperators) == 0 &&
			req.Body.BackupOperators == nil &&
			req.Body.Administrators == nil
	})).Return(expectedResponse, nil)

	cvpClient := &cvpapi.Cvp{ActiveDirectories: mockActiveDirectoriesClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	result, err := activity.UpdateSdeActiveDirectory(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockActiveDirectoriesClient.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateSdeActiveDirectory_MixedEmptyAndNonEmptyLists(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	// Test mixed: empty SecurityOperators, non-empty BackupOperators, empty Administrators
	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123456789",
		LocationId:        "us-central1",
		XCorrelationId:    "test-correlation-id",
		SecurityOperators: []string{},                // Empty list
		BackupOperators:   []string{"backup-user-1"}, // Non-empty list
		Administrators:    []string{},                // Empty list
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	expectedDone := false
	expectedResponse := &active_directories.V1betaUpdateActiveDirectoryAccepted{
		Payload: &cvpModels.OperationV1beta{
			Done: &expectedDone,
			Name: "operations/test-operation-123",
		},
	}

	mockActiveDirectoriesClient := active_directories.NewMockClientService(t)
	// Verify the correct mix is sent to SDE
	mockActiveDirectoriesClient.On("V1betaUpdateActiveDirectory", mock.MatchedBy(func(req *active_directories.V1betaUpdateActiveDirectoryParams) bool {
		return req.Body != nil &&
			req.Body.SecurityOperators != nil && len(req.Body.SecurityOperators) == 0 &&
			req.Body.BackupOperators != nil && len(req.Body.BackupOperators) == 1 && req.Body.BackupOperators[0] == "backup-user-1" &&
			req.Body.Administrators != nil && len(req.Body.Administrators) == 0
	})).Return(expectedResponse, nil)

	cvpClient := &cvpapi.Cvp{ActiveDirectories: mockActiveDirectoriesClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	result, err := activity.UpdateSdeActiveDirectory(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockActiveDirectoriesClient.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateSdeActiveDirectory_CVPError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123456789",
		LocationId:        "us-central1",
		XCorrelationId:    "test-correlation-id",
		DNS:               nillable.GetStringPtr("10.0.0.2"),
	}

	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	mockActiveDirectoriesClient := active_directories.NewMockClientService(t)
	mockActiveDirectoriesClient.On("V1betaUpdateActiveDirectory", mock.Anything).
		Return(nil, &active_directories.V1betaUpdateActiveDirectoryNotFound{
			Payload: &cvpModels.Error{
				Code:    404,
				Message: "Not found",
			},
		})

	cvpClient := &cvpapi.Cvp{ActiveDirectories: mockActiveDirectoriesClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	result, err := activity.UpdateSdeActiveDirectory(ctx, params)

	assert.Nil(t, result)
	assert.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	assert.True(t, customErr.IsError(vsaerrors.ErrCVPNotFound))
}

func TestActiveDirectoryUpdateActivity_UpdateSdeActiveDirectory_PasswordDecryptionFailure(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123456789",
		LocationId:        "us-central1",
		XCorrelationId:    "test-correlation-id",
		Password:          nillable.GetStringPtr("bad-encrypted-data"),
	}

	originalDecryptPassword := decryptPassword
	decryptPassword = func(password log.Secret) (*string, error) {
		return nil, stderrors.New("decryption failed")
	}
	defer func() { decryptPassword = originalDecryptPassword }()

	result, err := activity.UpdateSdeActiveDirectory(ctx, params)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Password could not be decrypted")
}

func TestActiveDirectoryUpdateActivity_UpdateSdeActiveDirectory_NilPasswordSkipsDecryption(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123456789",
		LocationId:        "us-central1",
		XCorrelationId:    "test-correlation-id",
		DNS:               nillable.GetStringPtr("10.0.0.2"),
	}

	decryptCalled := false
	originalDecryptPassword := decryptPassword
	decryptPassword = func(password log.Secret) (*string, error) {
		decryptCalled = true
		return nil, stderrors.New("should not be called")
	}
	defer func() { decryptPassword = originalDecryptPassword }()

	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	expectedDone := false
	expectedResponse := &active_directories.V1betaUpdateActiveDirectoryAccepted{
		Payload: &cvpModels.OperationV1beta{
			Done: &expectedDone,
			Name: "operations/test-operation-123",
		},
	}

	mockActiveDirectoriesClient := active_directories.NewMockClientService(t)
	mockActiveDirectoriesClient.On("V1betaUpdateActiveDirectory", mock.MatchedBy(func(p *active_directories.V1betaUpdateActiveDirectoryParams) bool {
		return p.Body.Password == ""
	})).Return(expectedResponse, nil)

	cvpClient := &cvpapi.Cvp{ActiveDirectories: mockActiveDirectoriesClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	result, err := activity.UpdateSdeActiveDirectory(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, decryptCalled, "decryptPassword should not be called when password is nil")
	mockActiveDirectoriesClient.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateSdeActiveDirectory_DecryptedPasswordSentToSDE(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123456789",
		LocationId:        "us-central1",
		XCorrelationId:    "test-correlation-id",
		Password:          nillable.GetStringPtr("encrypted-payload"),
	}

	originalDecryptPassword := decryptPassword
	decryptPassword = func(password log.Secret) (*string, error) {
		assert.Equal(t, log.Secret("encrypted-payload"), password, "encrypted password should be passed to decryptPassword")
		decrypted := "plaintext-password"
		return &decrypted, nil
	}
	defer func() { decryptPassword = originalDecryptPassword }()

	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	expectedDone := false
	expectedResponse := &active_directories.V1betaUpdateActiveDirectoryAccepted{
		Payload: &cvpModels.OperationV1beta{
			Done: &expectedDone,
			Name: "operations/test-operation-123",
		},
	}

	var capturedBody *cvpModels.ActiveDirectoryUpdateV1beta
	mockActiveDirectoriesClient := active_directories.NewMockClientService(t)
	mockActiveDirectoriesClient.On("V1betaUpdateActiveDirectory", mock.MatchedBy(func(p *active_directories.V1betaUpdateActiveDirectoryParams) bool {
		capturedBody = p.Body
		return true
	})).Return(expectedResponse, nil)

	cvpClient := &cvpapi.Cvp{ActiveDirectories: mockActiveDirectoriesClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	result, err := activity.UpdateSdeActiveDirectory(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "plaintext-password", capturedBody.Password, "decrypted plaintext password should be sent to SDE")
	mockActiveDirectoriesClient.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_TokenError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	// Mock JWT token error in context - the activity should get token from context
	// Remove JWT token from context to simulate token retrieval failure
	ctx := context.Background() // No JWT token

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_OperationNotFinished(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	// Mock the CvpClient function
	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(&async.V1betaDescribeOperationOK{
			Payload: &cvpModels.OperationV1beta{
				Done: &done,
				Name: "operations/test-operation-123",
			},
		}, nil)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Error SDE job not done")

	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.False(t, appErr.NonRetryable(), "not-finished must be retryable so Temporal polls again")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_OperationCompletedSuccessfully(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	completedDone := true
	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(&async.V1betaDescribeOperationOK{
			Payload: &cvpModels.OperationV1beta{
				Done: &completedDone,
				Name: "operations/test-operation-123",
			},
		}, nil)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.NoError(t, err)
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_OperationCompletedWithBadRequestError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	completedDone := true
	code := float64(400)
	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(&async.V1betaDescribeOperationOK{
			Payload: &cvpModels.OperationV1beta{
				Done:  &completedDone,
				Error: &cvpModels.StatusV1Beta{Code: code, Message: "Bad request"},
				Name:  "operations/test-operation-123",
			},
		}, nil)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	assert.True(t, customErr.IsError(vsaerrors.ErrCVPBadRequest))

	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "terminal poll 400 must be non-retryable")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_OperationCompletedWithUnauthorizedError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	completedDone := true
	code := float64(401)
	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(&async.V1betaDescribeOperationOK{
			Payload: &cvpModels.OperationV1beta{
				Done:  &completedDone,
				Error: &cvpModels.StatusV1Beta{Code: code, Message: "Unauthorized"},
				Name:  "operations/test-operation-123",
			},
		}, nil)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	assert.True(t, customErr.IsError(vsaerrors.ErrCVPUnauthorized))

	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "terminal poll 401 must be non-retryable")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_OperationCompletedWithForbiddenError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	completedDone := true
	code := float64(403)
	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(&async.V1betaDescribeOperationOK{
			Payload: &cvpModels.OperationV1beta{
				Done:  &completedDone,
				Error: &cvpModels.StatusV1Beta{Code: code, Message: "Forbidden"},
				Name:  "operations/test-operation-123",
			},
		}, nil)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	assert.True(t, customErr.IsError(vsaerrors.ErrCVPForbidden))

	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "terminal poll 403 must be non-retryable")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_OperationCompletedWithNotFoundError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	completedDone := true
	code := float64(404)
	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(&async.V1betaDescribeOperationOK{
			Payload: &cvpModels.OperationV1beta{
				Done:  &completedDone,
				Error: &cvpModels.StatusV1Beta{Code: code, Message: "Not found"},
				Name:  "operations/test-operation-123",
			},
		}, nil)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	assert.NotNil(t, customErr)
	assert.True(t, customErr.IsError(vsaerrors.ErrCVPNotFound))

	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "terminal poll 404 must be non-retryable")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_OperationCompletedWithInternalServerError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	completedDone := true
	code := float64(500)
	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(&async.V1betaDescribeOperationOK{
			Payload: &cvpModels.OperationV1beta{
				Done:  &completedDone,
				Error: &cvpModels.StatusV1Beta{Code: code, Message: "Internal server error"},
				Name:  "operations/test-operation-123",
			},
		}, nil)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	assert.True(t, customErr.IsError(vsaerrors.ErrCVPInternalServerError))

	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "terminal poll 500 must be non-retryable (Done=true means retrying cannot help)")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_OperationCompletedWithTooManyRequestsError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	completedDone := true
	code := float64(429)
	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(&async.V1betaDescribeOperationOK{
			Payload: &cvpModels.OperationV1beta{
				Done:  &completedDone,
				Error: &cvpModels.StatusV1Beta{Code: code, Message: "Too many requests"},
				Name:  "operations/test-operation-123",
			},
		}, nil)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	assert.True(t, customErr.IsError(vsaerrors.ErrCVPTooManyRequests))

	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "terminal poll 429 must be non-retryable (Done=true means retrying cannot help)")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_OperationCompletedWithUnknownError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	// Mock JWT token in context
	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	completedDone := true
	code := float64(999)
	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(&async.V1betaDescribeOperationOK{
			Payload: &cvpModels.OperationV1beta{
				Done:  &completedDone,
				Error: &cvpModels.StatusV1Beta{Code: code, Message: "Unknown error"},
				Name:  "operations/test-operation-123",
			},
		}, nil)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	assert.NotNil(t, customErr)
	assert.True(t, customErr.IsError(vsaerrors.ErrCVPInternalServerError))

	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "terminal poll with unknown error code must be non-retryable (Done=true)")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_NilResult(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	err := activity.PollSdeUpdateActivity(ctx, params, nil)
	assert.NoError(t, err, "nil result should be a no-op")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_AlreadyDoneSynchronously(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := true
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)
	assert.NoError(t, err, "synchronously completed operation should return nil without polling")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_EmptyOperationName(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "",
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "empty operation name must be non-retryable")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_DescribeOperationNotFoundError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	mockAsyncClient := async.NewMockClientService(t)
	notFoundErr := &async.V1betaDescribeOperationNotFound{}
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(nil, notFoundErr)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "describe-operation 404 should be non-retryable")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_DescribeOperationBadRequestError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	mockAsyncClient := async.NewMockClientService(t)
	badReqErr := &async.V1betaDescribeOperationBadRequest{}
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(nil, badReqErr)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "describe-operation 400 should be non-retryable")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_DescribeOperationGenericError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(nil, stderrors.New("connection timeout"))

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "poll failure wraps as non-retryable via WrapAsNonRetryableTemporalApplicationError")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_OperationCompletedWithConflictError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	completedDone := true
	code := float64(409)
	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(&async.V1betaDescribeOperationOK{
			Payload: &cvpModels.OperationV1beta{
				Done:  &completedDone,
				Error: &cvpModels.StatusV1Beta{Code: code, Message: "Conflict"},
				Name:  "operations/test-operation-123",
			},
		}, nil)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	assert.NotNil(t, customErr)
	assert.True(t, customErr.IsError(vsaerrors.ErrCVPConflict))

	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "terminal poll 409 must be non-retryable")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_OperationCompletedWithUnprocessableEntityError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:  "123456789",
		LocationId: "us-central1",
	}

	done := false
	result := &cvpModels.OperationV1beta{
		Done: &done,
		Name: "operations/test-operation-123",
	}

	ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

	completedDone := true
	code := float64(422)
	mockAsyncClient := async.NewMockClientService(t)
	mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
		Return(&async.V1betaDescribeOperationOK{
			Payload: &cvpModels.OperationV1beta{
				Done:  &completedDone,
				Error: &cvpModels.StatusV1Beta{Code: code, Message: "Unprocessable entity"},
				Name:  "operations/test-operation-123",
			},
		}, nil)

	cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	err := activity.PollSdeUpdateActivity(ctx, params, result)

	assert.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	assert.NotNil(t, customErr)
	assert.True(t, customErr.IsError(vsaerrors.ErrCVPUnprocessableEntity))

	var appErr *temporal.ApplicationError
	assert.True(t, stderrors.As(err, &appErr))
	assert.True(t, appErr.NonRetryable(), "terminal poll 422 must be non-retryable")
}

func TestActiveDirectoryUpdateActivity_PollSdeUpdateActivity_AllTerminalErrorsAreNonRetryable(t *testing.T) {
	codes := []struct {
		code       float64
		trackingID int
		label      string
	}{
		{400, vsaerrors.ErrCVPBadRequest, "400 Bad Request"},
		{401, vsaerrors.ErrCVPUnauthorized, "401 Unauthorized"},
		{403, vsaerrors.ErrCVPForbidden, "403 Forbidden"},
		{404, vsaerrors.ErrCVPNotFound, "404 Not Found"},
		{409, vsaerrors.ErrCVPConflict, "409 Conflict"},
		{422, vsaerrors.ErrCVPUnprocessableEntity, "422 Unprocessable Entity"},
		{429, vsaerrors.ErrCVPTooManyRequests, "429 Too Many Requests"},
		{500, vsaerrors.ErrCVPInternalServerError, "500 Internal Server Error"},
		{999, vsaerrors.ErrCVPInternalServerError, "999 Unknown Code"},
	}

	for _, tc := range codes {
		t.Run(tc.label, func(t *testing.T) {
			ctx := context.Background()
			mockStorage := database.NewMockStorage(t)
			activity := &ActiveDirectoryUpdateActivity{SE: mockStorage}

			params := &common.UpdateActiveDirectoryParams{
				AccountId:  "123456789",
				LocationId: "us-central1",
			}

			done := false
			result := &cvpModels.OperationV1beta{
				Done: &done,
				Name: "operations/test-operation-123",
			}

			ctx = context.WithValue(ctx, "jwt_token", "test-jwt-token")

			completedDone := true
			mockAsyncClient := async.NewMockClientService(t)
			mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).
				Return(&async.V1betaDescribeOperationOK{
					Payload: &cvpModels.OperationV1beta{
						Done:  &completedDone,
						Error: &cvpModels.StatusV1Beta{Code: tc.code, Message: "error message"},
						Name:  "operations/test-operation-123",
					},
				}, nil)

			cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
			originalCvpClient := CvpClient
			defer func() { CvpClient = originalCvpClient }()
			CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
				return *cvpClient
			}

			err := activity.PollSdeUpdateActivity(ctx, params, result)

			assert.Error(t, err)

			customErr := vsaerrors.ExtractCustomError(err)
			assert.NotNil(t, customErr, "should have VCP custom error for %s", tc.label)
			assert.True(t, customErr.IsError(tc.trackingID), "wrong tracking ID for %s", tc.label)

			var appErr *temporal.ApplicationError
			assert.True(t, stderrors.As(err, &appErr))
			assert.True(t, appErr.NonRetryable(),
				"terminal poll %s must be non-retryable (Done=true, retrying cannot change outcome)", tc.label)
		})
	}
}

func TestActiveDirectoryUpdateActivity_MarkVcpAdToUpdatingActivity_Success(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
	}

	oldAd := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		AccountId: 123,
		State:     datamodel.LifeCycleStateREADY,
	}

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil)
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)
	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return ad.State == datamodel.LifeCycleStateUpdating &&
			ad.StateDetails == datamodel.LifeCycleStateUpdatingDetails
	})).Return(existingRecord, nil)

	err := activity.MarkVcpAdToUpdatingActivity(ctx, params, oldAd)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_MarkVcpAdToUpdatingActivity_AccountNotFound(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
	}

	oldAd := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
	}

	mockStorage.On("GetAccount", mock.Anything, "123").Return(nil, errors.New("account not found"))

	err := activity.MarkVcpAdToUpdatingActivity(ctx, params, oldAd)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Could not fetch related Account for Active Directory update")
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_MarkVcpAdToUpdatingActivity_ADNotFoundInVCP(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
	}

	oldAd := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil)
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(nil, nil)

	err := activity.MarkVcpAdToUpdatingActivity(ctx, params, oldAd)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_MarkVcpAdToUpdatingActivity_UpdateFailed(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
	}

	oldAd := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		AccountId: 123,
		State:     datamodel.LifeCycleStateREADY,
	}

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil)
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)
	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.Anything).
		Return(nil, vsaerrors.New("database update failed"))

	err := activity.MarkVcpAdToUpdatingActivity(ctx, params, oldAd)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database update failed")
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_MarkVcpAdToErrorActivity_Success(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
	}

	oldAd := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		AccountId: 123,
		State:     datamodel.LifeCycleStateUpdating,
	}

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil)
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)
	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return ad.State == datamodel.LifeCycleStateError &&
			ad.StateDetails == datamodel.LifeCycleStateUpdateErrorDetails
	})).Return(existingRecord, nil)

	err := activity.MarkVcpAdToErrorActivity(ctx, params, oldAd)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_MarkVcpAdToErrorActivity_AccountNotFound(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
	}

	oldAd := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
	}

	mockStorage.On("GetAccount", mock.Anything, "123").Return(nil, errors.New("account not found"))

	err := activity.MarkVcpAdToErrorActivity(ctx, params, oldAd)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Could not fetch related Account for Active Directory update")
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_MarkVcpAdToErrorActivity_ADNotFoundInVCP(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
	}

	oldAd := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil)
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(nil, nil)

	err := activity.MarkVcpAdToErrorActivity(ctx, params, oldAd)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_MarkVcpAdToErrorActivity_UpdateFailed(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	activity := &ActiveDirectoryUpdateActivity{
		SE: mockStorage,
	}

	params := &common.UpdateActiveDirectoryParams{
		ActiveDirectoryId: "test-ad-uuid",
		AccountId:         "123",
	}

	oldAd := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName: "test-ad",
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}

	existingRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName:    "test-ad",
		AccountId: 123,
		State:     datamodel.LifeCycleStateUpdating,
	}

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil)
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)
	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.Anything).
		Return(nil, vsaerrors.New("database update failed"))

	err := activity.MarkVcpAdToErrorActivity(ctx, params, oldAd)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database update failed")
	mockStorage.AssertExpectations(t)
}

func TestActiveDirectoryUpdateActivity_UpdateVcpActiveDirectory_PasswordDecryption(t *testing.T) {
	t.Run("Successfully decrypts and updates password", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		activity := &ActiveDirectoryUpdateActivity{
			SE: mockStorage,
		}

		encryptedPassword := "encrypted-password"
		params := &common.UpdateActiveDirectoryParams{
			ActiveDirectoryId: "test-ad-uuid",
			AccountId:         "123",
			Password:          &encryptedPassword,
		}

		oldAd := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName: "test-ad",
			ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
				OrganizationalUnit:         "OU=test",
				Site:                       "test-site",
				SecurityOperators:          []string{},
				BackupOperators:            []string{},
				Administrators:             []string{},
				KdcIP:                      "192.168.1.1",
				KdcHostname:                "kdc.test.com",
				AesEncryption:              true,
				EncryptDCConnections:       true,
				LdapSigning:                true,
				AllowLocalNFSUsersWithLdap: true,
			},
		}

		oldAdDbRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName:         "test-ad",
			AccountId:      123,
			CredentialPath: "old-credential-path",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AdUsers: map[string][]string{},
			},
		}

		// Mock DecryptPassword
		originalDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(password log.Secret) (*string, error) {
			decrypted := "decrypted-password"
			return &decrypted, nil
		}
		defer func() { utils.DecryptPassword = originalDecryptPassword }()

		// Mock updatePasswordSecret
		updatePasswordSecretCalled := false
		originalUpdatePasswordSecret := updatePasswordSecret
		updatePasswordSecret = func(ctx context.Context, password string, secretID string) error {
			assert.Equal(t, "decrypted-password", password)
			assert.Equal(t, "old-credential-path", secretID)
			updatePasswordSecretCalled = true
			return nil
		}
		defer func() { updatePasswordSecret = originalUpdatePasswordSecret }()

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 123,
			},
		}
		mockStorage.On("GetAccount", mock.Anything, "123").Return(mockAccount, nil)
		mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).Return(oldAdDbRecord, nil)
		mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.Anything).Return(oldAdDbRecord, nil)

		err := activity.UpdateVcpActiveDirectory(ctx, params, oldAd, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

		assert.NoError(t, err)
		assert.True(t, updatePasswordSecretCalled, "updatePasswordSecret should have been called")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Returns error when password decryption fails", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		activity := &ActiveDirectoryUpdateActivity{
			SE: mockStorage,
		}

		encryptedPassword := "encrypted-password"
		params := &common.UpdateActiveDirectoryParams{
			ActiveDirectoryId: "test-ad-uuid",
			AccountId:         "123",
			Password:          &encryptedPassword,
		}

		oldAd := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName: "test-ad",
			ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
				OrganizationalUnit:         "OU=test",
				Site:                       "test-site",
				SecurityOperators:          []string{},
				BackupOperators:            []string{},
				Administrators:             []string{},
				KdcIP:                      "192.168.1.1",
				KdcHostname:                "kdc.test.com",
				AesEncryption:              true,
				EncryptDCConnections:       true,
				LdapSigning:                true,
				AllowLocalNFSUsersWithLdap: true,
			},
		}

		oldAdDbRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName:         "test-ad",
			AccountId:      123,
			CredentialPath: "old-credential-path",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AdUsers: map[string][]string{},
			},
		}

		// Mock DecryptPassword to fail
		originalDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(password log.Secret) (*string, error) {
			return nil, vsaerrors.New("decryption failed")
		}
		defer func() { utils.DecryptPassword = originalDecryptPassword }()

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 123,
			},
		}
		mockStorage.On("GetAccount", mock.Anything, "123").Return(mockAccount, nil)
		mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).Return(oldAdDbRecord, nil)

		err := activity.UpdateVcpActiveDirectory(ctx, params, oldAd, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

		assert.Error(t, err)
	})

	t.Run("Returns error when updatePasswordSecret fails", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		activity := &ActiveDirectoryUpdateActivity{
			SE: mockStorage,
		}

		encryptedPassword := "encrypted-password"
		params := &common.UpdateActiveDirectoryParams{
			ActiveDirectoryId: "test-ad-uuid",
			AccountId:         "123",
			Password:          &encryptedPassword,
		}

		oldAd := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName: "test-ad",
			ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
				OrganizationalUnit:         "OU=test",
				Site:                       "test-site",
				SecurityOperators:          []string{},
				BackupOperators:            []string{},
				Administrators:             []string{},
				KdcIP:                      "192.168.1.1",
				KdcHostname:                "kdc.test.com",
				AesEncryption:              true,
				EncryptDCConnections:       true,
				LdapSigning:                true,
				AllowLocalNFSUsersWithLdap: true,
			},
		}

		oldAdDbRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName:         "test-ad",
			AccountId:      123,
			CredentialPath: "old-credential-path",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AdUsers: map[string][]string{},
			},
		}

		// Mock DecryptPassword
		originalDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(password log.Secret) (*string, error) {
			decrypted := "decrypted-password"
			return &decrypted, nil
		}
		defer func() { utils.DecryptPassword = originalDecryptPassword }()

		// Mock updatePasswordSecret to fail
		originalUpdatePasswordSecret := updatePasswordSecret
		updatePasswordSecret = func(ctx context.Context, password string, secretID string) error {
			return vsaerrors.New("failed to update password secret")
		}
		defer func() { updatePasswordSecret = originalUpdatePasswordSecret }()

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 123,
			},
		}
		mockStorage.On("GetAccount", mock.Anything, "123").Return(mockAccount, nil)
		mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).Return(oldAdDbRecord, nil)

		err := activity.UpdateVcpActiveDirectory(ctx, params, oldAd, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

		assert.Error(t, err)
	})

	t.Run("Does not update password when password is nil", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		activity := &ActiveDirectoryUpdateActivity{
			SE: mockStorage,
		}

		params := &common.UpdateActiveDirectoryParams{
			ActiveDirectoryId: "test-ad-uuid",
			AccountId:         "123",
			Password:          nil, // No password update
		}

		oldAd := &models.ActiveDirectory{
			BaseModel: models.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName: "test-ad",
			ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
				OrganizationalUnit:         "OU=test",
				Site:                       "test-site",
				SecurityOperators:          []string{},
				BackupOperators:            []string{},
				Administrators:             []string{},
				KdcIP:                      "192.168.1.1",
				KdcHostname:                "kdc.test.com",
				AesEncryption:              true,
				EncryptDCConnections:       true,
				LdapSigning:                true,
				AllowLocalNFSUsersWithLdap: true,
			},
		}

		oldAdDbRecord := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{
				UUID: "test-ad-uuid",
			},
			AdName:         "test-ad",
			AccountId:      123,
			CredentialPath: "old-credential-path",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AdUsers: map[string][]string{},
			},
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 123,
			},
		}
		mockStorage.On("GetAccount", mock.Anything, "123").Return(mockAccount, nil)
		mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).Return(oldAdDbRecord, nil)
		mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.Anything).Return(oldAdDbRecord, nil)

		err := activity.UpdateVcpActiveDirectory(ctx, params, oldAd, "test-change-id", datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		// Verify that DecryptPassword was not called by not setting up mock
	})
}

func TestPropagateAdChangeIdToPool_Success(t *testing.T) {
	storage := &database.MockStorage{}
	activity := ActiveDirectoryActivity{SE: storage}
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid"}}
	adChangeId := "change-id"

	storage.On("UpdatePoolFields", mock.Anything, pool.UUID, mock.Anything).Return(nil)

	err := activity.PropagateAdChangeIdToPool(context.Background(), pool, adChangeId)
	assert.NoError(t, err)
	assert.Equal(t, adChangeId, pool.ActiveDirectoryChangeId)
	storage.AssertExpectations(t)
}

func TestPropagateAdChangeIdToPool_NilPool(t *testing.T) {
	activity := ActiveDirectoryActivity{}
	err := activity.PropagateAdChangeIdToPool(context.Background(), nil, "change-id")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool is nil")
}

func TestPropagateAdChangeIdToPool_EmptyChangeId(t *testing.T) {
	activity := ActiveDirectoryActivity{}
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid"}}
	err := activity.PropagateAdChangeIdToPool(context.Background(), pool, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "adChangeId is empty")
}

func TestPropagateAdChangeIdToPool_UpdateError(t *testing.T) {
	storage := &database.MockStorage{}
	activity := ActiveDirectoryActivity{SE: storage}
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid"}}
	adChangeId := "change-id"

	storage.On("UpdatePoolFields", mock.Anything, pool.UUID, mock.Anything).Return(errors.New("db error"))

	err := activity.PropagateAdChangeIdToPool(context.Background(), pool, adChangeId)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db error")
	storage.AssertExpectations(t)
}

func TestUpdateAdCredentialsForSvm_UpdateFails(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	params := vsa.UpdateActiveDirectoryCredentialsParams{
		NewCredentials: &vsa.ActiveDirectory{DNS: "10.0.0.2"},
	}

	activity := ActiveDirectoryActivity{}
	err := activity.UpdateAdCredentialsForSvm(ctx, &models.Node{}, params, "svm-name", "", ontapRest.CifsService{})

	assert.Error(t, err)
	var appErr *temporal.ApplicationError
	if assert.True(t, stderrors.As(err, &appErr)) {
		var tid int
		var origMsg string
		if assert.NoError(t, appErr.Details(&tid, &origMsg)) {
			assert.Equal(t, vsaerrors.ErrADUnclassified, tid)
			assert.Contains(t, origMsg, "Error determining server for update")
		}
	}
}
