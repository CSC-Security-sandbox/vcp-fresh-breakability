package active_directory_activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/active_directories"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
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
		State:                     models.LifeCycleStateREADY,
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
		State:     models.LifeCycleStateREADY,
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
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateReadyDetails,
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
			ad.State == models.LifeCycleStateREADY &&
			ad.StateDetails == models.LifeCycleStateReadyDetails
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

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id")

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

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id")

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
		Return(existingRecord, nil)

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

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id")

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

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id")

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
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateReadyDetails,
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

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id")

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
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateReadyDetails,
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

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id")

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
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateReadyDetails,
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

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id")

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
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateReadyDetails,
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

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id")

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
		SecurityOperators: []string{},                   // Empty list -> should become nil
		BackupOperators:   []string{"backup-user-1"},   // Non-empty list -> should remain as array
		Administrators:    []string{},                   // Empty list -> should become nil
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
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateReadyDetails,
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

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id")

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
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateReadyDetails,
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

	err := activity.UpdateVcpActiveDirectory(ctx, params, ad, "test-change-id")

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
		Password:                   nillable.GetStringPtr("NewSecurePass123!"),
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
	assert.Contains(t, err.Error(), "Bad request")
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
	assert.Contains(t, err.Error(), "Unauthorised")
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
	assert.Contains(t, err.Error(), "Forbidden")
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
	assert.Contains(t, err.Error(), "not found")
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
	assert.Contains(t, err.Error(), "Internal server error")
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
	assert.Contains(t, err.Error(), "Too many requests")
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
		State:     models.LifeCycleStateREADY,
	}

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil)
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)
	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return ad.State == models.LifeCycleStateUpdating &&
			ad.StateDetails == models.LifeCycleStateUpdatingDetails
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
		State:     models.LifeCycleStateREADY,
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
		State:     models.LifeCycleStateUpdating,
	}

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil)
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).
		Return(existingRecord, nil)
	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return ad.State == models.LifeCycleStateError &&
			ad.StateDetails == models.LifeCycleStateUpdateErrorDetails
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
		State:     models.LifeCycleStateUpdating,
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

		err := activity.UpdateVcpActiveDirectory(ctx, params, oldAd, "test-change-id")

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

		err := activity.UpdateVcpActiveDirectory(ctx, params, oldAd, "test-change-id")

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

		err := activity.UpdateVcpActiveDirectory(ctx, params, oldAd, "test-change-id")

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
		}

		mockAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 123,
			},
		}
		mockStorage.On("GetAccount", mock.Anything, "123").Return(mockAccount, nil)
		mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).Return(oldAdDbRecord, nil)
		mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.Anything).Return(oldAdDbRecord, nil)

		err := activity.UpdateVcpActiveDirectory(ctx, params, oldAd, "test-change-id")

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
