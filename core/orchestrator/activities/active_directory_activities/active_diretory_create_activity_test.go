package active_directory_activities

import (
	"context"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestActiveDirectoryCreateActivity_CreateVcpActiveDirectory(t *testing.T) {
	t.Run("SuccessfulCreation", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockGCPService := &hyperscaler.MockGoogleServices{}

		activity := &ActiveDirectoryCreateActivity{
			SE: mockStorage,
		}

		ctx := context.Background()
		params := &common.CreateActiveDirectoryParams{
			ResourceId:                 "test-ad",
			Domain:                     "test.example.com",
			DNS:                        "8.8.8.8",
			NetBIOS:                    "TESTNETBIOS",
			OrganizationalUnit:         "OU=test",
			Username:                   "admin",
			Password:                   "password123",
			BackupOperators:            []string{"user1", "user2"},
			Administrators:             []string{"admin1"},
			SecurityOperators:          []string{"security1"},
			KdcIP:                      "192.168.1.1",
			KdcHostname:                "example.com",
			AesEncryption:              true,
			EncryptDCConnections:       true,
			LdapSigning:                true,
			AllowLocalNFSUsersWithLdap: true,
			Description:                "Test AD",
		}
		adUUID := "test-ad-uuid"
		accountId := int64(123)

		// Mock that no existing AD is found
		mockStorage.On("GetActiveDirectoryByNameAndAccountID", ctx, params.ResourceId, accountId).Return(nil, customerrors.New("not found"))

		expectedAD := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{UUID: adUUID},
			AdName:    params.ResourceId,
			Username:  params.Username,
			Domain:    params.Domain,
			DNS:       params.DNS,
			NetBIOS:   params.NetBIOS,
			AccountId: accountId,
			State:     string(gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY),
		}

		mockStorage.On("CreateActiveDirectory", ctx, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
			return ad.AdName == params.ResourceId &&
				ad.Username == params.Username &&
				ad.Domain == params.Domain &&
				ad.DNS == params.DNS &&
				ad.NetBIOS == params.NetBIOS &&
				ad.AccountId == accountId &&
				ad.UUID == adUUID &&
				ad.State == string(gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY) &&
				ad.ActiveDirectoryAttributes.OrganizationalUnit == params.OrganizationalUnit &&
				ad.ActiveDirectoryAttributes.PrimaryAD == true
		})).Return(expectedAD, nil)

		// Mock GCP service for secret storage
		mockSecret := &hyperscalermodels.CustomSecret{
			SecretVersion: &hyperscalermodels.CustomSecretVersion{
				Value: params.Password,
			},
		}
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, customerrors.New("secret not found"))
		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockSecret, nil)

		// Instead of mocking the global function, we need to inject the mock service
		// into the activity or ensure the activity uses our mock
		originalStorePasswordSecret := storePasswordSecret
		storePasswordSecret = func(gcpService hyperscaler.GoogleServices, password, secretID string) error {
			// Use the mockGCPService that we set up expectations for
			_, err := mockGCPService.GetSecretWithLatestVersion("test-project", secretID)
			if err != nil && !strings.Contains(err.Error(), "secret not found") {
				return err
			}
			if err != nil { // secret not found, create it
				_, err := mockGCPService.CreateSecret("test-project", "us-central1", secretID, password)
				return err
			}
			return nil
		}
		defer func() {
			storePasswordSecret = originalStorePasswordSecret
		}()

		result, err := activity.CreateVcpActiveDirectory(ctx, params, adUUID, accountId)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, expectedAD.AdName, result.AdName)
		assert.Equal(t, expectedAD.Username, result.Username)
		assert.Equal(t, expectedAD.State, result.State)
		mockStorage.AssertExpectations(t)
		mockGCPService.AssertExpectations(t)
	})

	t.Run("ExistingActiveDirectoryConflict", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &ActiveDirectoryCreateActivity{
			SE: mockStorage,
		}

		ctx := context.Background()
		params := &common.CreateActiveDirectoryParams{
			ResourceId: "existing-ad",
		}
		adUUID := "test-ad-uuid"
		accountId := int64(123)

		existingAD := &datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{UUID: "existing-uuid"},
			AdName:    params.ResourceId,
			AccountId: accountId,
		}

		mockStorage.On("GetActiveDirectoryByNameAndAccountID", ctx, params.ResourceId, accountId).Return(existingAD, nil)

		result, err := activity.CreateVcpActiveDirectory(ctx, params, adUUID, accountId)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "already exists")
		mockStorage.AssertExpectations(t)
	})

	t.Run("DatabaseErrorDuringLookup", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &ActiveDirectoryCreateActivity{
			SE: mockStorage,
		}

		ctx := context.Background()
		params := &common.CreateActiveDirectoryParams{
			ResourceId: "test-ad",
		}
		adUUID := "test-ad-uuid"
		accountId := int64(123)

		dbError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, vsaerrors.New("database lookup failed"))
		mockStorage.On("GetActiveDirectoryByNameAndAccountID", ctx, params.ResourceId, accountId).Return(nil, dbError)

		result, err := activity.CreateVcpActiveDirectory(ctx, params, adUUID, accountId)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, dbError, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("DatabaseErrorDuringCreation", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &ActiveDirectoryCreateActivity{
			SE: mockStorage,
		}

		ctx := context.Background()
		params := &common.CreateActiveDirectoryParams{
			ResourceId: "test-ad",
			Username:   "admin",
			Password:   "password123",
		}
		adUUID := "test-ad-uuid"
		accountId := int64(123)

		mockStorage.On("GetActiveDirectoryByNameAndAccountID", ctx, params.ResourceId, accountId).Return(nil, customerrors.New("not found"))

		createError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, vsaerrors.New("database creation failed"))
		mockStorage.On("CreateActiveDirectory", ctx, mock.Anything).Return(nil, createError)

		// Mock the password secret storage to prevent GCP authentication issues
		originalStorePasswordSecret := storePasswordSecret
		storePasswordSecret = func(gcpService hyperscaler.GoogleServices, password, secretID string) error {
			return nil // Mock successful secret storage
		}
		defer func() {
			storePasswordSecret = originalStorePasswordSecret
		}()

		result, err := activity.CreateVcpActiveDirectory(ctx, params, adUUID, accountId)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, createError, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("DatabaseErrorDuringCreation", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &ActiveDirectoryCreateActivity{
			SE: mockStorage,
		}

		ctx := context.Background()
		params := &common.CreateActiveDirectoryParams{
			ResourceId:                 "test-ad",
			Domain:                     "test.example.com",
			DNS:                        "8.8.8.8",
			NetBIOS:                    "TESTNETBIOS",
			OrganizationalUnit:         "OU=test",
			Username:                   "admin",
			Password:                   "password123",
			BackupOperators:            []string{},
			Administrators:             []string{},
			SecurityOperators:          []string{},
			KdcIP:                      "192.168.1.1",
			KdcHostname:                "example.com",
			AesEncryption:              true,
			EncryptDCConnections:       true,
			LdapSigning:                true,
			AllowLocalNFSUsersWithLdap: true,
			Description:                "Test AD",
		}
		adUUID := "test-ad-uuid"
		accountId := int64(123)

		mockStorage.On("GetActiveDirectoryByNameAndAccountID", ctx, params.ResourceId, accountId).Return(nil, customerrors.New("not found"))

		createError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, vsaerrors.New("database creation failed"))
		mockStorage.On("CreateActiveDirectory", ctx, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
			return ad.AdName == params.ResourceId &&
				ad.Username == params.Username &&
				ad.Domain == params.Domain &&
				ad.DNS == params.DNS &&
				ad.NetBIOS == params.NetBIOS &&
				ad.AccountId == accountId &&
				ad.UUID == adUUID &&
				ad.ActiveDirectoryAttributes.PrimaryAD == true
		})).Return(nil, createError)

		// Mock the password secret storage to prevent GCP authentication issues
		originalStorePasswordSecret := storePasswordSecret
		storePasswordSecret = func(gcpService hyperscaler.GoogleServices, password, secretID string) error {
			return nil // Mock successful secret storage
		}
		defer func() {
			storePasswordSecret = originalStorePasswordSecret
		}()

		result, err := activity.CreateVcpActiveDirectory(ctx, params, adUUID, accountId)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, createError, err)
		mockStorage.AssertExpectations(t)
	})
}

func TestActiveDirectoryCreateActivity_CreateSdeActiveDirectory(t *testing.T) {
	t.Run("PlaceholderImplementation", func(t *testing.T) {
		activity := &ActiveDirectoryCreateActivity{}

		ctx := context.Background()
		params := &common.CreateActiveDirectoryParams{
			ResourceId: "test-sde-ad",
		}

		result, err := activity.CreateSdeActiveDirectory(ctx, params)

		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("ReturnsCorrectType", func(t *testing.T) {
		activity := &ActiveDirectoryCreateActivity{}

		ctx := context.Background()
		params := &common.CreateActiveDirectoryParams{
			ResourceId: "test-sde-ad",
			Domain:     "sde.example.com",
		}

		result, err := activity.CreateSdeActiveDirectory(ctx, params)

		assert.NoError(t, err)
		assert.Nil(t, result)

		// Verify the function signature returns the correct type
		var expectedType *models.AggregateDistributionResult
		assert.IsType(t, expectedType, result)
	})
}

func TestStorePasswordSecret(t *testing.T) {
	t.Run("CreateNewSecretSuccess", func(t *testing.T) {
		mockGCPService := &hyperscaler.MockGoogleServices{}

		password := "test-password"
		secretID := "test-secret-id"

		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, secretID, password).
			Return(&hyperscalermodels.CustomSecret{SecretVersion: &hyperscalermodels.CustomSecretVersion{
				Value: "",
			}}, nil)

		err := storePasswordSecret(mockGCPService, password, secretID)

		assert.NoError(t, err)
		mockGCPService.AssertExpectations(t)
	})

	t.Run("PropagateCreateSecretError", func(t *testing.T) {
		mockGCPService := &hyperscaler.MockGoogleServices{}

		password := "test-password"
		secretID := "test-secret-id"

		createError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, vsaerrors.New("create secret failed"))
		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, secretID, password).Return(nil, createError)
		err := storePasswordSecret(mockGCPService, password, secretID)

		assert.Error(t, err)
		assert.Equal(t, createError, err)
		mockGCPService.AssertExpectations(t)
	})
}

func TestGeneratePasswordSecretId(t *testing.T) {
	t.Run("ValidSecretIdGeneration", func(t *testing.T) {
		projectID := "test-project"
		accountID := "test-account"
		adName := "test-ad"
		region := "us-central1"

		secretID := generatePasswordSecretId(projectID, accountID, adName, region)

		assert.NotEmpty(t, secretID)
		assert.True(t, len(secretID) == 20) // "gcnv-" (5 chars) + 15 hex chars
		assert.True(t, strings.HasPrefix(secretID, "gcnv-"))
	})

	t.Run("DeterministicGeneration", func(t *testing.T) {
		projectID := "test-project"
		adName := "test-ad"
		accountID := "test-account"
		region := "us-central1"

		secretID1 := generatePasswordSecretId(projectID, accountID, adName, region)
		secretID2 := generatePasswordSecretId(projectID, accountID, adName, region)

		assert.Equal(t, secretID1, secretID2)
	})

	t.Run("DifferentInputsProduceDifferentIds", func(t *testing.T) {
		secretID1 := generatePasswordSecretId("project1", "test-account", "ad1", "region1")
		secretID2 := generatePasswordSecretId("project2", "test-account", "ad2", "region2")

		assert.NotEqual(t, secretID1, secretID2)
	})

	t.Run("HandleEmptyInputs", func(t *testing.T) {
		secretID := generatePasswordSecretId("", "", "", "")

		assert.NotEmpty(t, secretID)
		assert.True(t, strings.HasPrefix(secretID, "gcnv-"))
		assert.True(t, len(secretID) == 20)
	})

	t.Run("HandleSpecialCharacters", func(t *testing.T) {
		projectID := "test-project-123"
		accountID := "acc!@#"
		adName := "test_ad-name"
		region := "us-central1-a"

		secretID := generatePasswordSecretId(projectID, accountID, adName, region)

		assert.NotEmpty(t, secretID)
		assert.True(t, strings.HasPrefix(secretID, "gcnv-"))
		assert.True(t, len(secretID) == 20)
	})
}

func TestActiveDirectoryCreateActivity_Constants(t *testing.T) {
	t.Run("VerifyConstants", func(t *testing.T) {
		assert.Equal(t, `BUILTIN\Backup Operators`, ActiveDirectoryGroupBuiltInBackupOperators)
		assert.Equal(t, `BUILTIN\Administrators`, ActiveDirectoryGroupBuiltInAdministrators)
		assert.Equal(t, `SeSecurityPrivilege`, ActiveDirectorySeSecurityPrivilege)
	})
}
