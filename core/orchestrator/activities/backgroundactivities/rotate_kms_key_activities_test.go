package backgroundactivities

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	hyperscalerModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestRotateKmsSAKeyActivity_ListKmsConfigs(t *testing.T) {
	ctx := context.Background()

	t.Run("ListKmsConfigsReturnsKmsConfigsOnSuccess", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		expectedKmsConfigs := []*datamodel.KmsConfig{
			{
				BaseModel: datamodel.BaseModel{UUID: "kms-uuid-1"},
				State:     string(gcpserver.KmsConfigV1betaKmsStateINUSE),
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "kms-uuid-2"},
				State:     string(gcpserver.KmsConfigV1betaKmsStateREADY),
			},
		}

		// Setup expected conditions for GetMultipleKmsConfigs
		expectedConditions := [][]interface{}{
			{"state in ?", []string{string(gcpserver.KmsConfigV1betaKmsStateINUSE), string(gcpserver.KmsConfigV1betaKmsStateREADY)}},
		}

		mockSE.On("GetMultipleKmsConfigs", ctx, expectedConditions).Return(expectedKmsConfigs, nil)

		result, err := activity.ListKmsConfigs(ctx)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedKmsConfigs, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListKmsConfigsReturnsErrorWhenDatabaseFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		dbError := errors.New("database connection error")
		expectedConditions := [][]interface{}{
			{"state in ?", []string{string(gcpserver.KmsConfigV1betaKmsStateINUSE), string(gcpserver.KmsConfigV1betaKmsStateREADY)}},
		}
		mockSE.On("GetMultipleKmsConfigs", ctx, expectedConditions).Return(nil, dbError)

		result, err := activity.ListKmsConfigs(ctx)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListKmsConfigsReturnsEmptyListWhenNoKmsConfigsFound", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		expectedConditions := [][]interface{}{
			{"state in ?", []string{string(gcpserver.KmsConfigV1betaKmsStateINUSE), string(gcpserver.KmsConfigV1betaKmsStateREADY)}},
		}
		mockSE.On("GetMultipleKmsConfigs", ctx, expectedConditions).Return([]*datamodel.KmsConfig{}, nil)

		result, err := activity.ListKmsConfigs(ctx)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
		mockSE.AssertExpectations(tt)
	})
}

func TestRotateKmsSAKeyActivity_RotateServiceAccountKey(t *testing.T) {
	ctx := context.Background()

	t.Run("RotateServiceAccountKeySuccessfulWithMultiplePools", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		serviceAccount := &datamodel.ServiceAccount{
			BaseModel:                      datamodel.BaseModel{UUID: "sa-uuid", ID: 1},
			ServiceAccountEmail:            "test@project.iam.gserviceaccount.com",
			ServiceName:                    serviceNameCmek,
			ServiceAccountPasswordLocation: "old-encrypted-service-account-key",
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "kms-uuid", ID: 123},
		}

		// Multiple pools to test the loop
		pools := []*datamodel.Pool{
			{BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"}},
			{BaseModel: datamodel.BaseModel{ID: 2, UUID: "pool-2"}},
		}

		// Use valid base64 encoded private key data
		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))
		newServiceAccountKey := &hyperscalerModels.ServiceAccountKey{
			Name:           "projects/test-project/serviceAccounts/test@project.iam.gserviceaccount.com/keys/new-key-id",
			PrivateKeyData: validPrivateKeyData,
		}

		// Mock dependencies
		originalGetGcpService := getGcpService
		originalGcpServiceCreateServiceAccountKey := gcpServiceCreateServiceAccountKey
		originalDeleteServiceAccountKeysExcludingKey := deleteServiceAccountKeysExcludingKey
		originalUtilsEncryptPassword := utils.EncryptPassword
		originalListPoolsByKmsConfigId := listPoolsByKmsConfigId
		originalSyncKeyWithOntap := syncKeyWithOntap
		originalExtractKeyID := extractKeyID
		defer func() {
			getGcpService = originalGetGcpService
			gcpServiceCreateServiceAccountKey = originalGcpServiceCreateServiceAccountKey
			deleteServiceAccountKeysExcludingKey = originalDeleteServiceAccountKeysExcludingKey
			utils.EncryptPassword = originalUtilsEncryptPassword
			listPoolsByKmsConfigId = originalListPoolsByKmsConfigId
			syncKeyWithOntap = originalSyncKeyWithOntap
			extractKeyID = originalExtractKeyID
		}()

		mockGcpService := &google.GcpServices{}
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}
		gcpServiceCreateServiceAccountKey = func(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*hyperscalerModels.ServiceAccountKey, error) {
			assert.Equal(tt, serviceAccount.ServiceAccountEmail, email)
			return newServiceAccountKey, nil
		}
		deleteServiceAccountKeysExcludingKey = func(ctx context.Context, gcpService *google.GcpServices, email, keyToExclude string) error {
			assert.Equal(tt, serviceAccount.ServiceAccountEmail, email)
			assert.Equal(tt, newServiceAccountKey.Name, keyToExclude)
			return nil
		}
		encryptedPassword := "encrypted-password"
		utils.EncryptPassword = func(secret log.Secret) (*string, error) {
			// Verify the password being encrypted is the private key data
			assert.Equal(tt, log.Secret(validPrivateKeyData), secret)
			return &encryptedPassword, nil
		}
		listPoolsByKmsConfigId = func(ctx context.Context, kmsConfigId int64, se database.Storage) ([]*datamodel.Pool, error) {
			assert.Equal(tt, kmsConfig.ID, kmsConfigId)
			return pools, nil
		}
		syncKeyWithOntap = func(ctx context.Context, se database.Storage, newServiceAccountKey string, oldServiceAccountKey string, pool *datamodel.Pool) error {
			assert.Equal(tt, validPrivateKeyData, newServiceAccountKey)
			assert.Equal(tt, serviceAccount.ServiceAccountPasswordLocation, oldServiceAccountKey)
			assert.Contains(tt, []int64{1, 2}, pool.ID)
			return nil
		}
		extractKeyID = func(serviceAccountKey string) (string, error) {
			return "old-key-id", nil
		}

		// Mock database calls
		updatedServiceAccount := &datamodel.ServiceAccount{
			BaseModel:                      serviceAccount.BaseModel,
			ServiceAccountEmail:            serviceAccount.ServiceAccountEmail,
			ServiceAccountPasswordLocation: validPrivateKeyData,
		}
		mockSE.On("UpdateServiceAccountEmailAndKey", ctx, serviceAccount.UUID, serviceAccount.ServiceAccountEmail, validPrivateKeyData).Return(updatedServiceAccount, nil)

		// Mock AccessCryptoKey
		originalAccessCryptoKey := kms_activities.AccessCryptoKeyAndEncryptData
		defer func() { kms_activities.AccessCryptoKeyAndEncryptData = originalAccessCryptoKey }()
		kms_activities.AccessCryptoKeyAndEncryptData = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, secretPassword string, timeout, timeoutInterval time.Duration) error {
			assert.Equal(tt, encryptedPassword, secretPassword)
			assert.Equal(tt, "kms-uuid", kmsConfig.UUID)
			return nil
		}

		err := activity.RotateServiceAccountKey(ctx, serviceAccount, kmsConfig)

		assert.NoError(tt, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("RotateServiceAccountKeySuccessfulWithNoPools", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		serviceAccount := &datamodel.ServiceAccount{
			BaseModel:                      datamodel.BaseModel{UUID: "sa-uuid", ID: 1},
			ServiceAccountEmail:            "test@project.iam.gserviceaccount.com",
			ServiceName:                    serviceNameCmek,
			ServiceAccountPasswordLocation: "old-encrypted-service-account-key",
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "kms-uuid", ID: 123},
		}

		// Use valid base64 encoded private key data
		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))
		newServiceAccountKey := &hyperscalerModels.ServiceAccountKey{
			Name:           "projects/test-project/serviceAccounts/test@project.iam.gserviceaccount.com/keys/new-key-id",
			PrivateKeyData: validPrivateKeyData,
		}

		// Mock dependencies
		originalGetGcpService := getGcpService
		originalGcpServiceCreateServiceAccountKey := gcpServiceCreateServiceAccountKey
		originalDeleteServiceAccountKeysExcludingKey := deleteServiceAccountKeysExcludingKey
		originalUtilsEncryptPassword := utils.EncryptPassword
		originalListPoolsByKmsConfigId := listPoolsByKmsConfigId
		originalExtractKeyID := extractKeyID
		defer func() {
			getGcpService = originalGetGcpService
			gcpServiceCreateServiceAccountKey = originalGcpServiceCreateServiceAccountKey
			deleteServiceAccountKeysExcludingKey = originalDeleteServiceAccountKeysExcludingKey
			utils.EncryptPassword = originalUtilsEncryptPassword
			listPoolsByKmsConfigId = originalListPoolsByKmsConfigId
			extractKeyID = originalExtractKeyID
		}()

		mockGcpService := &google.GcpServices{}
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}
		gcpServiceCreateServiceAccountKey = func(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*hyperscalerModels.ServiceAccountKey, error) {
			return newServiceAccountKey, nil
		}
		deleteServiceAccountKeysExcludingKey = func(ctx context.Context, gcpService *google.GcpServices, email, keyToExclude string) error {
			return nil
		}
		encryptedPassword := "encrypted-password"
		utils.EncryptPassword = func(secret log.Secret) (*string, error) {
			return &encryptedPassword, nil
		}
		listPoolsByKmsConfigId = func(ctx context.Context, kmsConfigId int64, se database.Storage) ([]*datamodel.Pool, error) {
			// Return empty pool list
			return []*datamodel.Pool{}, nil
		}
		extractKeyID = func(serviceAccountKey string) (string, error) {
			return "old-key-id", nil
		}

		// Mock database calls
		mockSE.On("UpdateServiceAccountEmailAndKey", ctx, serviceAccount.UUID, serviceAccount.ServiceAccountEmail, validPrivateKeyData).Return(&datamodel.ServiceAccount{}, nil)

		// Mock AccessCryptoKey
		originalAccessCryptoKey := kms_activities.AccessCryptoKeyAndEncryptData
		defer func() { kms_activities.AccessCryptoKeyAndEncryptData = originalAccessCryptoKey }()
		kms_activities.AccessCryptoKeyAndEncryptData = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, secretPassword string, timeout, timeoutInterval time.Duration) error {
			return nil
		}

		err := activity.RotateServiceAccountKey(ctx, serviceAccount, kmsConfig)

		assert.NoError(tt, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("RotateServiceAccountKeyFailsWhenGetGcpServiceFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		serviceAccount := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail: "test@project.iam.gserviceaccount.com",
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		}

		originalGetGcpService := getGcpService
		originalExtractKeyID := extractKeyID
		defer func() {
			getGcpService = originalGetGcpService
			extractKeyID = originalExtractKeyID
		}()

		// Mock extractKeyID to succeed so we can test the getGcpService failure
		extractKeyID = func(serviceAccountKey string) (string, error) {
			return "existing-key-id", nil
		}

		gcpError := errors.New("failed to get GCP service")
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, gcpError
		}

		err := activity.RotateServiceAccountKey(ctx, serviceAccount, kmsConfig)

		assert.Error(tt, err)
		assert.Equal(tt, gcpError, err)
	})

	t.Run("RotateServiceAccountKeyFailsWhenCreateServiceAccountKeyFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		serviceAccount := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid"},
			ServiceAccountEmail: "test@project.iam.gserviceaccount.com",
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		}

		originalGetGcpService := getGcpService
		originalGcpServiceCreateServiceAccountKey := gcpServiceCreateServiceAccountKey
		originalExtractKeyID := extractKeyID
		defer func() {
			getGcpService = originalGetGcpService
			gcpServiceCreateServiceAccountKey = originalGcpServiceCreateServiceAccountKey
			extractKeyID = originalExtractKeyID
		}()

		// Mock extractKeyID to succeed so we can test the createServiceAccountKey failure
		extractKeyID = func(serviceAccountKey string) (string, error) {
			return "existing-key-id", nil
		}

		mockGcpService := &google.GcpServices{}
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}

		createKeyError := errors.New("failed to create service account key")
		gcpServiceCreateServiceAccountKey = func(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*hyperscalerModels.ServiceAccountKey, error) {
			return nil, createKeyError
		}

		err := activity.RotateServiceAccountKey(ctx, serviceAccount, kmsConfig)

		assert.Error(tt, err)
		assert.Equal(tt, createKeyError, err)
	})

	t.Run("RotateServiceAccountKeyFailsWhenSyncKeyWithOntapFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		serviceAccount := &datamodel.ServiceAccount{
			BaseModel:                      datamodel.BaseModel{UUID: "sa-uuid", ID: 1},
			ServiceAccountEmail:            "test@project.iam.gserviceaccount.com",
			ServiceAccountPasswordLocation: "old-encrypted-service-account-key",
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "kms-uuid", ID: 123},
		}

		// Use valid base64 encoded private key data
		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))
		newServiceAccountKey := &hyperscalerModels.ServiceAccountKey{
			Name:           "projects/test-project/serviceAccounts/test@project.iam.gserviceaccount.com/keys/new-key-id",
			PrivateKeyData: validPrivateKeyData,
		}

		originalGetGcpService := getGcpService
		originalGcpServiceCreateServiceAccountKey := gcpServiceCreateServiceAccountKey
		originalListPoolsByKmsConfigId := listPoolsByKmsConfigId
		originalSyncKeyWithOntap := syncKeyWithOntap
		originalUtilsEncryptPassword := utils.EncryptPassword
		originalDeleteServiceAccountKeysExcludingKey := deleteServiceAccountKeysExcludingKey
		originalExtractKeyID := extractKeyID
		defer func() {
			getGcpService = originalGetGcpService
			gcpServiceCreateServiceAccountKey = originalGcpServiceCreateServiceAccountKey
			listPoolsByKmsConfigId = originalListPoolsByKmsConfigId
			syncKeyWithOntap = originalSyncKeyWithOntap
			utils.EncryptPassword = originalUtilsEncryptPassword
			deleteServiceAccountKeysExcludingKey = originalDeleteServiceAccountKeysExcludingKey
			extractKeyID = originalExtractKeyID
		}()

		// Mock extractKeyID to succeed so we can test the sync failure
		extractKeyID = func(serviceAccountKey string) (string, error) {
			return "existing-key-id", nil
		}

		mockGcpService := &google.GcpServices{}
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}
		gcpServiceCreateServiceAccountKey = func(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*hyperscalerModels.ServiceAccountKey, error) {
			return newServiceAccountKey, nil
		}
		deleteServiceAccountKeysExcludingKey = func(ctx context.Context, gcpService *google.GcpServices, email, keyToExclude string) error {
			return nil
		}

		// Mock EncryptPassword to avoid calling the real function
		encryptedPassword := "encrypted-password"
		utils.EncryptPassword = func(secret log.Secret) (*string, error) {
			return &encryptedPassword, nil
		}

		// Mock AccessCryptoKey to avoid calling the real function
		originalAccessCryptoKey := kms_activities.AccessCryptoKeyAndEncryptData
		defer func() { kms_activities.AccessCryptoKeyAndEncryptData = originalAccessCryptoKey }()
		kms_activities.AccessCryptoKeyAndEncryptData = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, secretPassword string, timeout, timeoutInterval time.Duration) error {
			return nil
		}

		// Mock listPoolsByKmsConfigId to return a pool
		listPoolsByKmsConfigId = func(ctx context.Context, kmsConfigId int64, se database.Storage) ([]*datamodel.Pool, error) {
			return []*datamodel.Pool{{BaseModel: datamodel.BaseModel{ID: 1}}}, nil
		}

		// Mock syncKeyWithOntap to fail
		syncError := errors.New("sync failed")
		syncKeyCalled := false
		syncKeyWithOntap = func(ctx context.Context, se database.Storage, newServiceAccountKey string, oldServiceAccountKey string, pool *datamodel.Pool) error {
			syncKeyCalled = true
			assert.Equal(tt, validPrivateKeyData, newServiceAccountKey)
			assert.Equal(tt, serviceAccount.ServiceAccountPasswordLocation, oldServiceAccountKey)
			return syncError
		}

		err := activity.RotateServiceAccountKey(ctx, serviceAccount, kmsConfig)

		// Verify that syncKeyWithOntap was called and the error was returned
		assert.True(tt, syncKeyCalled, "syncKeyWithOntap should have been called")
		assert.Error(tt, err)
		assert.Equal(tt, syncError, err)
	})

	t.Run("RotateServiceAccountKeyFailsWhenUpdateServiceAccountEmailAndKeyFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		serviceAccount := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid", ID: 1},
			ServiceAccountEmail: "test@project.iam.gserviceaccount.com",
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		}

		// Use valid base64 encoded private key data
		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))
		newServiceAccountKey := &hyperscalerModels.ServiceAccountKey{
			Name:           "projects/test-project/serviceAccounts/test@project.iam.gserviceaccount.com/keys/new-key-id",
			PrivateKeyData: validPrivateKeyData,
		}

		originalGetGcpService := getGcpService
		originalGcpServiceCreateServiceAccountKey := gcpServiceCreateServiceAccountKey
		originalUtilsEncryptPassword := utils.EncryptPassword
		originalExtractKeyID := extractKeyID
		originalDeleteServiceAccountKeysExcludingKey := deleteServiceAccountKeysExcludingKey
		originalListPoolsByKmsConfigId := listPoolsByKmsConfigId
		defer func() {
			getGcpService = originalGetGcpService
			gcpServiceCreateServiceAccountKey = originalGcpServiceCreateServiceAccountKey
			utils.EncryptPassword = originalUtilsEncryptPassword
			listPoolsByKmsConfigId = originalListPoolsByKmsConfigId
			extractKeyID = originalExtractKeyID
			deleteServiceAccountKeysExcludingKey = originalDeleteServiceAccountKeysExcludingKey
		}()

		extractKeyID = func(encryptedCreds string) (string, error) {
			return "existing-key-id", nil
		}

		mockGcpService := &google.GcpServices{}
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}
		gcpServiceCreateServiceAccountKey = func(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*hyperscalerModels.ServiceAccountKey, error) {
			return newServiceAccountKey, nil
		}
		deleteServiceAccountKeysExcludingKey = func(ctx context.Context, gcpService *google.GcpServices, email, keyToExclude string) error {
			return nil
		}
		encryptedPassword := "encrypted-password"
		utils.EncryptPassword = func(secret log.Secret) (*string, error) {
			return &encryptedPassword, nil
		}

		listPoolsByKmsConfigId = func(ctx context.Context, kmsConfigId int64, se database.Storage) ([]*datamodel.Pool, error) {
			// Return empty pool list so we skip sync and go to update
			return []*datamodel.Pool{}, nil
		}
		// Mock AccessCryptoKey to succeed, but UpdateServiceAccountEmailAndKey to fail
		originalAccessCryptoKey := kms_activities.AccessCryptoKeyAndEncryptData
		defer func() { kms_activities.AccessCryptoKeyAndEncryptData = originalAccessCryptoKey }()
		kms_activities.AccessCryptoKeyAndEncryptData = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, secretPassword string, timeout, timeoutInterval time.Duration) error {
			return nil
		}

		updateError := errors.New("failed to update service account")
		updateCalled := false
		mockSE.On("UpdateServiceAccountEmailAndKey", ctx, serviceAccount.UUID, serviceAccount.ServiceAccountEmail, validPrivateKeyData).
			Run(func(args mock.Arguments) { updateCalled = true }).
			Return(nil, updateError)

		err := activity.RotateServiceAccountKey(ctx, serviceAccount, kmsConfig)

		// Verify that UpdateServiceAccountEmailAndKey was called and the error was returned
		assert.True(tt, updateCalled, "UpdateServiceAccountEmailAndKey should have been called")
		assert.Error(tt, err)
		assert.Equal(tt, updateError, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("RotateServiceAccountKeyFailsWhenAccessCryptoKeyFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		serviceAccount := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid", ID: 1},
			ServiceAccountEmail: "test@project.iam.gserviceaccount.com",
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		}

		// Use valid base64 encoded private key data
		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))
		newServiceAccountKey := &hyperscalerModels.ServiceAccountKey{
			Name:           "projects/test-project/serviceAccounts/test@project.iam.gserviceaccount.com/keys/new-key-id",
			PrivateKeyData: validPrivateKeyData,
		}

		originalGetGcpService := getGcpService
		originalGcpServiceCreateServiceAccountKey := gcpServiceCreateServiceAccountKey
		originalUtilsEncryptPassword := utils.EncryptPassword
		originalExtractKeyID := extractKeyID
		defer func() {
			getGcpService = originalGetGcpService
			gcpServiceCreateServiceAccountKey = originalGcpServiceCreateServiceAccountKey
			utils.EncryptPassword = originalUtilsEncryptPassword
			extractKeyID = originalExtractKeyID
		}()

		// Mock extractKeyID to succeed so we can test the AccessCryptoKey failure
		extractKeyID = func(serviceAccountKey string) (string, error) {
			return "existing-key-id", nil
		}

		mockGcpService := &google.GcpServices{}
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}
		gcpServiceCreateServiceAccountKey = func(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*hyperscalerModels.ServiceAccountKey, error) {
			return newServiceAccountKey, nil
		}
		encryptedPassword := "encrypted-password"
		utils.EncryptPassword = func(secret log.Secret) (*string, error) {
			return &encryptedPassword, nil
		}

		accessKeyError := errors.New("failed to access crypto key")
		originalAccessCryptoKey := kms_activities.AccessCryptoKeyAndEncryptData
		defer func() { kms_activities.AccessCryptoKeyAndEncryptData = originalAccessCryptoKey }()
		kms_activities.AccessCryptoKeyAndEncryptData = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, secretPassword string, timeout, timeoutInterval time.Duration) error {
			return accessKeyError
		}

		err := activity.RotateServiceAccountKey(ctx, serviceAccount, kmsConfig)

		assert.Error(tt, err)
		assert.Equal(tt, accessKeyError, err)
	})

	t.Run("RotateServiceAccountKeyFailsWhenEncryptPasswordFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		serviceAccount := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid", ID: 1},
			ServiceAccountEmail: "test@project.iam.gserviceaccount.com",
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		}

		// Use valid base64 encoded private key data
		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))
		newServiceAccountKey := &hyperscalerModels.ServiceAccountKey{
			Name:           "projects/test-project/serviceAccounts/test@project.iam.gserviceaccount.com/keys/new-key-id",
			PrivateKeyData: validPrivateKeyData,
		}

		originalGetGcpService := getGcpService
		originalGcpServiceCreateServiceAccountKey := gcpServiceCreateServiceAccountKey
		originalUtilsEncryptPassword := utils.EncryptPassword
		originalExtractKeyID := extractKeyID
		defer func() {
			getGcpService = originalGetGcpService
			gcpServiceCreateServiceAccountKey = originalGcpServiceCreateServiceAccountKey
			utils.EncryptPassword = originalUtilsEncryptPassword
			extractKeyID = originalExtractKeyID
		}()

		// Mock extractKeyID to succeed so we can test the EncryptPassword failure
		extractKeyID = func(serviceAccountKey string) (string, error) {
			return "existing-key-id", nil
		}

		mockGcpService := &google.GcpServices{}
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}
		gcpServiceCreateServiceAccountKey = func(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*hyperscalerModels.ServiceAccountKey, error) {
			return newServiceAccountKey, nil
		}

		encryptError := errors.New("failed to encrypt password")
		utils.EncryptPassword = func(secret log.Secret) (*string, error) {
			return nil, encryptError
		}

		err := activity.RotateServiceAccountKey(ctx, serviceAccount, kmsConfig)

		assert.Error(tt, err)
		assert.Equal(tt, encryptError, err)
	})

	t.Run("RotateServiceAccountKeyFailsWhenDeleteServiceAccountKeysExcludingKeyFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		serviceAccount := &datamodel.ServiceAccount{
			BaseModel:           datamodel.BaseModel{UUID: "sa-uuid", ID: 1},
			ServiceAccountEmail: "test@project.iam.gserviceaccount.com",
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		}

		// Use valid base64 encoded private key data
		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))
		newServiceAccountKey := &hyperscalerModels.ServiceAccountKey{
			Name:           "projects/test-project/serviceAccounts/test@project.iam.gserviceaccount.com/keys/new-key-id",
			PrivateKeyData: validPrivateKeyData,
		}

		originalGetGcpService := getGcpService
		originalGcpServiceCreateServiceAccountKey := gcpServiceCreateServiceAccountKey
		originalDeleteServiceAccountKeysExcludingKey := deleteServiceAccountKeysExcludingKey
		originalUtilsEncryptPassword := utils.EncryptPassword
		originalExtractKeyID := extractKeyID
		originalListPoolsByKmsConfigId := listPoolsByKmsConfigId
		defer func() {
			getGcpService = originalGetGcpService
			gcpServiceCreateServiceAccountKey = originalGcpServiceCreateServiceAccountKey
			deleteServiceAccountKeysExcludingKey = originalDeleteServiceAccountKeysExcludingKey
			utils.EncryptPassword = originalUtilsEncryptPassword
			listPoolsByKmsConfigId = originalListPoolsByKmsConfigId
			extractKeyID = originalExtractKeyID
		}()

		extractKeyID = func(encryptedCreds string) (string, error) {
			return "existing-key-id", nil
		}

		mockGcpService := &google.GcpServices{}
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}
		gcpServiceCreateServiceAccountKey = func(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*hyperscalerModels.ServiceAccountKey, error) {
			return newServiceAccountKey, nil
		}
		encryptedPassword := "encrypted-password"
		utils.EncryptPassword = func(secret log.Secret) (*string, error) {
			return &encryptedPassword, nil
		}

		deleteKeysError := errors.New("failed to delete old service account keys")
		deleteServiceAccountKeysExcludingKey = func(ctx context.Context, gcpService *google.GcpServices, email, keyToExclude string) error {
			return deleteKeysError
		}
		listPoolsByKmsConfigId = func(ctx context.Context, kmsConfigId int64, se database.Storage) ([]*datamodel.Pool, error) {
			// Return empty pool list so we skip sync and go to update
			return []*datamodel.Pool{}, nil
		}
		mockSE.On("UpdateServiceAccountEmailAndKey", ctx, serviceAccount.UUID, serviceAccount.ServiceAccountEmail, validPrivateKeyData).Return(&datamodel.ServiceAccount{}, nil)

		// Mock AccessCryptoKey
		originalAccessCryptoKey := kms_activities.AccessCryptoKeyAndEncryptData
		defer func() { kms_activities.AccessCryptoKeyAndEncryptData = originalAccessCryptoKey }()
		kms_activities.AccessCryptoKeyAndEncryptData = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, secretPassword string, timeout, timeoutInterval time.Duration) error {
			return nil
		}

		err := activity.RotateServiceAccountKey(ctx, serviceAccount, kmsConfig)

		// Since deleteServiceAccountKeysExcludingKey is called in a defer block,
		// errors from it are not returned by the main function
		assert.NoError(tt, err)
		mockSE.AssertExpectations(tt)
	})
}

func TestRotateKmsSAKeyActivity_Integration(t *testing.T) {
	ctx := context.Background()

	t.Run("FullRotationWorkflowIntegrationTest", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockSE}

		// Setup service accounts for listing
		serviceAccounts := []*datamodel.ServiceAccount{
			{
				BaseModel:           datamodel.BaseModel{UUID: "sa-uuid-1", ID: 1},
				ServiceAccountEmail: "sa1@project.iam.gserviceaccount.com",
				ServiceName:         serviceNameCmek,
			},
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel:      datamodel.BaseModel{UUID: "kms-uuid"},
			ServiceAccount: serviceAccounts[0],
		}

		// Use valid base64 encoded private key data
		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))
		newServiceAccountKey := &hyperscalerModels.ServiceAccountKey{
			Name:           "projects/test-project/serviceAccounts/sa1@project.iam.gserviceaccount.com/keys/new-key-id",
			PrivateKeyData: validPrivateKeyData,
		}

		// Mock all dependencies for the complete workflow
		originalGetGcpService := getGcpService
		originalGcpServiceCreateServiceAccountKey := gcpServiceCreateServiceAccountKey
		originalDeleteServiceAccountKeysExcludingKey := deleteServiceAccountKeysExcludingKey
		originalUtilsEncryptPassword := utils.EncryptPassword
		originalListPoolsByKmsConfigId := listPoolsByKmsConfigId
		originalExtractKeyID := extractKeyID
		defer func() {
			getGcpService = originalGetGcpService
			gcpServiceCreateServiceAccountKey = originalGcpServiceCreateServiceAccountKey
			deleteServiceAccountKeysExcludingKey = originalDeleteServiceAccountKeysExcludingKey
			utils.EncryptPassword = originalUtilsEncryptPassword
			listPoolsByKmsConfigId = originalListPoolsByKmsConfigId
			extractKeyID = originalExtractKeyID
		}()

		mockGcpService := &google.GcpServices{}
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}
		gcpServiceCreateServiceAccountKey = func(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*hyperscalerModels.ServiceAccountKey, error) {
			return newServiceAccountKey, nil
		}
		deleteServiceAccountKeysExcludingKey = func(ctx context.Context, gcpService *google.GcpServices, email, keyToExclude string) error {
			return nil
		}
		listPoolsByKmsConfigId = func(ctx context.Context, kmsConfigId int64, se database.Storage) ([]*datamodel.Pool, error) {
			// Return empty pool list
			return []*datamodel.Pool{}, nil
		}
		extractKeyID = func(serviceAccountKey string) (string, error) {
			return "existing-key-id", nil
		}
		encryptedPassword := "encrypted-password"
		utils.EncryptPassword = func(secret log.Secret) (*string, error) {
			return &encryptedPassword, nil
		}

		// Mock AccessCryptoKey
		originalAccessCryptoKey := kms_activities.AccessCryptoKeyAndEncryptData
		defer func() { kms_activities.AccessCryptoKeyAndEncryptData = originalAccessCryptoKey }()
		kms_activities.AccessCryptoKeyAndEncryptData = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, secretPassword string, timeout, timeoutInterval time.Duration) error {
			return nil
		}

		// Setup database expectations
		mockSE.On("GetMultipleKmsConfigs", ctx, mock.Anything).Return([]*datamodel.KmsConfig{kmsConfig}, nil)
		mockSE.On("UpdateServiceAccountEmailAndKey", ctx, serviceAccounts[0].UUID, serviceAccounts[0].ServiceAccountEmail, validPrivateKeyData).Return(&datamodel.ServiceAccount{}, nil)

		// Test the complete workflow
		// 1. List KMS configs
		listResult, err := activity.ListKmsConfigs(ctx)
		assert.NoError(tt, err)
		assert.Len(tt, listResult, 1)

		// 2. Rotate key for each service account
		for _, kmsConfig := range listResult {
			err = activity.RotateServiceAccountKey(ctx, kmsConfig.ServiceAccount, kmsConfig)
			assert.NoError(tt, err)
		}

		mockSE.AssertExpectations(tt)
	})
}

func Test_syncKeyWithOntap(t *testing.T) {
	ctx := context.Background()

	t.Run("SyncKeyWithOntapSuccessful", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "svm-uuid",
			},
			SvmDetails: &datamodel.SvmDetails{
				ExternalKmsConfigUUID: "kms-config-uuid",
			},
		}

		// Valid base64 encoded service account key
		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))

		// Mock GetSvmForPoolID
		mockSE.On("GetSvmForPoolID", ctx, pool.ID).Return(svm, nil)

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()

		mockProvider := vsa.NewMockProvider(tt)
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock ModifyGcpKms
		mockProvider.On("ModifyGcpKms", "kms-config-uuid", mock.AnythingOfType("*log.Secret")).Return(nil, nil, nil)

		// Mock IsGcpKmsReachable
		mockProvider.On("IsGcpKmsReachable", vsa.GetKmsConfigParams{
			ExternalKmsConfigID: "kms-config-uuid",
		}).Return(true, nil)

		err := _syncKeyWithOntap(ctx, mockSE, validPrivateKeyData, "old-key-data", pool)

		assert.NoError(tt, err)
		mockSE.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SyncKeyWithOntapFailsWhenGetSvmForPoolIDFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		}

		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))

		svmError := errors.New("failed to get SVM")
		mockSE.On("GetSvmForPoolID", ctx, pool.ID).Return(nil, svmError)

		err := _syncKeyWithOntap(ctx, mockSE, validPrivateKeyData, "old-key-data", pool)

		assert.Error(tt, err)
		assert.Equal(tt, svmError, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("SyncKeyWithOntapFailsWhenGetOntapRestProviderFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "svm-uuid",
			},
			SvmDetails: &datamodel.SvmDetails{
				ExternalKmsConfigUUID: "kms-config-uuid",
			},
		}

		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))

		mockSE.On("GetSvmForPoolID", ctx, pool.ID).Return(svm, nil)

		// Mock GetOntapRestProviderForPool to fail
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()

		providerError := errors.New("failed to get ONTAP provider")
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return nil, providerError
		}

		err := _syncKeyWithOntap(ctx, mockSE, validPrivateKeyData, "old-key-data", pool)

		assert.Error(tt, err)
		assert.Equal(tt, providerError, err)
		mockSE.AssertExpectations(tt)
	})

	t.Run("SyncKeyWithOntapFailsWhenBase64DecodeFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "svm-uuid",
			},
			SvmDetails: &datamodel.SvmDetails{
				ExternalKmsConfigUUID: "kms-config-uuid",
			},
		}

		// Invalid base64 string
		invalidBase64Key := "invalid-base64!@#$"

		mockSE.On("GetSvmForPoolID", ctx, pool.ID).Return(svm, nil)

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()

		mockProvider := vsa.NewMockProvider(tt)
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Even with invalid base64, ModifyGcpKms might be called with the raw string
		mockProvider.On("ModifyGcpKms", "kms-config-uuid", mock.AnythingOfType("*log.Secret")).Return(nil, nil, nil).Maybe()
		mockProvider.On("IsGcpKmsReachable", mock.Anything).Return(true, nil).Maybe()

		err := _syncKeyWithOntap(ctx, mockSE, invalidBase64Key, "old-key-data", pool)

		// The function should return an error for invalid base64
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "illegal base64 data")
		mockSE.AssertExpectations(tt)
	})

	t.Run("SyncKeyWithOntapFailsWhenModifyGcpKmsFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "svm-uuid",
			},
			SvmDetails: &datamodel.SvmDetails{
				ExternalKmsConfigUUID: "kms-config-uuid",
			},
		}

		validPrivateKeyData := base64.StdEncoding.EncodeToString([]byte("test-private-key-content"))

		mockSE.On("GetSvmForPoolID", ctx, pool.ID).Return(svm, nil)

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()

		mockProvider := vsa.NewMockProvider(tt)
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		modifyError := errors.New("failed to modify GCP KMS")
		mockProvider.On("ModifyGcpKms", "kms-config-uuid", mock.AnythingOfType("*log.Secret")).Return(nil, nil, modifyError)

		err := _syncKeyWithOntap(ctx, mockSE, validPrivateKeyData, "old-key-data", pool)

		assert.Error(tt, err)
		assert.Equal(tt, modifyError, err)
		mockSE.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})
}

func TestListPoolsByKmsConfigId(t *testing.T) {
	ctx := context.Background()

	t.Run("ListPoolsByKmsConfigIdReturnsPoolsOnSuccess", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)

		kmsConfigId := int64(123)
		expectedPoolViews := []*datamodel.PoolView{
			{Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-uuid-1", ID: 1},
				Name:      "pool-1",
			},
			},
			{Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "pool-uuid-2", ID: 2},
				Name:      "pool-2",
			},
			},
		}

		// Setup expected filter conditions
		mockSE.On("ListPools", ctx, mock.MatchedBy(func(filter *utils2.Filter) bool {
			// Verify the filter has the correct condition
			return len(filter.Conditions) == 2 &&
				filter.Conditions[0].Field == "kms_config_id" &&
				filter.Conditions[0].Op == "=" &&
				filter.Conditions[0].Value == kmsConfigId &&
				filter.Conditions[1].Field == "state" &&
				filter.Conditions[1].Op == "!=" &&
				filter.Conditions[1].Value == gcpserver.PoolV1betaStoragePoolStateERROR
		})).Return(expectedPoolViews, nil)

		result, err := ListPoolsByKmsConfigId(ctx, kmsConfigId, mockSE)

		assert.NoError(tt, err)
		assert.Len(tt, result, 2)
		// Verify pools are properly converted from PoolView
		for i, pool := range result {
			assert.Equal(tt, expectedPoolViews[i].UUID, pool.UUID)
			assert.Equal(tt, expectedPoolViews[i].ID, pool.ID)
		}
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListPoolsByKmsConfigIdReturnsErrorWhenDatabaseFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)

		kmsConfigId := int64(123)
		dbError := errors.New("database connection error")

		mockSE.On("ListPools", ctx, mock.Anything).Return(nil, dbError)

		result, err := ListPoolsByKmsConfigId(ctx, kmsConfigId, mockSE)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListPoolsByKmsConfigIdReturnsEmptyListWhenNoPoolsFound", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)

		kmsConfigId := int64(123)

		mockSE.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

		result, err := ListPoolsByKmsConfigId(ctx, kmsConfigId, mockSE)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListPoolsByKmsConfigIdHandlesNilPoolViews", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)

		kmsConfigId := int64(123)

		mockSE.On("ListPools", ctx, mock.Anything).Return(nil, nil)

		result, err := ListPoolsByKmsConfigId(ctx, kmsConfigId, mockSE)

		assert.NoError(tt, err)
		assert.Empty(tt, result)
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListPoolsByKmsConfigIdWithLargeDataset", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)

		kmsConfigId := int64(123)

		// Create a large dataset of pool views
		var expectedPoolViews []*datamodel.PoolView
		for i := 0; i < 100; i++ {
			expectedPoolViews = append(expectedPoolViews, &datamodel.PoolView{
				Pool: datamodel.Pool{
					BaseModel: datamodel.BaseModel{
						UUID: fmt.Sprintf("pool-uuid-%d", i),
						ID:   int64(i),
					},
					Name: fmt.Sprintf("pool-%d", i),
				},
			})
		}

		mockSE.On("ListPools", ctx, mock.Anything).Return(expectedPoolViews, nil)

		result, err := ListPoolsByKmsConfigId(ctx, kmsConfigId, mockSE)

		assert.NoError(tt, err)
		assert.Len(tt, result, 100)
		// Verify first and last pool to ensure proper conversion
		assert.Equal(tt, "pool-uuid-0", result[0].UUID)
		assert.Equal(tt, "pool-uuid-99", result[99].UUID)
		mockSE.AssertExpectations(tt)
	})

	t.Run("ListPoolsByKmsConfigIdWithDifferentKmsConfigIds", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)

		testCases := []int64{0, -1, 999999999}

		for _, kmsConfigId := range testCases {
			mockSE.On("ListPools", ctx, mock.MatchedBy(func(filter *utils2.Filter) bool {
				return filter.Conditions[0].Value == kmsConfigId
			})).Return([]*datamodel.PoolView{}, nil).Once()

			result, err := ListPoolsByKmsConfigId(ctx, kmsConfigId, mockSE)

			assert.NoError(tt, err)
			assert.Empty(tt, result)
		}

		mockSE.AssertExpectations(tt)
	})
}

// New tests for GetKmsConfigServiceAccount method
func TestRotateKmsSAKeyActivity_GetKmsConfigServiceAccount(t *testing.T) {
	ctx := context.Background()

	t.Run("GetKmsConfigServiceAccount_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)

		activity := &RotateKmsSAKeyActivity{
			SE: mockStorage,
		}

		kmsConfigID := "test-kms-config-uuid"
		serviceAccountID := int64(123)

		// Test data
		serviceAccount := &datamodel.ServiceAccount{
			BaseModel: datamodel.BaseModel{
				ID:   serviceAccountID,
				UUID: "test-sa-uuid",
			},
			Name:                "test-service-account",
			ServiceAccountEmail: "test-sa@test-project.iam.gserviceaccount.com",
			ServiceName:         "cmek",
		}

		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{
				UUID: kmsConfigID,
			},
			Name:             "test-kms-config",
			ServiceAccountID: &serviceAccountID,
			ServiceAccount:   serviceAccount, // Preloaded
		}

		// Set up expectations
		mockStorage.EXPECT().GetKmsConfigByUUID(ctx, kmsConfigID).Return(kmsConfig, nil)

		// Execute
		result, err := activity.GetKmsConfigServiceAccount(ctx, kmsConfigID)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, serviceAccount.UUID, result.UUID)
		assert.Equal(tt, serviceAccount.ServiceAccountEmail, result.ServiceAccountEmail)
		assert.Equal(tt, serviceAccount.Name, result.Name)

		// Verify expectations
		mockStorage.AssertExpectations(tt)
	})

	t.Run("GetKmsConfigServiceAccount_KmsConfigNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)

		activity := &RotateKmsSAKeyActivity{
			SE: mockStorage,
		}

		kmsConfigID := "non-existent-kms-config"

		// Set up expectations
		mockStorage.EXPECT().GetKmsConfigByUUID(ctx, kmsConfigID).Return(nil, errors.New("KMS config not found"))

		// Execute
		result, err := activity.GetKmsConfigServiceAccount(ctx, kmsConfigID)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, result)

		// Verify expectations
		mockStorage.AssertExpectations(tt)
	})

	t.Run("GetKmsConfigServiceAccount_NoServiceAccountID", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)

		activity := &RotateKmsSAKeyActivity{
			SE: mockStorage,
		}

		kmsConfigID := "test-kms-config-uuid"

		// Test data - KMS config without service account ID
		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{
				UUID: kmsConfigID,
			},
			Name:             "test-kms-config",
			ServiceAccountID: nil, // No service account ID
		}

		// Set up expectations
		mockStorage.EXPECT().GetKmsConfigByUUID(ctx, kmsConfigID).Return(kmsConfig, nil)

		// Execute
		result, err := activity.GetKmsConfigServiceAccount(ctx, kmsConfigID)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "no service account associated with KMS config")

		// Verify expectations
		mockStorage.AssertExpectations(tt)
	})

	t.Run("GetKmsConfigServiceAccount_ServiceAccountNotPreloaded", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)

		activity := &RotateKmsSAKeyActivity{
			SE: mockStorage,
		}

		kmsConfigID := "test-kms-config-uuid"
		serviceAccountID := int64(123)

		// Test data - KMS config with service account ID but not preloaded
		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{
				UUID: kmsConfigID,
			},
			Name:             "test-kms-config",
			ServiceAccountID: &serviceAccountID,
			ServiceAccount:   nil, // Not preloaded
		}

		// Set up expectations
		mockStorage.EXPECT().GetKmsConfigByUUID(ctx, kmsConfigID).Return(kmsConfig, nil)

		// Execute
		result, err := activity.GetKmsConfigServiceAccount(ctx, kmsConfigID)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "service account not found for KMS config")

		// Verify expectations
		mockStorage.AssertExpectations(tt)
	})

	t.Run("GetKmsConfigServiceAccount_ZeroServiceAccountID", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)

		activity := &RotateKmsSAKeyActivity{
			SE: mockStorage,
		}

		kmsConfigID := "test-kms-config-uuid"
		serviceAccountID := int64(0) // Zero ID

		// Test data - KMS config with zero service account ID
		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{
				UUID: kmsConfigID,
			},
			Name:             "test-kms-config",
			ServiceAccountID: &serviceAccountID,
		}

		// Set up expectations
		mockStorage.EXPECT().GetKmsConfigByUUID(ctx, kmsConfigID).Return(kmsConfig, nil)

		// Execute
		result, err := activity.GetKmsConfigServiceAccount(ctx, kmsConfigID)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "no service account associated with KMS config")
	})
}

// Tests for GetKmsConfig method
func TestRotateKmsSAKeyActivity_GetKmsConfig(t *testing.T) {
	ctx := context.Background()

	t.Run("GetKmsConfig_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockStorage}

		kmsConfigID := "test-kms-config-id"
		expectedKmsConfig := &datamodel.KmsConfig{
			BaseModel:    datamodel.BaseModel{UUID: kmsConfigID},
			State:        string(gcpserver.KmsConfigV1betaKmsStateINUSE),
			KeyProjectID: "test-project",
		}

		// Mock the database call
		mockStorage.EXPECT().GetKmsConfigByUUID(ctx, kmsConfigID).Return(expectedKmsConfig, nil)

		// Execute
		result, err := activity.GetKmsConfig(ctx, kmsConfigID)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedKmsConfig.UUID, result.UUID)
		assert.Equal(tt, expectedKmsConfig.State, result.State)
		assert.Equal(tt, expectedKmsConfig.KeyProjectID, result.KeyProjectID)
	})

	t.Run("GetKmsConfig_KmsConfigNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockStorage}

		kmsConfigID := "non-existent-kms-config-id"
		dbError := errors.New("KMS config not found")

		// Mock the database call to return error
		mockStorage.EXPECT().GetKmsConfigByUUID(ctx, kmsConfigID).Return(nil, dbError)

		// Execute
		result, err := activity.GetKmsConfig(ctx, kmsConfigID)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "An internal error occurred")
	})

	t.Run("GetKmsConfig_DatabaseError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &RotateKmsSAKeyActivity{SE: mockStorage}

		kmsConfigID := "test-kms-config-id"
		dbError := errors.New("database connection failed")

		// Mock the database call to return error
		mockStorage.EXPECT().GetKmsConfigByUUID(ctx, kmsConfigID).Return(nil, dbError)

		// Execute
		result, err := activity.GetKmsConfig(ctx, kmsConfigID)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "An internal error occurred")
	})
}

// Tests for _extractKeyID function
func Test_extractKeyID(t *testing.T) {
	t.Run("extractKeyID_Success", func(tt *testing.T) {
		// Create a valid service account key JSON
		keyData := map[string]interface{}{
			"type":                        "service_account",
			"project_id":                  "test-project",
			"private_key_id":              "test-key-id-123",
			"private_key":                 "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
			"client_email":                "test@test-project.iam.gserviceaccount.com",
			"client_id":                   "123456789",
			"auth_uri":                    "https://accounts.google.com/o/oauth2/auth",
			"token_uri":                   "https://oauth2.googleapis.com/token",
			"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
		}

		// Convert to JSON
		keyBytes, err := json.Marshal(keyData)
		assert.NoError(tt, err)

		// Base64 encode
		encodedKey := base64.StdEncoding.EncodeToString(keyBytes)

		// Encrypt the key (mock the encryption)
		originalEncryptPassword := utils.EncryptPassword
		defer func() { utils.EncryptPassword = originalEncryptPassword }()
		utils.EncryptPassword = func(password log.Secret) (*string, error) {
			encryptedValue := string(password)
			return &encryptedValue, nil
		}

		// Mock decrypt password
		originalDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = originalDecryptPassword }()
		utils.DecryptPassword = func(encryptedPassword log.Secret) (*string, error) {
			decryptedValue := encodedKey
			return &decryptedValue, nil
		}

		// Execute
		result, err := _extractKeyID("encrypted-service-account-key")

		// Assert
		assert.NoError(tt, err)
		assert.Equal(tt, "test-key-id-123", result)
	})

	t.Run("extractKeyID_DecryptionFailed", func(tt *testing.T) {
		// Mock decrypt password to fail
		originalDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = originalDecryptPassword }()
		utils.DecryptPassword = func(encryptedPassword log.Secret) (*string, error) {
			return nil, errors.New("decryption failed")
		}

		// Execute
		result, err := _extractKeyID("invalid-encrypted-key")

		// Assert
		assert.Error(tt, err)
		assert.Empty(tt, result)
		assert.Contains(tt, err.Error(), "failed to decrypt service account key")
	})

	t.Run("extractKeyID_InvalidBase64", func(tt *testing.T) {
		// Mock decrypt password to return invalid base64
		originalDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = originalDecryptPassword }()
		utils.DecryptPassword = func(encryptedPassword log.Secret) (*string, error) {
			invalidBase64 := "invalid-base64-data!!!"
			return &invalidBase64, nil
		}

		// Execute
		result, err := _extractKeyID("encrypted-service-account-key")

		// Assert
		assert.Error(tt, err)
		assert.Empty(tt, result)
	})

	t.Run("extractKeyID_InvalidJSON", func(tt *testing.T) {
		// Mock decrypt password to return invalid JSON
		originalDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = originalDecryptPassword }()
		utils.DecryptPassword = func(encryptedPassword log.Secret) (*string, error) {
			invalidJSON := base64.StdEncoding.EncodeToString([]byte("invalid-json"))
			return &invalidJSON, nil
		}

		// Execute
		result, err := _extractKeyID("encrypted-service-account-key")

		// Assert
		assert.Error(tt, err)
		assert.Empty(tt, result)
		assert.Contains(tt, err.Error(), "failed to unmarshal credentials")
	})

	t.Run("extractKeyID_MissingPrivateKeyID", func(tt *testing.T) {
		// Create service account key JSON without private_key_id
		keyData := map[string]interface{}{
			"type":         "service_account",
			"project_id":   "test-project",
			"client_email": "test@test-project.iam.gserviceaccount.com",
			// Missing private_key_id
		}

		// Convert to JSON and base64 encode
		keyBytes, err := json.Marshal(keyData)
		assert.NoError(tt, err)
		encodedKey := base64.StdEncoding.EncodeToString(keyBytes)

		// Mock decrypt password
		originalDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = originalDecryptPassword }()
		utils.DecryptPassword = func(encryptedPassword log.Secret) (*string, error) {
			return &encodedKey, nil
		}

		// Execute
		result, err := _extractKeyID("encrypted-service-account-key")

		// Assert
		assert.Error(tt, err)
		assert.Empty(tt, result)
		assert.Contains(tt, err.Error(), "key not found or not a string")
	})

	t.Run("extractKeyID_PrivateKeyIDNotString", func(tt *testing.T) {
		// Create service account key JSON with private_key_id as non-string
		keyData := map[string]interface{}{
			"type":           "service_account",
			"project_id":     "test-project",
			"private_key_id": 12345, // Not a string
			"client_email":   "test@test-project.iam.gserviceaccount.com",
		}

		// Convert to JSON and base64 encode
		keyBytes, err := json.Marshal(keyData)
		assert.NoError(tt, err)
		encodedKey := base64.StdEncoding.EncodeToString(keyBytes)

		// Mock decrypt password
		originalDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = originalDecryptPassword }()
		utils.DecryptPassword = func(encryptedPassword log.Secret) (*string, error) {
			return &encodedKey, nil
		}

		// Execute
		result, err := _extractKeyID("encrypted-service-account-key")

		// Assert
		assert.Error(tt, err)
		assert.Empty(tt, result)
		assert.Contains(tt, err.Error(), "key not found or not a string")
	})
}
