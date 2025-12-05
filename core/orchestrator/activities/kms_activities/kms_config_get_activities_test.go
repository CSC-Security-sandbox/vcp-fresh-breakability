package kms_activities

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestGetKmsConfigSDEActivity(t *testing.T) {
	t.Run("DescribeKmsConfigurationActivityReturnsKmsConfigOnSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		mockClient := kms_configurations.NewMockClientService(t)
		keyFullPath := "key-full-path"
		resourceID := "resource-id"
		uuid := "external-uuid"
		serviceAccountEmail := "svc@account"
		instructions := "instructions"
		mockResponse := &kms_configurations.V1betaDescribeKmsConfigurationOK{
			Payload: &models.KmsConfigV1beta{
				UUID:                uuid,
				KeyFullPath:         &keyFullPath,
				ResourceID:          &resourceID,
				ServiceAccountEmail: serviceAccountEmail,
				Instructions:        instructions,
			},
		}
		params := &common.GetKmsConfigParams{UUID: "SdeKmsConfigUUID",
			LocationID: "location"}
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		activity := &KmsConfigActivity{SE: mockSE}
		result, err := activity.DescribeSDEKmsConfigurationActivity(ctx, params)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		assert.NotNil(tt, result)
	})
	t.Run("DescribeKmsConfigurationActivityReturnsErrorOnDescribeFailure", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		params := &common.GetKmsConfigParams{UUID: "uuid",
			LocationID: "location"}
		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(nil, errors.New("describe error"))
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		activity := &KmsConfigActivity{SE: mockSE}
		_, err := activity.DescribeSDEKmsConfigurationActivity(ctx, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("DescribeKmsConfigurationActivityReturnsErrorOnNilPayload", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		params := &common.GetKmsConfigParams{UUID: "uuid",
			LocationID: "location"}
		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(&kms_configurations.V1betaDescribeKmsConfigurationOK{Payload: nil}, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := cvp.CreateClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		activity := &KmsConfigActivity{SE: mockSE}
		_, err := activity.DescribeSDEKmsConfigurationActivity(ctx, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestGetKmsConfigActivity(t *testing.T) {
	t.Run("GetKmsConfigActivityReturnsKmsConfigOnSuccess", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		uuid := "kms-uuid"
		expected := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: uuid}}
		mockSE.On("GetKmsConfig", ctx, uuid).Return(expected, nil)
		result, err := activity.GetKmsConfigActivity(ctx, uuid)
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
	t.Run("GetKmsConfigActivityReturnsNonRetryableErrorWhenNotFound", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		uuid := "not-found-uuid"
		notFoundErr := errors.NewNotFoundErr("not found", nil)
		mockSE.On("GetKmsConfig", ctx, uuid).Return(nil, notFoundErr)
		result, err := activity.GetKmsConfigActivity(ctx, uuid)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "not found")
	})
	t.Run("GetKmsConfigActivityReturnsErrorOnStorageFailure", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		uuid := "kms-uuid"
		storageErr := errors.New("db error")
		mockSE.On("GetKmsConfig", ctx, uuid).Return(nil, storageErr)
		result, err := activity.GetKmsConfigActivity(ctx, uuid)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "db error")
	})
}

func TestListKmsConfigActivity(t *testing.T) {
	t.Run("ListKMSConfigFailureOnGetAccountFailure", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		projectNumber := "1234567"
		storageErr := errors.New("Failed to get account")
		mockSE.EXPECT().GetAccount(mock.Anything, mock.Anything).Return(nil, storageErr)
		result, err := activity.ListKmsConfigActivity(ctx, projectNumber)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Failed to get account")
	})
	t.Run("ListKMSConfigFailureOnInvokingListKmsConfigByAccountID", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		projectNumber := "1234567"
		mockSE.EXPECT().GetAccount(mock.Anything, mock.Anything).Return(&datamodel.Account{Name: projectNumber}, nil)
		mockSE.EXPECT().ListKmsConfigByAccountID(mock.Anything, mock.Anything).Return(nil, errors.New("Failed to list KMS configs"))
		result, err := activity.ListKmsConfigActivity(ctx, projectNumber)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Failed to list KMS configs")
	})

	t.Run("ListKMSConfigWhenListKmsConfigByAccountIDReturnsEmpty", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		projectNumber := "1234567"
		mockSE.EXPECT().GetAccount(mock.Anything, mock.Anything).Return(&datamodel.Account{Name: projectNumber}, nil)
		mockSE.EXPECT().ListKmsConfigByAccountID(mock.Anything, mock.Anything).Return([]*datamodel.KmsConfig{}, nil)
		result, err := activity.ListKmsConfigActivity(ctx, projectNumber)
		assert.Nil(t, err)
		assert.Empty(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("ListKMSConfigWhenListKmsConfigByAccountIDReturnsKMSConfig", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		projectNumber := "1234567"
		mockSE.EXPECT().GetAccount(mock.Anything, mock.Anything).Return(&datamodel.Account{Name: projectNumber}, nil)
		kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "kms-uuid"}}
		mockSE.EXPECT().ListKmsConfigByAccountID(mock.Anything, mock.Anything).Return([]*datamodel.KmsConfig{kmsConfig}, nil)
		result, err := activity.ListKmsConfigActivity(ctx, projectNumber)
		assert.Nil(t, err)
		assert.NotEmpty(t, result)
		assert.Equal(t, 1, len(result))
	})
}

func TestGetSDEKmsConfiguration_JWTTokenExtraction(t *testing.T) {
	// Generated using AI
	t.Run("JWTTokenFromAuthTokenContext", func(tt *testing.T) {
		// Test when GetAuthTokenFromContext returns a token
		testJWTToken := "Bearer test-jwt-token-from-auth-context"
		ctx := context.WithValue(context.Background(), middleware.AuthorizationToken, testJWTToken)
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

		mockSE := database.NewMockStorage(t)
		mockClient := kms_configurations.NewMockClientService(t)
		uuid := "test-uuid"
		mockResponse := &kms_configurations.V1betaDescribeKmsConfigurationOK{
			Payload: &models.KmsConfigV1beta{
				UUID: uuid,
			},
		}
		params := &common.GetKmsConfigParams{
			UUID:       uuid,
			LocationID: "location",
		}
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		var capturedJWT string
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			capturedJWT = JWT
			return *cvpClient
		}

		activity := &KmsConfigActivity{SE: mockSE}
		_, err := activity.DescribeSDEKmsConfigurationActivity(ctx, params)
		assert.NoError(tt, err)
		assert.Equal(tt, testJWTToken, capturedJWT, "Should use token from GetAuthTokenFromContext")
	})

	t.Run("JWTTokenFallbackToHeaderContext", func(tt *testing.T) {
		// Test when GetAuthTokenFromContext returns empty and falls back to GetJWTTokenFromContext
		testJWTToken := "Bearer test-jwt-token-from-header"
		headers := http.Header{}
		headers.Set("Authorization", testJWTToken)
		ctx := context.WithValue(context.Background(), middleware.HeaderContextKey, headers)
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

		mockSE := database.NewMockStorage(t)
		mockClient := kms_configurations.NewMockClientService(t)
		uuid := "test-uuid"
		mockResponse := &kms_configurations.V1betaDescribeKmsConfigurationOK{
			Payload: &models.KmsConfigV1beta{
				UUID: uuid,
			},
		}
		params := &common.GetKmsConfigParams{
			UUID:       uuid,
			LocationID: "location",
		}
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		var capturedJWT string
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			capturedJWT = JWT
			return *cvpClient
		}

		activity := &KmsConfigActivity{SE: mockSE}
		_, err := activity.DescribeSDEKmsConfigurationActivity(ctx, params)
		assert.NoError(tt, err)
		assert.Equal(tt, testJWTToken, capturedJWT, "Should fallback to token from GetJWTTokenFromContext")
	})
}
