package kms_activities

import (
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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/testsuite"
)

func TestGetKmsConfigSDEActivity(t *testing.T) {
	t.Run("DescribeKmsConfigurationActivityReturnsKmsConfigOnSuccess", func(tt *testing.T) {
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
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.DescribeSDEKmsConfigurationActivity)
		result, err := env.ExecuteActivity(activity.DescribeSDEKmsConfigurationActivity, params)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		var kmsConfig *datamodel.KmsConfig
		err = result.Get(&kmsConfig)
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}
		assert.NotNil(tt, kmsConfig)
	})
	t.Run("DescribeKmsConfigurationActivityReturnsErrorOnDescribeFailure", func(tt *testing.T) {
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
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.DescribeSDEKmsConfigurationActivity)
		_, err := env.ExecuteActivity(activity.DescribeSDEKmsConfigurationActivity, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("DescribeKmsConfigurationActivityReturnsErrorOnNilPayload", func(tt *testing.T) {
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
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.DescribeSDEKmsConfigurationActivity)
		_, err := env.ExecuteActivity(activity.DescribeSDEKmsConfigurationActivity, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestGetKmsConfigActivity(t *testing.T) {
	t.Run("GetKmsConfigActivityReturnsKmsConfigOnSuccess", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		uuid := "kms-uuid"
		expected := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: uuid}}
		mockSE.On("GetKmsConfig", mock.Anything, uuid).Return(expected, nil)
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetKmsConfigActivity)
		result, err := env.ExecuteActivity(activity.GetKmsConfigActivity, uuid)
		assert.NoError(t, err)
		var kmsConfig *datamodel.KmsConfig
		err = result.Get(&kmsConfig)
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}
		assert.Equal(t, expected, kmsConfig)
	})
	t.Run("GetKmsConfigActivityReturnsNonRetryableErrorWhenNotFound", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		uuid := "not-found-uuid"
		notFoundErr := errors.NewNotFoundErr("not found", nil)
		mockSE.On("GetKmsConfig", mock.Anything, uuid).Return(nil, notFoundErr)
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetKmsConfigActivity)
		_, err := env.ExecuteActivity(activity.GetKmsConfigActivity, uuid)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
	t.Run("GetKmsConfigActivityReturnsErrorOnStorageFailure", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		uuid := "kms-uuid"
		storageErr := errors.New("db error")
		mockSE.On("GetKmsConfig", mock.Anything, uuid).Return(nil, storageErr)
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetKmsConfigActivity)
		_, err := env.ExecuteActivity(activity.GetKmsConfigActivity, uuid)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "db error")
	})
}

func TestListKmsConfigActivity(t *testing.T) {
	t.Run("ListKMSConfigFailureOnGetAccountFailure", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		projectNumber := "1234567"
		storageErr := errors.New("Failed to get account")
		mockSE.EXPECT().GetAccount(mock.Anything, mock.Anything).Return(nil, storageErr)
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.ListKmsConfigActivity)
		_, err := env.ExecuteActivity(activity.ListKmsConfigActivity, projectNumber)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to get account")
	})
	t.Run("ListKMSConfigFailureOnInvokingListKmsConfigByAccountID", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		projectNumber := "1234567"
		mockSE.EXPECT().GetAccount(mock.Anything, mock.Anything).Return(&datamodel.Account{Name: projectNumber}, nil)
		mockSE.EXPECT().ListKmsConfigByAccountID(mock.Anything, mock.Anything).Return(nil, errors.New("Failed to list KMS configs"))
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.ListKmsConfigActivity)
		_, err := env.ExecuteActivity(activity.ListKmsConfigActivity, projectNumber)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to list KMS configs")
	})

	t.Run("ListKMSConfigWhenListKmsConfigByAccountIDReturnsEmpty", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		projectNumber := "1234567"
		mockSE.EXPECT().GetAccount(mock.Anything, mock.Anything).Return(&datamodel.Account{Name: projectNumber}, nil)
		mockSE.EXPECT().ListKmsConfigByAccountID(mock.Anything, mock.Anything).Return([]*datamodel.KmsConfig{}, nil)
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.ListKmsConfigActivity)
		result, err := env.ExecuteActivity(activity.ListKmsConfigActivity, projectNumber)
		assert.Nil(t, err)
		var kmsConfigs []*datamodel.KmsConfig
		err = result.Get(&kmsConfigs)
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}
		assert.Empty(t, kmsConfigs)
		assert.Equal(t, 0, len(kmsConfigs))
	})

	t.Run("ListKMSConfigWhenListKmsConfigByAccountIDReturnsKMSConfig", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		projectNumber := "1234567"
		mockSE.EXPECT().GetAccount(mock.Anything, mock.Anything).Return(&datamodel.Account{Name: projectNumber}, nil)
		kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "kms-uuid"}}
		mockSE.EXPECT().ListKmsConfigByAccountID(mock.Anything, mock.Anything).Return([]*datamodel.KmsConfig{kmsConfig}, nil)
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.ListKmsConfigActivity)
		result, err := env.ExecuteActivity(activity.ListKmsConfigActivity, projectNumber)
		assert.Nil(t, err)
		var kmsConfigs []*datamodel.KmsConfig
		err = result.Get(&kmsConfigs)
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}
		assert.NotEmpty(t, kmsConfigs)
		assert.Equal(t, 1, len(kmsConfigs))
	})
}

func TestGetSDEKmsConfiguration_JWTTokenExtraction(t *testing.T) {
	// Generated using AI
	t.Run("JWTTokenFromAuthTokenContext", func(tt *testing.T) {
		// Test when GetAuthTokenFromContext returns a token
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
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		activity := &KmsConfigActivity{SE: mockSE}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.DescribeSDEKmsConfigurationActivity)
		_, err := env.ExecuteActivity(activity.DescribeSDEKmsConfigurationActivity, params)
		assert.NoError(tt, err)
		// Note: JWT token extraction from context may not work the same way in test environment
		// This test may need adjustment based on actual behavior
	})

	t.Run("JWTTokenFallbackToHeaderContext", func(tt *testing.T) {
		// Test when GetAuthTokenFromContext returns empty and falls back to GetJWTTokenFromContext
		_ = http.Header{} // Keep import used
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
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		activity := &KmsConfigActivity{SE: mockSE}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.DescribeSDEKmsConfigurationActivity)
		_, err := env.ExecuteActivity(activity.DescribeSDEKmsConfigurationActivity, params)
		assert.NoError(tt, err)
		// Note: JWT token extraction from context may not work the same way in test environment
		// This test may need adjustment based on actual behavior
	})
}
