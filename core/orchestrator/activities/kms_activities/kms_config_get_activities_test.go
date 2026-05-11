package kms_activities

import (
	"context"
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
		originalGetSignedJwtToken := getSignedJwtToken
		defer func() {
			createClient = originalCreateClient
			getSignedJwtToken = originalGetSignedJwtToken
		}()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "mock-jwt-token", nil
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
		originalGetSignedJwtToken := getSignedJwtToken
		defer func() {
			createClient = originalCreateClient
			getSignedJwtToken = originalGetSignedJwtToken
		}()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "mock-jwt-token", nil
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
		originalGetSignedJwtToken := getSignedJwtToken
		defer func() {
			createClient = originalCreateClient
			getSignedJwtToken = originalGetSignedJwtToken
		}()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "mock-jwt-token", nil
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
	t.Run("DescribeKmsConfigurationActivityReturnsErrorOnTokenFailure", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		params := &common.GetKmsConfigParams{UUID: "uuid",
			LocationID: "location"}
		originalGetSignedJwtToken := getSignedJwtToken
		defer func() {
			getSignedJwtToken = originalGetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", errors.New("failed to get signed token")
		}
		activity := &KmsConfigActivity{SE: mockSE}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.DescribeSDEKmsConfigurationActivity)
		_, err := env.ExecuteActivity(activity.DescribeSDEKmsConfigurationActivity, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		assert.Contains(tt, err.Error(), "failed to get signed token")
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

func TestGetSDEKmsConfiguration_JWTTokenGeneration(t *testing.T) {
	t.Run("JWTTokenGeneratedFromProjectNumber", func(tt *testing.T) {
		// Test that JWT token is generated using project number from params
		mockSE := database.NewMockStorage(t)
		mockClient := kms_configurations.NewMockClientService(t)
		uuid := "test-uuid"
		projectNumber := "123456789"
		mockResponse := &kms_configurations.V1betaDescribeKmsConfigurationOK{
			Payload: &models.KmsConfigV1beta{
				UUID: uuid,
			},
		}
		params := &common.GetKmsConfigParams{
			UUID:          uuid,
			LocationID:    "location",
			ProjectNumber: projectNumber,
		}
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		originalGetSignedJwtToken := getSignedJwtToken
		defer func() {
			createClient = originalCreateClient
			getSignedJwtToken = originalGetSignedJwtToken
		}()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		var capturedProjectNumber string
		getSignedJwtToken = func(pn string) (string, error) {
			capturedProjectNumber = pn
			return "mock-jwt-token", nil
		}

		activity := &KmsConfigActivity{SE: mockSE}
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.DescribeSDEKmsConfigurationActivity)
		_, err := env.ExecuteActivity(activity.DescribeSDEKmsConfigurationActivity, params)
		assert.NoError(tt, err)
		assert.Equal(tt, projectNumber, capturedProjectNumber, "JWT token should be generated using project number from params")
	})
}

// TestGetSDEKmsConfiguration_NonActivityContext verifies that _getSDEKmsConfiguration can be called
// from a non-activity context (e.g., HTTP handlers) without panicking.
// Before the fix (VSCP-4440), this test would have panicked due to activity.RecordHeartbeat
// being called outside of a Temporal activity context.
func TestGetSDEKmsConfiguration_NonActivityContext(t *testing.T) {
	t.Run("DoesNotPanicWhenCalledFromNonActivityContext", func(tt *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)
		uuid := "test-uuid"
		mockResponse := &kms_configurations.V1betaDescribeKmsConfigurationOK{
			Payload: &models.KmsConfigV1beta{
				UUID: uuid,
			},
		}
		params := &common.GetKmsConfigParams{
			UUID:          uuid,
			LocationID:    "us-central1",
			ProjectNumber: "123456789",
		}
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}

		originalCreateClient := createClient
		originalGetSignedJwtToken := getSignedJwtToken
		defer func() {
			createClient = originalCreateClient
			getSignedJwtToken = originalGetSignedJwtToken
		}()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "mock-jwt-token", nil
		}

		// Call helper directly with context.Background() - NO activity context
		// This simulates how it's called from HTTP handlers
		assert.NotPanics(tt, func() {
			result, err := _getSDEKmsConfiguration(context.Background(), params)
			assert.NoError(tt, err)
			assert.NotNil(tt, result)
			assert.Equal(tt, uuid, result.UUID)
		})
	})
}

func TestConvertToCreateKmsConfigParams(t *testing.T) {
	t.Run("MapsSdeKmsFieldsAndPoolParamsIncludingAccountName", func(tt *testing.T) {
		accountName := "846223794136"
		region := "us-east4"
		keyFullPath := "projects/p/locations/us-east4/keyRings/r/cryptoKeys/k"
		resourceID := "cmek-east4"
		desc := "KMS description"
		kmsUUID := "646df1f1-896c-cc0b-c22c-0d1e65ce64dd"
		saEmail := "n-cmek-auso1-644189374367@429679891893.iam.gserviceaccount.com"
		instructions := "run gcloud ..."
		kmsState := "READY"
		kmsStateDetails := "Ready for use"

		params := &models.KmsConfigV1beta{
			UUID:                kmsUUID,
			KmsState:            kmsState,
			KmsStateDetails:     kmsStateDetails,
			ServiceAccountEmail: saEmail,
			Instructions:        instructions,
			KeyFullPath:         &keyFullPath,
			Description:         &desc,
			ResourceID:          &resourceID,
		}
		createPoolParams := &common.CreatePoolParams{
			AccountName: accountName,
			Region:      region,
		}

		got := ConvertToCreateKmsConfigParams(params, createPoolParams)

		assert.Equal(tt, accountName, got.AccountName, "AccountName must match pool AccountName for GetAccount / DB attribution")
		assert.Equal(tt, accountName, got.ProjectNumber)
		assert.Equal(tt, kmsUUID, got.UUID)
		assert.Equal(tt, kmsState, got.KmsState)
		assert.Equal(tt, kmsStateDetails, got.KmsStateDetails)
		assert.Equal(tt, saEmail, got.ServiceAccountEmail)
		assert.Equal(tt, instructions, got.Instructions)
		assert.Equal(tt, region, got.LocationID)
		assert.Equal(tt, desc, got.Description)
		assert.Equal(tt, keyFullPath, got.KeyFullPath)
		assert.Equal(tt, resourceID, got.ResourceID)
	})

	t.Run("MapsWhenOptionalDescriptionResourceIdNil", func(tt *testing.T) {
		accountName := "123456789"
		keyFullPath := "projects/p/locations/loc/keyRings/r/cryptoKeys/k"
		params := &models.KmsConfigV1beta{
			UUID:                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
			KmsState:            "IN_USE",
			KmsStateDetails:     "Available for use",
			ServiceAccountEmail: "svc@example.com",
			Instructions:        "inst",
			KeyFullPath:         &keyFullPath,
			Description:         nil,
			ResourceID:          nil,
		}
		createPoolParams := &common.CreatePoolParams{
			AccountName: accountName,
			Region:      "europe-west1",
		}

		got := ConvertToCreateKmsConfigParams(params, createPoolParams)

		assert.Equal(tt, accountName, got.AccountName)
		assert.Equal(tt, accountName, got.ProjectNumber)
		assert.Empty(tt, got.Description)
		assert.Empty(tt, got.ResourceID)
	})
}
