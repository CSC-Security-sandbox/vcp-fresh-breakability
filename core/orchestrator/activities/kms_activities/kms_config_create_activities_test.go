package kms_activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateKmsConfigSDEActivity(t *testing.T) {
	t.Run("CreateKmsConfigSDEActivityReturnsKmsConfigOnSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		params := &common.CreateKmsConfigParams{}
		mockClient := kms_configurations.NewMockClientService(t)
		// Define mock response
		kfp := "kfp"
		mockResponse := &kms_configurations.V1betaCreateKmsConfigurationAccepted{
			Payload: &models.OperationV1beta{
				Name:     "operation-id",
				Done:     nillable.GetBoolPtr(true),
				Response: models.KmsConfigV1beta{UUID: "test", KeyFullPath: &kfp},
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		activity := &KmsConfigActivity{SE: mockSE}

		_, err := activity.CreateKmsConfigSDEActivity(ctx, params)
		if err != nil {
			t.Fatal("expected no error, got error:", err)
		}
	})
	t.Run("CreateKmsConfigSDEActivityReturnsErrorOnCreateFailure", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		params := &common.CreateKmsConfigParams{}
		mockClient := kms_configurations.NewMockClientService(t)
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, errors.New("create error"))
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		activity := &KmsConfigActivity{SE: mockSE}
		_, err := activity.CreateKmsConfigSDEActivity(ctx, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("CreateKmsConfigSDEActivityReturnsErrorOnNilPayload", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		params := &common.CreateKmsConfigParams{}
		mockClient := kms_configurations.NewMockClientService(t)
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		activity := &KmsConfigActivity{SE: mockSE}
		_, err := activity.CreateKmsConfigSDEActivity(ctx, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestUpdateKmsConfigAttributesActivity(t *testing.T) {
	t.Run("WhenNoError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		expectedResult := &datamodel.KmsConfig{}
		mockSE.On("UpdateKmsConfigAttributes", ctx, mock.Anything, mock.Anything).Return(expectedResult, nil)
		activity := &KmsConfigActivity{SE: mockSE}
		result, err := activity.UpdateKmsConfigAttributesActivity(ctx, &datamodel.KmsConfig{}, &datamodel.KmsAttributes{})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if result != expectedResult {
			t.Fatalf("expected %v, got %v", expectedResult, result)
		}
	})
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		mockSE.On("UpdateKmsConfigAttributes", ctx, "test-uuid", mock.Anything).Return(nil, errors.New("update error"))
		_, err := activity.UpdateKmsConfigAttributesActivity(ctx, &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "test-uuid"}}, &datamodel.KmsAttributes{})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestCreateAndSyncKmsConfigActivity(t *testing.T) {
	t.Run("CreateAndSyncKmsConfigActivityReturnsKmsConfigOnSuccess", func(t *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		params := &common.CreateKmsConfigParams{
			AccountName:         "acc",
			KeyFullPath:         "projects/p/locations/l/keyRings/r/cryptoKeys/k",
			UUID:                "uuid",
			KmsState:            "ENABLED",
			KmsStateDetails:     "Active",
			ProjectNumber:       "123",
			ResourceID:          "res-id",
			Instructions:        "inst",
			ServiceAccountEmail: "sa@email.com",
		}
		mockAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}}
		mockSE.On("GetAccount", ctx, "acc").Return(mockAccount, nil)
		mockSE.On("CreateKmsConfig", ctx, mock.Anything).Return(&datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}}, nil)
		activity := &KmsConfigActivity{SE: mockSE}

		result, err := activity.CreateAndSyncKmsConfigActivity(ctx, params)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if result.UUID != "uuid" {
			t.Fatalf("expected uuid, got %v", result.UUID)
		}
	})
	t.Run("CreateAndSyncKmsConfigActivityReturnsErrorWhenGetAccountFails", func(t *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		params := &common.CreateKmsConfigParams{AccountName: "acc"}
		mockSE.On("GetAccount", ctx, "acc").Return(nil, errors.New("account error"))
		activity := &KmsConfigActivity{SE: mockSE}

		_, err := activity.CreateAndSyncKmsConfigActivity(ctx, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("CreateAndSyncKmsConfigActivityReturnsErrorWhenParseKeyFullPathResourceFails", func(t *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		params := &common.CreateKmsConfigParams{
			AccountName: "acc",
			KeyFullPath: "invalid-key-path",
		}
		mockAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}}
		mockSE.On("GetAccount", ctx, "acc").Return(mockAccount, nil)
		activity := &KmsConfigActivity{SE: mockSE}

		_, err := activity.CreateAndSyncKmsConfigActivity(ctx, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("CreateAndSyncKmsConfigActivityReturnsErrorWhenCreateKmsConfigFails", func(t *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		params := &common.CreateKmsConfigParams{
			AccountName: "acc",
			KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k",
		}
		mockAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}}
		mockSE.On("GetAccount", ctx, "acc").Return(mockAccount, nil)
		mockSE.On("CreateKmsConfig", ctx, mock.Anything).Return(nil, errors.New("create error"))
		activity := &KmsConfigActivity{SE: mockSE}

		_, err := activity.CreateAndSyncKmsConfigActivity(ctx, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

// TestCreateDnsActivity tests the CreateDnsActivity method.
func TestCreateDnsActivity(t *testing.T) {
	ctx := context.Background()
	node := &coreModels.Node{}

	t.Run("returns error if GetProviderByNode fails", func(t *testing.T) {
		// Patch activities.GetProviderByNode to return error
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *coreModels.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		activity := &KmsConfigActivity{}
		err := activity.CreateDnsActivity(ctx, node)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("returns error if provider.CreateDns fails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockProvider.On("CreateDns", mock.Anything).Return(errors.New("dns error"))

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *coreModels.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		activity := &KmsConfigActivity{}
		err := activity.CreateDnsActivity(ctx, node)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("Returns non-retriable error if provider.CreateDns is unable to reach node", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockProvider.On("CreateDns", mock.Anything).Return(errors.New("Retries exhausted when attempting to reach the storage server"))

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *coreModels.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		activity := &KmsConfigActivity{}
		err := activity.CreateDnsActivity(ctx, node)
		assert.Error(t, err)
		assert.EqualError(t, err, "Unable to create DNS: Node not reachable (type: CreateDNSError, retryable: false): unable to reach node")
	})

	t.Run("returns nil on success", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockProvider.On("CreateDns", mock.Anything).Return(nil)

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *coreModels.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		activity := &KmsConfigActivity{}
		err := activity.CreateDnsActivity(ctx, node)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}
