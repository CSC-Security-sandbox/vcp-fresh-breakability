package api

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/client"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaCoreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// V1betaCreateKmsConfiguration unittests
func TestV1betaCreateKmsConfigurations(t *testing.T) {
	t.Run("CreateKmsConfigurationReturnsBadRequestWhenKeyFullPathIsInvalid", func(t *testing.T) {
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		req := &gcpgenserver.KmsConfigV1beta{KeyFullPath: "invalid"}
		handler := Handler{}
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaCreateKmsConfigurationBadRequest).Code)
		assert.Equal(t, "Invalid KeyFullPath format", result.(*gcpgenserver.V1betaCreateKmsConfigurationBadRequest).Message)
	})
	t.Run("CreateKmsConfigurationReturnsBadRequestWhenLocationIdIsInvalid", func(t *testing.T) {
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
		}

		req := &gcpgenserver.KmsConfigV1beta{KeyFullPath: "projects/test-project/locations/us-central1/keyRings/test-keyring/cryptoKeys/test-key"}
		handler := Handler{}
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaCreateKmsConfigurationBadRequest).Code)
		assert.Equal(t, "LocationID represents neither a region nor a zone", result.(*gcpgenserver.V1betaCreateKmsConfigurationBadRequest).Message)
	})
	t.Run("CreateKmsConfigurationFailsWithConflictError", func(t *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		req := &gcpgenserver.KmsConfigV1beta{KeyFullPath: "projects/test-project/locations/us-central1/keyRings/test-keyring/cryptoKeys/test-key"}

		mockClient := kms_configurations.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		// Define mock response
		mockResponse := &kms_configurations.V1betaCreateKmsConfigurationAccepted{
			Payload: &models.OperationV1beta{
				Name:     "operation-id",
				Done:     nillable.GetBoolPtr(false),
				Response: models.KmsConfigV1beta{UUID: "test", KeyFullPath: nil},
			},
		}
		mockClient.On("V1betaCreateKmsConfiguration", mock.Anything).Return(mockResponse, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfigByKeyFullPath(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("KMS configuration not found", nil))
		mockOrchestrator.EXPECT().CreateKmsConfig(mock.Anything, mock.Anything).Return(nil, "", errors.NewConflictErr("some error"))
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(http.StatusConflict), result.(*gcpgenserver.V1betaCreateKmsConfigurationConflict).Code)
	})
	t.Run("CreateKmsConfigurationFails", func(t *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		req := &gcpgenserver.KmsConfigV1beta{KeyFullPath: "projects/test-project/locations/us-central1/keyRings/test-keyring/cryptoKeys/test-key"}

		mockClient := kms_configurations.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		// Define mock response
		mockResponse := &kms_configurations.V1betaCreateKmsConfigurationAccepted{
			Payload: &models.OperationV1beta{
				Name:     "operation-id",
				Done:     nillable.GetBoolPtr(false),
				Response: models.KmsConfigV1beta{UUID: "test", KeyFullPath: nil},
			},
		}
		mockClient.On("V1betaCreateKmsConfiguration", mock.Anything).Return(mockResponse, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfigByKeyFullPath(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("KMS configuration not found", nil))
		mockOrchestrator.EXPECT().CreateKmsConfig(mock.Anything, mock.Anything).Return(nil, "", errors.New("some error"))
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(500), result.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Code)
	})
	t.Run("V1betaCreateKmsConfigurationFails", func(t *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		req := &gcpgenserver.KmsConfigV1beta{KeyFullPath: "projects/test-project/locations/us-central1/keyRings/test-keyring/cryptoKeys/test-key"}

		mockClient := kms_configurations.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		origParseKmsConfigResponse := parseKmsConfigResponse
		defer func() {
			createClient = originalCreateClient
			parseKmsConfigResponse = origParseKmsConfigResponse
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		mockClient.On("V1betaCreateKmsConfiguration", mock.Anything).Return(nil, errors.New("some error"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfigByKeyFullPath(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("KMS configuration not found", nil))
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(500), result.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Code)
	})
	t.Run("ParseKmsConfigResponseFails", func(t *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		req := &gcpgenserver.KmsConfigV1beta{KeyFullPath: "projects/test-project/locations/us-central1/keyRings/test-keyring/cryptoKeys/test-key"}

		mockClient := kms_configurations.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		origParseKmsConfigResponse := parseKmsConfigResponse
		defer func() {
			createClient = originalCreateClient
			parseKmsConfigResponse = origParseKmsConfigResponse
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		// Define mock response
		mockResponse := &kms_configurations.V1betaCreateKmsConfigurationAccepted{
			Payload: &models.OperationV1beta{
				Name:     "operation-id",
				Done:     nillable.GetBoolPtr(false),
				Response: "not-a-json-object",
			},
		}
		parseKmsConfigResponse = func(payloadResponse interface{}) (*models.KmsConfigV1beta, error) {
			return nil, errors.New("some error")
		}
		mockClient.On("V1betaCreateKmsConfiguration", mock.Anything).Return(mockResponse, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfigByKeyFullPath(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("KMS configuration not found", nil))
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(500), result.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Code)
	})
	t.Run("CreateKmsConfigurationSuccess", func(t *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		kmsConfig := &vsaCoreModels.KmsConfig{KmsAttributes: &vsaCoreModels.KmsAttributes{}}

		req := &gcpgenserver.KmsConfigV1beta{KeyFullPath: "projects/test-project/locations/us-central1/keyRings/test-keyring/cryptoKeys/test-key"}

		mockClient := kms_configurations.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		// Define mock response
		mockResponse := &kms_configurations.V1betaCreateKmsConfigurationAccepted{
			Payload: &models.OperationV1beta{
				Name:     "operation-id",
				Done:     nillable.GetBoolPtr(true),
				Response: models.KmsConfigV1beta{UUID: "test", KeyFullPath: nil},
			},
		}
		mockClient.On("V1betaCreateKmsConfiguration", mock.Anything).Return(mockResponse, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfigByKeyFullPath(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("KMS configuration not found", nil))
		operationID := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, "operation-id")
		mockOrchestrator.EXPECT().CreateKmsConfig(mock.Anything, mock.Anything).Return(kmsConfig, "operation-id", nil)
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, operationID, result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
	t.Run("GetKmsConfigByKeyFullPathFails", func(t *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		req := &gcpgenserver.KmsConfigV1beta{KeyFullPath: "projects/test-project/locations/us-central1/keyRings/test-keyring/cryptoKeys/test-key"}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfigByKeyFullPath(mock.Anything, mock.Anything).Return(nil, errors.New("some other error"))
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(500), result.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Code)
	})
	t.Run("GetKmsConfigByKeyFullPathReturnsKmsConfigInErrorState", func(t *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		req := &gcpgenserver.KmsConfigV1beta{KeyFullPath: "projects/test-project/locations/us-central1/keyRings/test-keyring/cryptoKeys/test-key"}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		kmsConfig := &vsaCoreModels.KmsConfig{State: vsaCoreModels.LifeCycleStateError}
		mockOrchestrator.EXPECT().GetKmsConfigByKeyFullPath(mock.Anything, mock.Anything).Return(kmsConfig, nil)
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(409), result.(*gcpgenserver.V1betaCreateKmsConfigurationConflict).Code)
	})
}

// V1betaDeleteKmsConfiguration unittests
func TestV1betaDelete1KmsConfiguration(t *testing.T) {
	t.Run("CreatKmsConfigurationSuccess", func(t *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		params := gcpgenserver.V1betaDeleteKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
			KmsConfigId:   "kms-config-uuid",
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		kmsConfig := &vsaCoreModels.KmsConfig{KmsAttributes: &vsaCoreModels.KmsAttributes{}}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		operationID := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, "operation-id")
		mockOrchestrator.EXPECT().DeleteKmsConfig(mock.Anything, mock.Anything).Return(kmsConfig, "operation-id", nil)
		result, err := handler.V1betaDeleteKmsConfiguration(context.Background(), params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, operationID, result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
	t.Run("WhenFailureNotFound", func(t *testing.T) {
		// Define input parameters
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		params := gcpgenserver.V1betaDeleteKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
			KmsConfigId:   "kms-config-uuid",
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().DeleteKmsConfig(mock.Anything, mock.Anything).Return(nil, "", errors.NewNotFoundErr("kms", nil))
		// Call the method under test
		result, err := handler.V1betaDeleteKmsConfiguration(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.Equal(t, result, &gcpgenserver.V1betaDeleteKmsConfigurationNotFound{Code: 404, Message: "kms not found"})
	})
	t.Run("WhenFailure", func(t *testing.T) {
		// Define input parameters
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		params := gcpgenserver.V1betaDeleteKmsConfigurationParams{
			LocationId:    "invalid-location",
			ProjectNumber: "test-project",
			KmsConfigId:   "kms-config-uuid",
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().DeleteKmsConfig(mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("error"))
		// Call the method under test
		result, err := handler.V1betaDeleteKmsConfiguration(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.Equal(t, result, &gcpgenserver.V1betaDeleteKmsConfigurationBadRequest{Code: 400, Message: "error"})
	})
}

// V1betaDescribeKmsConfiguration unittests
func TestV1betaDescribeKmsConfiguration(t *testing.T) {
	t.Run("DescribeKmsReturnsBadRequestWhenLocationIdIsInvalid", func(t *testing.T) {
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId: "invalid-location",
		}
		handler := Handler{}

		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaDescribeKmsConfigurationBadRequest).Code)
		assert.Equal(t, "LocationID represents neither a region nor a zone", result.(*gcpgenserver.V1betaDescribeKmsConfigurationBadRequest).Message)
	})

	t.Run("DescribeKmsHandlesNilLocationIdGracefully", func(t *testing.T) {
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId: "",
		}
		handler := Handler{}

		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaDescribeKmsConfigurationBadRequest).Code)
		assert.Equal(t, "LocationID represents neither a region nor a zone", result.(*gcpgenserver.V1betaDescribeKmsConfigurationBadRequest).Message)
	})

	t.Run("WhenDescribeKmsConfigurationSuccess", func(t *testing.T) {
		// Define request
		// create a mock orchestrator
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}

		// Define mock response
		updatedTime := strfmt.DateTime(time.Now())
		description := "test-description"
		keyFullPath := "test-key-full-path"
		mockResponse := &kms_configurations.V1betaDescribeKmsConfigurationOK{
			Payload: &models.KmsConfigV1beta{
				UUID:                "test-id",
				ServiceAccountEmail: "test-email",
				KeyFullPath:         &keyFullPath,
				KmsState:            "test-state",
				KmsStateDetails:     "test-details",
				Description:         &description,
				CreatedTime:         strfmt.DateTime(time.Now()),
				UpdatedTime:         &updatedTime,
				DeletedTime:         &updatedTime,
				Instructions:        "test-instructions",
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			createClient = originalCreateClient
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		// Call the method under test
		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, "test-id", result.(*gcpgenserver.KmsConfigV1beta).UUID.Value)
	})

	t.Run("WhenDescribeKmsConfigurationFailsWithNotFound", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		// create a mock orchestrator
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}

		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &kms_configurations.V1betaDescribeKmsConfigurationNotFound{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			createClient = originalCreateClient
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		// Call the method under test
		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeKmsConfigurationNotFound).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeKmsConfigurationNotFound).Message)
	})

	t.Run("WhenDescribeKmsConfigurationFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		// create a mock orchestrator
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}

		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &kms_configurations.V1betaDescribeKmsConfigurationBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			createClient = originalCreateClient
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		// Call the method under test
		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeKmsConfigurationBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeKmsConfigurationBadRequest).Message)
	})

	t.Run("WhenDescribeKmsConfigurationFailsWithUnprocessableEntity", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		// create a mock orchestrator
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Define input parameters
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(422)
		errorMessage := "Unprocessable error"
		mockError := &kms_configurations.V1betaDescribeKmsConfigurationUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			createClient = originalCreateClient
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		// Call the method under test
		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeKmsConfigurationUnprocessableEntity).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeKmsConfigurationUnprocessableEntity).Message)
	})

	t.Run("WhenDescribeKmsConfigurationFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		// create a mock orchestrator
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Define input parameters
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(409)
		errorMessage := "Conflict error"
		mockError := &kms_configurations.V1betaDescribeKmsConfigurationConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			createClient = originalCreateClient
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		// Call the method under test
		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeKmsConfigurationConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeKmsConfigurationConflict).Message)
	})

	t.Run("WhenDescribeKmsConfigurationFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		// create a mock orchestrator
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &kms_configurations.V1betaDescribeKmsConfigurationUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			createClient = originalCreateClient
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		// Call the method under test
		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeKmsConfigurationUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeKmsConfigurationUnauthorized).Message)
	})

	t.Run("WhenDescribeKmsConfigurationFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		// create a mock orchestrator
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &kms_configurations.V1betaDescribeKmsConfigurationForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			createClient = originalCreateClient
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		// Call the method under test
		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeKmsConfigurationForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeKmsConfigurationForbidden).Message)
	})

	t.Run("WhenDescribeKmsConfigurationFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		// create a mock orchestrator
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(429)
		errorMessage := "Too Many Requests error"
		mockError := &kms_configurations.V1betaDescribeKmsConfigurationTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			createClient = originalCreateClient
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		// Call the method under test
		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeKmsConfigurationTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeKmsConfigurationTooManyRequests).Message)
	})

	t.Run("WhenDescribeKmsConfigurationFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		// create a mock orchestrator
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &kms_configurations.V1betaDescribeKmsConfigurationDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			createClient = originalCreateClient
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		// Call the method under test
		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeKmsConfigurationInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeKmsConfigurationInternalServerError).Message)
	})

	t.Run("WhenDescribeKmsConfigurationFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		// create a mock orchestrator
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "unknown error during the describe kms configuration"
		mockError := &kms_configurations.V1betaDescribeKmsConfigurationInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			createClient = originalCreateClient
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		// Call the method under test
		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeKmsConfigurationInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeKmsConfigurationInternalServerError).Message)
	})
}

// V1betaUpdateKmsConfiguration unittests
func TestV1betaUpdateKmsConfiguration(t *testing.T) {
	t.Run("WhenUpdateKmsConfigurationSuccess", func(t *testing.T) {
		// Define input parameters
		params := gcpgenserver.V1betaUpdateKmsConfigurationParams{
			KmsConfigId:    "kms-config-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.KmsConfigUpdateV1beta{
			KeyFullPath: gcpgenserver.NewOptString("projects/projectID/locations/us/keyRings/keyRing/cryptoKeys/keyName"),
			Description: gcpgenserver.NewOptString("test-description"),
			ResourceId:  gcpgenserver.NewOptString("test-resource-id"),
		}

		resourceId := "kms1"
		kms := &vsaCoreModels.KmsConfig{
			Name:    resourceId,
			KeyName: "key1",
			State:   vsaCoreModels.LifeCycleStateCreating,
		}

		orchestrator.UpdateKmsConfig = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.UpdateKmsConfigParams) (*vsaCoreModels.KmsConfig, string, error) {
			return kms, "test-uuid", nil
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "region", "zone", nil
		}
		handler := Handler{
			Orchestrator: orchestrator.NewOrchestrator(nil, nil),
		}
		// Call the method under test
		result, err := handler.V1betaUpdateKmsConfiguration(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the uuid value is as expected
		assert.Equal(t, "/v1beta/projects/12345/locations/test-location/operations/test-uuid", result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
	t.Run("WhenFailure", func(t *testing.T) {
		// Define input parameters
		params := gcpgenserver.V1betaUpdateKmsConfigurationParams{
			KmsConfigId:    "kms-config-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.KmsConfigUpdateV1beta{
			KeyFullPath: gcpgenserver.NewOptString("projects/projectID/locations/us/keyRings/keyRing/cryptoKeys/keyName"),
			Description: gcpgenserver.NewOptString("test-description"),
			ResourceId:  gcpgenserver.NewOptString("test-resource-id"),
		}

		orchestrator.UpdateKmsConfig = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.UpdateKmsConfigParams) (*vsaCoreModels.KmsConfig, string, error) {
			return nil, "", errors.NewNotFoundErr("kms", nil)
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "region", "zone", nil
		}
		handler := Handler{
			Orchestrator: orchestrator.NewOrchestrator(nil, nil),
		}
		// Call the method under test
		result, err := handler.V1betaUpdateKmsConfiguration(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.Equal(t, result, &gcpgenserver.V1betaUpdateKmsConfigurationBadRequest{Code: 400, Message: "kms not found"})
	})
}

// V1betaCheckKmsConfiguration unittests
func TestV1betaCheckKmsConfiguration(t *testing.T) {
	t.Run("WhenCheckKmsConfigurationParseAndValidateRegionAndZoneFails", func(t *testing.T) {
		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{
			KmsConfigId:    "kms-config-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{}
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCheckKmsConfig(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("WhenCheckKmsConfigurationWhenVsaKmsConfigFoundSuccess", func(t *testing.T) {
		// Define request
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{
			KmsConfigId:    "kms-config-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		// Define mock response
		isHealthy := true
		mockResponse := &kms_configurations.V1betaCheckKmsConfigOK{
			Payload: &models.KmsConfigCheckV1beta{
				KmsConfigHealthCheck: &models.KmsConfigHealthCheckV1beta{
					HealthError:  "test-health-error",
					Instructions: "test-instructions",
					IsHealthy:    &isHealthy,
				},
				ServiceAccount: "test-service-account",
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCheckKmsConfig(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		kmsConfig := &vsaCoreModels.KmsConfig{KmsAttributes: &vsaCoreModels.KmsAttributes{SdeKmsConfigUUID: "sdeUUID"},
			ServiceAccount: &vsaCoreModels.ServiceAccount{}}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(kmsConfig, nil)
		mockOrchestrator.EXPECT().CheckAndUpdateKmsConfigHealth(mock.Anything, mock.Anything).Return(kmsConfig, nil)
		mockOrchestrator.EXPECT().AccessCryptoKeyWithImpersonation(mock.Anything, mock.Anything).Return(nil)
		// Call the method under test
		result, err := handler.V1betaCheckKmsConfig(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the ServiceAccount value is as expected
		assert.Equal(t, "test-service-account", result.(*gcpgenserver.KmsConfigCheckV1beta).ServiceAccount.Value)
	})

	t.Run("WhenCheckKmsConfigurationWhenVsaKmsConfigNotFoundSuccess", func(t *testing.T) {
		// Define request
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{
			KmsConfigId:    "kms-config-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		// Define mock response
		isHealthy := true
		mockResponse := &kms_configurations.V1betaCheckKmsConfigOK{
			Payload: &models.KmsConfigCheckV1beta{
				KmsConfigHealthCheck: &models.KmsConfigHealthCheckV1beta{
					HealthError:  "test-health-error",
					Instructions: "test-instructions",
					IsHealthy:    &isHealthy,
				},
				ServiceAccount: "test-service-account",
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCheckKmsConfig(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		// Call the method under test
		result, err := handler.V1betaCheckKmsConfig(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the ServiceAccount value is as expected
		assert.Equal(t, "test-service-account", result.(*gcpgenserver.KmsConfigCheckV1beta).ServiceAccount.Value)
	})

	t.Run("WhenDCheckKmsConfigurationFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{}

		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &kms_configurations.V1betaCheckKmsConfigBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCheckKmsConfig(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		// Call the method under test
		result, err := handler.V1betaCheckKmsConfig(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCheckKmsConfigBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCheckKmsConfigBadRequest).Message)
	})

	t.Run("WhenDCheckKmsConfigurationFailsWithUnprocessableEntity", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{}

		// Define mock error
		errorCode := float64(422)
		errorMessage := "Unprocessable error"
		mockError := &kms_configurations.V1betaCheckKmsConfigUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCheckKmsConfig(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		// Call the method under test
		result, err := handler.V1betaCheckKmsConfig(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCheckKmsConfigUnprocessableEntity).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCheckKmsConfigUnprocessableEntity).Message)
	})

	t.Run("WhenDCheckKmsConfigurationFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{}

		// Define mock error
		errorCode := float64(409)
		errorMessage := "Conflict error"
		mockError := &kms_configurations.V1betaCheckKmsConfigConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCheckKmsConfig(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		// Call the method under test
		result, err := handler.V1betaCheckKmsConfig(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCheckKmsConfigConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCheckKmsConfigConflict).Message)
	})

	t.Run("WhenDCheckKmsConfigurationFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{}

		// Define mock error
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &kms_configurations.V1betaCheckKmsConfigUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCheckKmsConfig(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		// Call the method under test
		result, err := handler.V1betaCheckKmsConfig(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCheckKmsConfigUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCheckKmsConfigUnauthorized).Message)
	})

	t.Run("WhenDCheckKmsConfigurationFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{}

		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &kms_configurations.V1betaCheckKmsConfigForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCheckKmsConfig(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		// Call the method under test
		result, err := handler.V1betaCheckKmsConfig(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCheckKmsConfigForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCheckKmsConfigForbidden).Message)
	})

	t.Run("WhenDCheckKmsConfigurationFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{}

		// Define mock error
		errorCode := float64(429)
		errorMessage := "Too Many Requests error"
		mockError := &kms_configurations.V1betaCheckKmsConfigTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCheckKmsConfig(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		// Call the method under test
		result, err := handler.V1betaCheckKmsConfig(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCheckKmsConfigTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCheckKmsConfigTooManyRequests).Message)
	})

	t.Run("WhenDCheckKmsConfigurationFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{}

		// Define mock error
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &kms_configurations.V1betaCheckKmsConfigDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCheckKmsConfig(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		// Call the method under test
		result, err := handler.V1betaCheckKmsConfig(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCheckKmsConfigInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCheckKmsConfigInternalServerError).Message)
	})

	t.Run("WhenDCheckKmsConfigurationFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{}

		// Define mock error
		errorCode := float64(500)
		errorMessage := "unknown error during the check kms config"
		mockError := &kms_configurations.V1betaCheckKmsConfigInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCheckKmsConfig(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().GetKmsConfig(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("not found", nil))
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		// Call the method under test
		result, err := handler.V1betaCheckKmsConfig(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCheckKmsConfigInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCheckKmsConfigInternalServerError).Message)
	})
}

// V1betaListKmsConfigurations unittests
func TestV1betaListKmsConfigurations(t *testing.T) {
	t.Run("WhenListKmsConfigurationsSuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListKmsConfigurationsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		// Define mock response
		kmsConfigurations := []*models.KmsConfigV1beta{}
		updatedTime := strfmt.DateTime(time.Now())
		description := "test-description"
		keyFullPath := "test-key-full-path"
		kmsConfig := models.KmsConfigV1beta{
			UUID:                "test-id",
			ServiceAccountEmail: "test-email",
			KeyFullPath:         &keyFullPath,
			KmsState:            "test-state",
			KmsStateDetails:     "test-details",
			Description:         &description,
			CreatedTime:         strfmt.DateTime(time.Now()),
			UpdatedTime:         &updatedTime,
			DeletedTime:         &updatedTime,
			Instructions:        "test-instructions",
		}
		kmsConfigurations = append(kmsConfigurations, &kmsConfig)
		mockResponse := &kms_configurations.V1betaListKmsConfigurationsOK{
			Payload: kmsConfigurations,
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListKmsConfigurations(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListKmsConfigurations(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, "test-id", result.(*gcpgenserver.V1betaListKmsConfigurationsOK).KmsMinusConfigurations[0].UUID.Value)
	})

	t.Run("WhenListKmsConfigurationFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListKmsConfigurationsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &kms_configurations.V1betaListKmsConfigurationsBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListKmsConfigurations(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListKmsConfigurations(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListKmsConfigurationsBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListKmsConfigurationsBadRequest).Message)
	})

	t.Run("WhenListKmsConfigurationFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListKmsConfigurationsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		// Define mock error
		errorCode := float64(409)
		errorMessage := "Conflict error"
		mockError := &kms_configurations.V1betaListKmsConfigurationsConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListKmsConfigurations(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListKmsConfigurations(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListKmsConfigurationsConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListKmsConfigurationsConflict).Message)
	})

	t.Run("WhenListKmsConfigurationFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListKmsConfigurationsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		// Define mock error
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &kms_configurations.V1betaListKmsConfigurationsUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListKmsConfigurations(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListKmsConfigurations(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListKmsConfigurationsUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListKmsConfigurationsUnauthorized).Message)
	})

	t.Run("WhenListKmsConfigurationFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListKmsConfigurationsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &kms_configurations.V1betaListKmsConfigurationsForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListKmsConfigurations(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListKmsConfigurations(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListKmsConfigurationsForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListKmsConfigurationsForbidden).Message)
	})

	t.Run("WhenListKmsConfigurationFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListKmsConfigurationsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		// Define mock error
		errorCode := float64(429)
		errorMessage := "Too Many Requests error"
		mockError := &kms_configurations.V1betaListKmsConfigurationsTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListKmsConfigurations(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListKmsConfigurations(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListKmsConfigurationsTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListKmsConfigurationsTooManyRequests).Message)
	})

	t.Run("WhenListKmsConfigurationFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListKmsConfigurationsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		// Define mock error
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &kms_configurations.V1betaListKmsConfigurationsDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListKmsConfigurations(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListKmsConfigurations(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListKmsConfigurationsInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListKmsConfigurationsInternalServerError).Message)
	})

	t.Run("WhenListKmsConfigurationFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListKmsConfigurationsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		// Define mock error
		errorCode := float64(500)
		errorMessage := "unknown error during the list kms configurations"
		mockError := &kms_configurations.V1betaListKmsConfigurationsInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListKmsConfigurations(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListKmsConfigurations(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListKmsConfigurationsInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListKmsConfigurationsInternalServerError).Message)
	})
}

func TestV1betaGetMultipleKmsConfigs(t *testing.T) {
	// Mock of VCP Orchestrator and Datastore
	mockLogger := log.NewLogger()
	store, err := database.NewTestStorage(mockLogger)
	if err != nil {
		t.Fatalf("Failed to create test storage: %v", err)
	}

	err = database.ClearInMemoryDB(store.DB())
	if err != nil {
		t.Fatalf("Failed to clean up test storage: %v", err)
	}

	orchInstance := orchestrator.NewOrchestrator(store, nil)

	serviceAccounts := []*datamodel.ServiceAccount{
		{BaseModel: datamodel.BaseModel{ID: int64(111), UUID: "uuid10"}, Name: "ServiceAccount1"},
		{BaseModel: datamodel.BaseModel{ID: int64(222), UUID: "uuid20"}, Name: "ServiceAccount2"},
	}
	err = store.DB().Create(serviceAccounts).Error
	if err != nil {
		t.Fatalf("Failed to create Service-Accounts table: %v", err)
	}

	kmsConfigs := []*datamodel.KmsConfig{
		{BaseModel: datamodel.BaseModel{UUID: "uuid1", DeletedAt: nil}, Name: "kmsConfig1", ResourceID: "Resource-Id1-VCP", ServiceAccountID: &serviceAccounts[0].ID,
			KmsAttributes: &datamodel.KmsAttributes{SdeServiceAccountEmail: "sdeServiceAccount1@account.com"}},
		{BaseModel: datamodel.BaseModel{UUID: "uuid2", DeletedAt: nil}, Name: "kmsConfig2", ResourceID: "Resource-Id2-VCP", ServiceAccountID: &serviceAccounts[1].ID,
			KmsAttributes: &datamodel.KmsAttributes{SdeServiceAccountEmail: "sdeServiceAccount2@account.com"}},
	}
	err = store.DB().Create(kmsConfigs).Error
	if err != nil {
		t.Fatalf("Failed to create KMS Configs table: %v", err)
	}

	// Mock of CVP Client
	mockClient := kms_configurations.NewMockClientService(t)

	params := gcpgenserver.V1betaGetMultipleKmsConfigsParams{
		LocationId:     "test-location",
		ProjectNumber:  "12345",
		XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
	}
	req := &gcpgenserver.KmsConfigIdListV1beta{
		KmsConfigIds: []string{"uuid3", "uuid4"},
	}

	kmsConfigsCVP := make([]*models.KmsConfigV1beta, 0)
	resourceId3 := "Resource-Id3-SDE"
	resourceId4 := "Resource-Id4-SDE"

	nowTime := strfmt.DateTime(time.Now())
	description := "Test-description"
	keyFullPath := "Test-key-SDE-full-path"
	kmsConfigsCVP = append(kmsConfigsCVP, &models.KmsConfigV1beta{
		UUID:                "uuid3",
		ServiceAccountEmail: "service-account3@sde.com",
		CreatedTime:         nowTime,
		UpdatedTime:         &nowTime,
		Description:         &description,
		ResourceID:          &resourceId3,
		KmsState:            "Test-state-3",
		KmsStateDetails:     "Test-state-details-3",
		Instructions:        "Test-instructions-3",
		KeyFullPath:         &keyFullPath,
	})
	kmsConfigsCVP = append(kmsConfigsCVP, &models.KmsConfigV1beta{
		UUID:                "uuid4",
		ServiceAccountEmail: "service-account4@sde.com",
		CreatedTime:         nowTime,
		UpdatedTime:         &nowTime,
		Description:         &description,
		ResourceID:          &resourceId4,
		KmsState:            "Test-state-4",
		KmsStateDetails:     "Test-state-details-4",
		Instructions:        "Test-instructions-4",
		KeyFullPath:         &keyFullPath,
	})

	handler := Handler{}
	handler.Orchestrator = orchInstance

	mockResponse := &kms_configurations.V1betaGetMultipleKmsConfigsOK{
		Payload: &kms_configurations.V1betaGetMultipleKmsConfigsOKBody{
			KmsConfigurations: kmsConfigsCVP,
		},
	}

	mockClient.EXPECT().
		V1betaGetMultipleKmsConfigs(mock.Anything).
		Return(mockResponse, nil)
	cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
	originalCreateClient := createClient
	defer func() { createClient = originalCreateClient }()
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	t.Run("WhenGetMultipleKmsConfigsOnlyHasEntriesInSDE", func(t *testing.T) {
		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 2, len(result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations))
		assert.Equal(t, "Resource-Id3-SDE", result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[0].ResourceId.Value)
		assert.Equal(t, "Resource-Id4-SDE", result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[1].ResourceId.Value)
	})
	t.Run("WhenGetMultipleKmsConfigsOnlyHasEntriesInVCP", func(t *testing.T) {
		req := &gcpgenserver.KmsConfigIdListV1beta{
			KmsConfigIds: []string{"uuid1", "uuid2"},
		}
		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 2, len(result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations))
		assert.Equal(t, "Resource-Id1-VCP", result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[0].ResourceId.Value)
		assert.Equal(t, "Resource-Id2-VCP", result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[1].ResourceId.Value)
	})
	t.Run("WhenGetMultipleKmsConfigsHasSomeEntriesInVcpAndInSde", func(t *testing.T) {
		req := &gcpgenserver.KmsConfigIdListV1beta{
			KmsConfigIds: []string{"uuid2", "uuid3", "uuid4"},
		}
		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 3, len(result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations))
		assert.Equal(t, "Resource-Id2-VCP", result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[0].ResourceId.Value)
		assert.Equal(t, "Resource-Id3-SDE", result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[1].ResourceId.Value)
		assert.Equal(t, "Resource-Id4-SDE", result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[2].ResourceId.Value)
	})
	t.Run("WhenGetMultipleKmsConfigsHasEntriesInVcpAndSde", func(t *testing.T) {
		req := &gcpgenserver.KmsConfigIdListV1beta{
			KmsConfigIds: []string{"uuid1", "uuid2", "uuid3", "uuid4"},
		}
		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, len(result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations))
		assert.Equal(t, "Resource-Id1-VCP", result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[0].ResourceId.Value)
		assert.Equal(t, "Resource-Id2-VCP", result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[1].ResourceId.Value)
		assert.Equal(t, "Resource-Id3-SDE", result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[2].ResourceId.Value)
		assert.Equal(t, "Resource-Id4-SDE", result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations[3].ResourceId.Value)
	})
}

func TestV1betaGetMultipleKmsConfigsErrorConditions(t *testing.T) {
	// Mock of VCP Orchestrator and Datastore
	mockLogger := log.NewLogger()
	store, err := database.NewTestStorage(mockLogger)
	if err != nil {
		t.Fatalf("Failed to create test storage: %v", err)
	}

	err = database.ClearInMemoryDB(store.DB())
	if err != nil {
		t.Fatalf("Failed to clean up test storage: %v", err)
	}

	orchInstance := orchestrator.NewOrchestrator(store, nil)

	serviceAccounts := []*datamodel.ServiceAccount{
		{BaseModel: datamodel.BaseModel{ID: int64(111), UUID: "uuid10"}, Name: "ServiceAccount1"},
		{BaseModel: datamodel.BaseModel{ID: int64(222), UUID: "uuid20"}, Name: "ServiceAccount2"},
	}
	err = store.DB().Create(serviceAccounts).Error
	if err != nil {
		t.Fatalf("Failed to create Service-Accounts table: %v", err)
	}

	kmsConfigs := []*datamodel.KmsConfig{
		{BaseModel: datamodel.BaseModel{UUID: "uuid1", DeletedAt: nil}, Name: "kmsConfig1", ResourceID: "Resource-Id1-SDE", ServiceAccountID: &serviceAccounts[0].ID,
			KmsAttributes: &datamodel.KmsAttributes{SdeServiceAccountEmail: "sdeServiceAccount1@account.com"}},
		{BaseModel: datamodel.BaseModel{UUID: "uuid2", DeletedAt: nil}, Name: "kmsConfig2", ResourceID: "Resource-Id2-SDE", ServiceAccountID: &serviceAccounts[1].ID,
			KmsAttributes: &datamodel.KmsAttributes{SdeServiceAccountEmail: "sdeServiceAccount2@account.com"}},
	}
	err = store.DB().Create(kmsConfigs).Error
	if err != nil {
		t.Fatalf("Failed to create KMS Configs table: %v", err)
	}

	params := gcpgenserver.V1betaGetMultipleKmsConfigsParams{
		LocationId:     "test-location",
		ProjectNumber:  "12345",
		XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
	}
	req := &gcpgenserver.KmsConfigIdListV1beta{
		KmsConfigIds: []string{""},
	}

	handler := Handler{}
	handler.Orchestrator = orchInstance

	t.Run("WhenGetMultipleKmsConfigsReturnsBadRequest", func(tt *testing.T) {
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &kms_configurations.V1betaGetMultipleKmsConfigsBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}

		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaGetMultipleKmsConfigs(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsBadRequest).Message)
	})
	t.Run("WhenGetMultipleKmsConfigsFailsWithUnauthorized", func(tt *testing.T) {
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &kms_configurations.V1betaGetMultipleKmsConfigsUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}

		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaGetMultipleKmsConfigs(mock.Anything).
			Return(nil, mockError)

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsUnauthorized).Message)
	})
	t.Run("WhenGetMultipleKmsConfigsFailsWithForbidden", func(tt *testing.T) {
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &kms_configurations.V1betaGetMultipleKmsConfigsForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}

		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaGetMultipleKmsConfigs(mock.Anything).
			Return(nil, mockError)

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsForbidden).Message)
	})
	t.Run("WhenGetMultipleKmsConfigsFailsWithNotFound", func(tt *testing.T) {
		errorCode := float64(404)
		errorMessage := "Not found"
		mockError := &kms_configurations.V1betaGetMultipleKmsConfigsNotFound{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}

		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaGetMultipleKmsConfigs(mock.Anything).
			Return(nil, mockError)

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsNotFound).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsNotFound).Message)
	})
	t.Run("WhenGetMultipleKmsConfigsFailsWithErrorTooManyRequests", func(tt *testing.T) {
		errorCode := float64(429)
		errorMessage := "Too many requests"
		mockError := &kms_configurations.V1betaGetMultipleKmsConfigsTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}

		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaGetMultipleKmsConfigs(mock.Anything).
			Return(nil, mockError)

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsTooManyRequests).Message)
	})
	t.Run("WhenGetMultipleKmsConfigsFailsWithDefault", func(tt *testing.T) {
		errorCode := float64(500)
		errorMessage := "Default error"
		mockError := &kms_configurations.V1betaGetMultipleKmsConfigsDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}

		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaGetMultipleKmsConfigs(mock.Anything).
			Return(nil, mockError)

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError).Message)
	})
	t.Run("WhenGetMultipleKmsConfigsFailsWithUnknownError", func(tt *testing.T) {
		errorCode := float64(500)
		errorMessage := "Unknown error encountered during Get Multiple KMS configurations operation"
		mockError := &kms_configurations.V1betaGetMultipleKmsConfigsInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}

		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaGetMultipleKmsConfigs(mock.Anything).
			Return(nil, mockError)

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError).Message)
	})
	t.Run("WhenGetMultipleKmsConfigsFailsWithCvpResponseNil", func(tt *testing.T) {
		expectedErrorMsg := "Unknown error encountered during Get Multiple KMS configurations operation"
		expectedErrorCode := float64(500)

		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaGetMultipleKmsConfigs(mock.Anything).
			Return(nil, nil)

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, expectedErrorCode, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError).Code)
		assert.Equal(t, expectedErrorMsg, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError).Message)
	})
	t.Run("WhenGetMultipleKmsConfigsFailsWithCvpPayloadNil", func(tt *testing.T) {
		req := &gcpgenserver.KmsConfigIdListV1beta{
			KmsConfigIds: []string{"uuid5"},
		}
		mockResponse := &kms_configurations.V1betaGetMultipleKmsConfigsOK{
			Payload: nil,
		}
		mockClient := kms_configurations.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaGetMultipleKmsConfigs(mock.Anything).
			Return(mockResponse, nil)

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result.(*gcpgenserver.V1betaGetMultipleKmsConfigsOK).KmsConfigurations))
	})
	t.Run("WhenGetMultipleKmsConfigsFailsWithUnknownErrorFromVCP", func(tt *testing.T) {
		expectedErrorMsg := "Unknown error encountered during Get Multiple KMS configurations operation"
		expectedErrorCode := float64(500)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().GetMultipleKMSConfigs(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("internal error"))
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		result, err := handler.V1betaGetMultipleKmsConfigs(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedErrorMsg, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError).Message)
		assert.Equal(tt, expectedErrorCode, result.(*gcpgenserver.V1betaGetMultipleKmsConfigsInternalServerError).Code)
	})
}

func TestGetKmsInstructions(t *testing.T) {
	t.Run("ReturnsEmptyStringWhenKmsAttributesIsNil", func(t *testing.T) {
		kmsConfig := &vsaCoreModels.KmsConfig{}
		instructions := getKmsInstructions(kmsConfig)
		assert.Equal(t, "", instructions)
	})
	t.Run("ReturnsEmptyStringWhenServiceAccountEmailIsEmpty", func(t *testing.T) {
		kmsConfig := &vsaCoreModels.KmsConfig{}
		kmsConfig.KmsAttributes = &vsaCoreModels.KmsAttributes{
			SdeServiceAccountEmail: "",
		}
		instructions := getKmsInstructions(kmsConfig)
		assert.Equal(t, "", instructions)
	})
	t.Run("UsesCustomerProjectIDWhenKeyProjectIDIsEmpty", func(t *testing.T) {
		kmsConfig := &vsaCoreModels.KmsConfig{
			KeyProjectID:      "",
			CustomerProjectID: "customer-project-id",
			KeyName:           "key-name",
			KeyRing:           "key-ring",
			KeyRingLocation:   "key-ring-location",
		}
		kmsConfig.KmsAttributes = &vsaCoreModels.KmsAttributes{
			SdeServiceAccountEmail: "service-account@test.com",
		}
		instructions := getKmsInstructions(kmsConfig)
		assert.Contains(t, instructions, "customer-project-id")
		assert.Contains(t, instructions, "serviceAccount:service-account@test.com")
	})
	t.Run("GeneratesInstructionsWithKeyProjectID", func(t *testing.T) {
		kmsConfig := &vsaCoreModels.KmsConfig{
			KeyProjectID:    "key-project-id",
			KeyName:         "key-name",
			KeyRing:         "key-ring",
			KeyRingLocation: "key-ring-location",
		}
		kmsConfig.KmsAttributes = &vsaCoreModels.KmsAttributes{
			SdeServiceAccountEmail: "service-account@test.com",
		}
		expectedOutput := `Please copy and paste the commands listed below into Google Cloud Shell in the project that contains the key ring. The commands create a KMS role and assign it to the CVS service account so that it can access the key.
## CREATE KMS role ## gcloud iam roles create cmekNetAppVolumesRole --project=key-project-id --title='cmekNetAppVolumesRole' --description='custom cmek cvs role' --permissions=cloudkms.cryptoKeyVersions.get,cloudkms.cryptoKeyVersions.list,cloudkms.cryptoKeyVersions.useToDecrypt,cloudkms.cryptoKeyVersions.useToEncrypt,cloudkms.cryptoKeys.get,cloudkms.keyRings.get,cloudkms.locations.get,cloudkms.locations.list,resourcemanager.projects.get --stage=GA
 ## ASSIGN role and give KEY ACCESS to CVS service account ## gcloud kms keys add-iam-policy-binding key-name --project=key-project-id --keyring key-ring --location key-ring-location --member serviceAccount:service-account@test.com --role projects/key-project-id/roles/cmekNetAppVolumesRole`

		instructions := getKmsInstructions(kmsConfig)
		assert.Contains(t, instructions, "key-project-id")
		assert.Contains(t, instructions, "key-name")
		assert.Contains(t, instructions, "key-ring")
		assert.Contains(t, instructions, "key-ring-location")
		assert.Contains(t, instructions, "serviceAccount:service-account@test.com")
		assert.Equal(t, expectedOutput, instructions)
	})
}

func TestConvertOrchestratorModelToKmsConfigV1beta(t *testing.T) {
	t.Run("ReturnsValidKmsConfigV1betaWhenAllFieldsArePopulated", func(t *testing.T) {
		expectedDate := time.Date(2022, time.February, 2, 2, 2, 2, 2, time.UTC)
		kmsConfig := &vsaCoreModels.KmsConfig{
			State:           "ACTIVE",
			KeyProjectID:    "test-project-id",
			KeyRingLocation: "test-location",
			KeyRing:         "test-key-ring",
			KeyName:         "test-key-name",
			StateDetails:    "test-state-details",
			Description:     "test-description",
			ResourceID:      "test-resource-id",
		}
		kmsConfig.BaseModel = vsaCoreModels.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: expectedDate,
			UpdatedAt: expectedDate,
			DeletedAt: &expectedDate,
		}
		kmsConfig.KmsAttributes = &vsaCoreModels.KmsAttributes{
			SdeServiceAccountEmail: "test-service-account@test.com",
		}

		result := convertOrchestratorModelToKmsConfigV1beta(kmsConfig)

		assert.NotNil(t, result)
		assert.Equal(t, kmsConfig.UUID, result.UUID.Value)
		assert.Equal(t, kmsConfig.KmsAttributes.SdeServiceAccountEmail, result.ServiceAccountEmail.Value)
		assert.Contains(t, result.KeyFullPath, kmsConfig.KeyProjectID)
		assert.Contains(t, result.KeyFullPath, kmsConfig.KeyRingLocation)
		assert.Contains(t, result.KeyFullPath, kmsConfig.KeyRing)
		assert.Contains(t, result.KeyFullPath, kmsConfig.KeyName)
		assert.Equal(t, kmsConfig.State, string(result.KmsState.Value))
		assert.Equal(t, kmsConfig.StateDetails, result.KmsStateDetails.Value)
		assert.Equal(t, kmsConfig.Description, result.Description.Value)
		assert.Equal(t, kmsConfig.ResourceID, result.ResourceId.Value)
		assert.Equal(t, expectedDate, result.CreatedTime.Value)
		assert.Equal(t, expectedDate, result.UpdatedTime.Value)
		assert.Equal(t, expectedDate, result.DeletedTime.Value)
	})
	t.Run("HandlesNilDeletedTimeGracefully", func(t *testing.T) {
		kmsConfig := &vsaCoreModels.KmsConfig{
			State:           "ACTIVE",
			KeyProjectID:    "test-project-id",
			KeyRingLocation: "test-location",
			KeyRing:         "test-key-ring",
			KeyName:         "test-key-name",
			StateDetails:    "test-state-details",
			Description:     "test-description",
			ResourceID:      "test-resource-id",
		}
		kmsConfig.BaseModel = vsaCoreModels.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			DeletedAt: nil,
		}
		kmsConfig.KmsAttributes = &vsaCoreModels.KmsAttributes{
			SdeServiceAccountEmail: "test-service-account@test.com",
		}
		zeroTime := gcpgenserver.OptDateTime{Value: time.Time{}}

		result := convertOrchestratorModelToKmsConfigV1beta(kmsConfig)
		assert.NotNil(t, result)
		assert.Equal(t, zeroTime, result.DeletedTime)
	})
	t.Run("HandlesNilKmsAttributesGracefully", func(t *testing.T) {
		kmsConfig := &vsaCoreModels.KmsConfig{
			State:           "ACTIVE",
			KeyProjectID:    "test-project-id",
			KeyRingLocation: "test-location",
			KeyRing:         "test-key-ring",
			KeyName:         "test-key-name",
			StateDetails:    "test-state-details",
			Description:     "test-description",
			ResourceID:      "test-resource-id",
		}
		kmsConfig.BaseModel = vsaCoreModels.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			DeletedAt: &time.Time{},
		}

		result := convertOrchestratorModelToKmsConfigV1beta(kmsConfig)
		assert.NotNil(t, result)
		assert.Equal(t, gcpgenserver.OptString{Value: ""}, result.ServiceAccountEmail)
	})
}

func TestCategorizeCvpClientErrorsForCreateKmsConfigs(t *testing.T) {
	t.Run("ReturnsUnprocessableEntityOnUnprocessableEntityError", func(t *testing.T) {
		code := float64(422)
		msg := "unprocessable"
		err := &kms_configurations.V1betaCreateKmsConfigurationUnprocessableEntity{
			Payload: &models.Error{Code: code, Message: msg},
		}
		res, _ := categorizeCvpClientErrorsForCreateKmsConfigs(err)
		assert.IsType(t, &gcpgenserver.V1betaCreateKmsConfigurationUnprocessableEntity{}, res)
		assert.Equal(t, code, res.(*gcpgenserver.V1betaCreateKmsConfigurationUnprocessableEntity).Code)
		assert.Equal(t, msg, res.(*gcpgenserver.V1betaCreateKmsConfigurationUnprocessableEntity).Message)
	})

	t.Run("ReturnsConflictOnConflictError", func(t *testing.T) {
		code := float64(409)
		msg := "conflict"
		err := &kms_configurations.V1betaCreateKmsConfigurationConflict{
			Payload: &models.Error{Code: code, Message: msg},
		}
		res, _ := categorizeCvpClientErrorsForCreateKmsConfigs(err)
		assert.IsType(t, &gcpgenserver.V1betaCreateKmsConfigurationConflict{}, res)
		assert.Equal(t, code, res.(*gcpgenserver.V1betaCreateKmsConfigurationConflict).Code)
		assert.Equal(t, msg, res.(*gcpgenserver.V1betaCreateKmsConfigurationConflict).Message)
	})

	t.Run("ReturnsBadRequestOnBadRequestError", func(t *testing.T) {
		code := float64(400)
		msg := "bad request"
		err := &kms_configurations.V1betaCreateKmsConfigurationBadRequest{
			Payload: &models.Error{Code: code, Message: msg},
		}
		res, _ := categorizeCvpClientErrorsForCreateKmsConfigs(err)
		assert.IsType(t, &gcpgenserver.V1betaCreateKmsConfigurationBadRequest{}, res)
		assert.Equal(t, code, res.(*gcpgenserver.V1betaCreateKmsConfigurationBadRequest).Code)
		assert.Equal(t, msg, res.(*gcpgenserver.V1betaCreateKmsConfigurationBadRequest).Message)
	})

	t.Run("ReturnsUnauthorizedOnUnauthorizedError", func(t *testing.T) {
		code := float64(401)
		msg := "unauthorized"
		err := &kms_configurations.V1betaCreateKmsConfigurationUnauthorized{
			Payload: &models.Error{Code: code, Message: msg},
		}
		res, _ := categorizeCvpClientErrorsForCreateKmsConfigs(err)
		assert.IsType(t, &gcpgenserver.V1betaCreateKmsConfigurationUnauthorized{}, res)
		assert.Equal(t, code, res.(*gcpgenserver.V1betaCreateKmsConfigurationUnauthorized).Code)
		assert.Equal(t, msg, res.(*gcpgenserver.V1betaCreateKmsConfigurationUnauthorized).Message)
	})

	t.Run("ReturnsForbiddenOnForbiddenError", func(t *testing.T) {
		code := float64(403)
		msg := "forbidden"
		err := &kms_configurations.V1betaCreateKmsConfigurationForbidden{
			Payload: &models.Error{Code: code, Message: msg},
		}
		res, _ := categorizeCvpClientErrorsForCreateKmsConfigs(err)
		assert.IsType(t, &gcpgenserver.V1betaCreateKmsConfigurationForbidden{}, res)
		assert.Equal(t, code, res.(*gcpgenserver.V1betaCreateKmsConfigurationForbidden).Code)
		assert.Equal(t, msg, res.(*gcpgenserver.V1betaCreateKmsConfigurationForbidden).Message)
	})

	t.Run("ReturnsTooManyRequestsOnTooManyRequestsError", func(t *testing.T) {
		code := float64(429)
		msg := "too many requests"
		err := &kms_configurations.V1betaCreateKmsConfigurationTooManyRequests{
			Payload: &models.Error{Code: code, Message: msg},
		}
		res, _ := categorizeCvpClientErrorsForCreateKmsConfigs(err)
		assert.IsType(t, &gcpgenserver.V1betaCreateKmsConfigurationTooManyRequests{}, res)
		assert.Equal(t, code, res.(*gcpgenserver.V1betaCreateKmsConfigurationTooManyRequests).Code)
		assert.Equal(t, msg, res.(*gcpgenserver.V1betaCreateKmsConfigurationTooManyRequests).Message)
	})

	t.Run("ReturnsInternalServerErrorOnDefaultError", func(t *testing.T) {
		code := float64(500)
		msg := "internal"
		err := &kms_configurations.V1betaCreateKmsConfigurationDefault{
			Payload: &models.Error{Code: code, Message: msg},
		}
		res, _ := categorizeCvpClientErrorsForCreateKmsConfigs(err)
		assert.IsType(t, &gcpgenserver.V1betaCreateKmsConfigurationInternalServerError{}, res)
		assert.Equal(t, code, res.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Code)
		assert.Equal(t, msg, res.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Message)
	})

	t.Run("ReturnsInternalServerErrorOnUnknownErrorType", func(t *testing.T) {
		err := fmt.Errorf("some unknown error")
		res, _ := categorizeCvpClientErrorsForCreateKmsConfigs(err)
		assert.IsType(t, &gcpgenserver.V1betaCreateKmsConfigurationInternalServerError{}, res)
		assert.Equal(t, float64(500), res.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Code)
		assert.Equal(t, "unknown error during the create kms config", res.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Message)
	})
}
