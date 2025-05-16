package api

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// V1betaCreateKmsConfiguration unittests
func TestV1betaCreateKmsConfiguration(t *testing.T) {
	t.Run("WhenCreateKmsConfigurationSuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.KmsConfigV1beta{
			UUID:                gcpgenserver.NewOptString("test-id"),
			ServiceAccountEmail: gcpgenserver.NewOptString("test-email"),
			KeyFullPath:         "test-full-path",
			KmsState:            gcpgenserver.NewOptKmsConfigV1betaKmsState("test-state"),
			KmsStateDetails:     gcpgenserver.NewOptString("test-details"),
			Description:         gcpgenserver.NewOptString("test-description"),
			CreatedTime:         gcpgenserver.NewOptDateTime(time.Now()),
			UpdatedTime:         gcpgenserver.NewOptDateTime(time.Now()),
			DeletedTime:         gcpgenserver.NewOptDateTime(time.Now()),
			Instructions:        gcpgenserver.NewOptString("test-instructions"),
		}

		// Define mock response
		mockResponse := &kms_configurations.V1betaCreateKmsConfigurationAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
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
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenCreateKmsConfigurationFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.KmsConfigV1beta{}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &kms_configurations.V1betaCreateKmsConfigurationBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateKmsConfigurationBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateKmsConfigurationBadRequest).Message)
	})

	t.Run("WhenCreateKmsConfigurationFailsWithUnprocessableEntity", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.KmsConfigV1beta{}
		// Define mock error
		errorCode := float64(422)
		errorMessage := "Unprocessable error"
		mockError := &kms_configurations.V1betaCreateKmsConfigurationUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateKmsConfigurationUnprocessableEntity).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateKmsConfigurationUnprocessableEntity).Message)
	})

	t.Run("WhenCreateKmsConfigurationFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.KmsConfigV1beta{}
		// Define mock error
		errorCode := float64(409)
		errorMessage := "Conflict error"
		mockError := &kms_configurations.V1betaCreateKmsConfigurationConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateKmsConfigurationConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateKmsConfigurationConflict).Message)
	})

	t.Run("WhenCreateKmsConfigurationFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.KmsConfigV1beta{}
		// Define mock error
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &kms_configurations.V1betaCreateKmsConfigurationUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateKmsConfigurationUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateKmsConfigurationUnauthorized).Message)
	})

	t.Run("WhenCreateKmsConfigurationFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.KmsConfigV1beta{}
		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &kms_configurations.V1betaCreateKmsConfigurationForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateKmsConfigurationForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateKmsConfigurationForbidden).Message)
	})

	t.Run("WhenCreateKmsConfigurationFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.KmsConfigV1beta{}
		// Define mock error
		errorCode := float64(429)
		errorMessage := "Too Many Requests error"
		mockError := &kms_configurations.V1betaCreateKmsConfigurationTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateKmsConfigurationTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateKmsConfigurationTooManyRequests).Message)
	})

	t.Run("WhenCreateKmsConfigurationFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.KmsConfigV1beta{}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &kms_configurations.V1betaCreateKmsConfigurationDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Message)
	})

	t.Run("WhenCreateKmsConfigurationFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.KmsConfigV1beta{}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "unknown error during the create kms configuration"
		mockError := &kms_configurations.V1betaCreateKmsConfigurationInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateKmsConfigurationInternalServerError).Message)
	})
}

// V1betaDeleteKmsConfiguration unittests
func TestV1betaDeleteKmsConfiguration(t *testing.T) {
	t.Run("WhenDeleteKmsConfigurationSuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock response
		mockResponse := &kms_configurations.V1betaDeleteKmsConfigurationAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(mockResponse, nil, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteKmsConfiguration(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteKmsConfigurationBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteKmsConfigurationBadRequest).Message)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithUnprocessableEntity", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(422)
		errorMessage := "Unprocessable error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteKmsConfigurationUnprocessableEntity).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteKmsConfigurationUnprocessableEntity).Message)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(409)
		errorMessage := "Conflict error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteKmsConfigurationConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteKmsConfigurationConflict).Message)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteKmsConfigurationUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteKmsConfigurationUnauthorized).Message)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteKmsConfigurationForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteKmsConfigurationForbidden).Message)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(429)
		errorMessage := "Too Many Requests error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteKmsConfigurationTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteKmsConfigurationTooManyRequests).Message)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteKmsConfiguration(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteKmsConfigurationInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteKmsConfigurationInternalServerError).Message)
	})
}

// V1betaDescribeKmsConfiguration unittests
func TestV1betaDescribeKmsConfiguration(t *testing.T) {
	t.Run("WhenDescribeKmsConfigurationSuccess", func(t *testing.T) {
		// Define request
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
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeKmsConfiguration(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, "test-id", result.(*gcpgenserver.KmsConfigV1beta).UUID.Value)
	})

	t.Run("WhenDescribeKmsConfigurationFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

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
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
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
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
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
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
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
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
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
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
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
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
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
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
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
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
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
		// Define request
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateKmsConfigurationParams{
			KmsConfigId:    "kms-config-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.KmsConfigUpdateV1beta{
			KeyFullPath: gcpgenserver.NewOptString("test-key"),
			Description: gcpgenserver.NewOptString("test-description"),
			ResourceId:  gcpgenserver.NewOptString("test-resource-id"),
		}

		// Define mock response
		updatedTime := strfmt.DateTime(time.Now())
		description := "test-description"
		keyFullPath := "test-key-full-path"
		mockResponse := &kms_configurations.V1betaUpdateKmsConfigurationOK{
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
			V1betaUpdateKmsConfiguration(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateKmsConfiguration(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the uuid value is as expected
		assert.Equal(t, "test-id", result.(*gcpgenserver.KmsConfigV1beta).UUID.Value)
		// Check if the ServiceAccountEmail value is as expected
		assert.Equal(t, "test-email", result.(*gcpgenserver.KmsConfigV1beta).ServiceAccountEmail.Value)
	})

	t.Run("WhenUpdateKmsConfigurationFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define request
		req := &gcpgenserver.KmsConfigUpdateV1beta{}

		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &kms_configurations.V1betaUpdateKmsConfigurationBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateKmsConfigurationBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateKmsConfigurationBadRequest).Message)
	})

	t.Run("WhenUpdateKmsConfigurationFailsWithUnprocessableEntity", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define request
		req := &gcpgenserver.KmsConfigUpdateV1beta{}

		// Define mock error
		errorCode := float64(422)
		errorMessage := "Unprocessable error"
		mockError := &kms_configurations.V1betaUpdateKmsConfigurationUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateKmsConfigurationUnprocessableEntity).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateKmsConfigurationUnprocessableEntity).Message)
	})

	t.Run("WhenUpdateKmsConfigurationFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define request
		req := &gcpgenserver.KmsConfigUpdateV1beta{}

		// Define mock error
		errorCode := float64(409)
		errorMessage := "Conflict error"
		mockError := &kms_configurations.V1betaUpdateKmsConfigurationConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateKmsConfigurationConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateKmsConfigurationConflict).Message)
	})

	t.Run("WhenUpdateKmsConfigurationFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define request
		req := &gcpgenserver.KmsConfigUpdateV1beta{}

		// Define mock error
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &kms_configurations.V1betaUpdateKmsConfigurationUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateKmsConfigurationUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateKmsConfigurationUnauthorized).Message)
	})

	t.Run("WhenUpdateKmsConfigurationFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define request
		req := &gcpgenserver.KmsConfigUpdateV1beta{}

		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &kms_configurations.V1betaUpdateKmsConfigurationForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateKmsConfigurationForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateKmsConfigurationForbidden).Message)
	})

	t.Run("WhenUpdateKmsConfigurationFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define request
		req := &gcpgenserver.KmsConfigUpdateV1beta{}

		// Define mock error
		errorCode := float64(429)
		errorMessage := "Too Many Requests error"
		mockError := &kms_configurations.V1betaUpdateKmsConfigurationTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateKmsConfigurationTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateKmsConfigurationTooManyRequests).Message)
	})

	t.Run("WhenUpdateKmsConfigurationFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define request
		req := &gcpgenserver.KmsConfigUpdateV1beta{}

		// Define mock error
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &kms_configurations.V1betaUpdateKmsConfigurationDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError).Message)
	})

	t.Run("WhenUpdateKmsConfigurationFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateKmsConfigurationParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			KmsConfigId:    "kms-config-id-1",
		}
		// Define request
		req := &gcpgenserver.KmsConfigUpdateV1beta{}

		// Define mock error
		errorCode := float64(500)
		errorMessage := "unknown error during the update kms configurations"
		mockError := &kms_configurations.V1betaUpdateKmsConfigurationInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateKmsConfiguration(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError).Message)
	})
}

// V1betaCheckKmsConfiguration unittests
func TestV1betaCheckKmsConfiguration(t *testing.T) {
	t.Run("WhenCheckKmsConfigurationSuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCheckKmsConfigParams{
			KmsConfigId:    "kms-config-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
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
		handler := Handler{}
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
		handler := Handler{}
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
		handler := Handler{}
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
		handler := Handler{}
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
		handler := Handler{}
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
		handler := Handler{}
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
		handler := Handler{}
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
		handler := Handler{}
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
		handler := Handler{}
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
