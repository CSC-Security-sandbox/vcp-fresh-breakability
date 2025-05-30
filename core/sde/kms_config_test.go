package sde

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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestV1betaUpdateKmsConfiguration(t *testing.T) {
	t.Run("WhenUpdateKmsConfigurationSuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := &common.UpdateKmsConfigParams{
			KmsConfigID:    "kms-config-id-1",
			Region:         "test-location",
			KeyName:        "key1",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{
				UUID: "test-id",
			},
			KeyName:       "key",
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
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
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		// Call the method under test
		_, err := UpdateSDEKmsConfiguration(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
	})

	t.Run("WhenUpdateKmsConfigurationFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := kms_configurations.NewMockClientService(t)

		// Define input parameters
		params := &common.UpdateKmsConfigParams{
			KmsConfigID:    "kms-config-id-1",
			Region:         "test-location",
			KeyName:        "key1",
			XCorrelationID: "test-correlation-id",
		}
		// Define request
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}

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
		// Call the method under test
		result, err := UpdateSDEKmsConfiguration(context.Background(), req, params)
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
		params := &common.UpdateKmsConfigParams{
			KmsConfigID:    "kms-config-id-1",
			Region:         "test-location",
			KeyName:        "key1",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}

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
		// Call the method under test
		result, err := UpdateSDEKmsConfiguration(context.Background(), req, params)
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
		params := &common.UpdateKmsConfigParams{
			KmsConfigID:    "kms-config-id-1",
			Region:         "test-location",
			KeyName:        "key1",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}

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
		// Call the method under test
		result, err := UpdateSDEKmsConfiguration(context.Background(), req, params)
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
		params := &common.UpdateKmsConfigParams{
			KmsConfigID:    "kms-config-id-1",
			Region:         "test-location",
			KeyName:        "key1",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}

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
		// Call the method under test
		result, err := UpdateSDEKmsConfiguration(context.Background(), req, params)
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
		params := &common.UpdateKmsConfigParams{
			KmsConfigID:    "kms-config-id-1",
			Region:         "test-location",
			KeyName:        "key1",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}

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
		// Call the method under test
		result, err := UpdateSDEKmsConfiguration(context.Background(), req, params)
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
		params := &common.UpdateKmsConfigParams{
			KmsConfigID:    "kms-config-id-1",
			Region:         "test-location",
			KeyName:        "key1",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}

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
		// Call the method under test
		result, err := UpdateSDEKmsConfiguration(context.Background(), req, params)
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
		params := &common.UpdateKmsConfigParams{
			KmsConfigID:    "kms-config-id-1",
			Region:         "test-location",
			KeyName:        "key1",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}

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
		// Call the method under test
		result, err := UpdateSDEKmsConfiguration(context.Background(), req, params)
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
		params := &common.UpdateKmsConfigParams{
			KmsConfigID:    "kms-config-id-1",
			Region:         "test-location",
			KeyName:        "key1",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}

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
		// Call the method under test
		result, err := UpdateSDEKmsConfiguration(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError).Message)
	})
}
