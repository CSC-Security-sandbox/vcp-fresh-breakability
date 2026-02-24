package sde

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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
		assert.Error(t, err)
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
		assert.Error(t, err)
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
		assert.Error(t, err)
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
		assert.Error(t, err)
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
		assert.Error(t, err)
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
		assert.Error(t, err)
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
		assert.Error(t, err)
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
		assert.Error(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError).Message)
	})
}

func TestListSDEKmsConfigurations(t *testing.T) {
	t.Run("WhenListSucceeds", func(t *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)
		projectNumber := "123456789"
		locationID := "us-east1"
		correlationID := "corr-id"
		resourceID := "resource-1"
		keyFullPath := "projects/123456789/locations/us-east1/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/1"

		mockClient.EXPECT().
			V1betaListKmsConfigurations(mock.MatchedBy(func(params *kms_configurations.V1betaListKmsConfigurationsParams) bool {
				return params != nil &&
					params.ProjectNumber == projectNumber &&
					params.LocationID == locationID &&
					params.XCorrelationID != nil &&
					*params.XCorrelationID == correlationID
			})).
			Return(&kms_configurations.V1betaListKmsConfigurationsOK{
				Payload: []*models.KmsConfigV1beta{
					{
						UUID:        "sde-uuid",
						ResourceID:  &resourceID,
						KeyFullPath: &keyFullPath,
					},
				},
			}, nil)

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, token string) cvpapi.Cvp {
			return *cvpClient
		}

		configs, err := ListSDEKmsConfigurations(context.Background(), projectNumber, locationID, correlationID)
		require.NoError(t, err)
		require.Len(t, configs, 1)
		require.Equal(t, "sde-uuid", configs[0].UUID)
		require.Equal(t, resourceID, configs[0].ResourceID)
		require.Equal(t, keyFullPath, configs[0].KeyFullPath)
	})

	t.Run("WhenListReturnsEmptyPayload", func(t *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)

		mockClient.EXPECT().
			V1betaListKmsConfigurations(mock.Anything).
			Return(&kms_configurations.V1betaListKmsConfigurationsOK{}, nil)

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, token string) cvpapi.Cvp {
			return *cvpClient
		}

		configs, err := ListSDEKmsConfigurations(context.Background(), "project", "location", "corr")
		require.Error(t, err)
		require.Nil(t, configs)
	})
}

func TestDeleteSDEKmsConfiguration(t *testing.T) {
	t.Run("WhenDeleteKmsConfigurationSuccess", func(t *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)
		params := &common.DeleteKmsConfigParams{
			Region:         "test-location",
			AccountName:    "test-account",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}
		done := false
		mockResponse := &kms_configurations.V1betaDeleteKmsConfigurationAccepted{
			Payload: &models.OperationV1beta{
				Name: "/v1beta/projects/909258763/locations/us-east4/operations/job-uuid",
				Done: &done,
			},
		}
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(mockResponse, nil, nil)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}
		res, err := DeleteSDEKmsConfiguration(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithBadRequest", func(t *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)
		params := &common.DeleteKmsConfigParams{
			Region:         "test-location",
			AccountName:    "test-account",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := DeleteSDEKmsConfiguration(context.Background(), req, params)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithUnprocessableEntity", func(t *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)
		params := &common.DeleteKmsConfigParams{
			Region:         "test-location",
			AccountName:    "test-account",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}
		errorCode := float64(422)
		errorMessage := "Unprocessable error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := DeleteSDEKmsConfiguration(context.Background(), req, params)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithConflict", func(t *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)
		params := &common.DeleteKmsConfigParams{
			Region:         "test-location",
			AccountName:    "test-account",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}
		errorCode := float64(409)
		errorMessage := "Conflict error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := DeleteSDEKmsConfiguration(context.Background(), req, params)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithUnauthorized", func(t *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)
		params := &common.DeleteKmsConfigParams{
			Region:         "test-location",
			AccountName:    "test-account",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := DeleteSDEKmsConfiguration(context.Background(), req, params)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithForbidden", func(t *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)
		params := &common.DeleteKmsConfigParams{
			Region:         "test-location",
			AccountName:    "test-account",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := DeleteSDEKmsConfiguration(context.Background(), req, params)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithTooManyRequests", func(t *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)
		params := &common.DeleteKmsConfigParams{
			Region:         "test-location",
			AccountName:    "test-account",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}
		errorCode := float64(429)
		errorMessage := "Too Many Requests error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := DeleteSDEKmsConfiguration(context.Background(), req, params)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithDefault", func(t *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)
		params := &common.DeleteKmsConfigParams{
			Region:         "test-location",
			AccountName:    "test-account",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := DeleteSDEKmsConfiguration(context.Background(), req, params)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("WhenDeleteKmsConfigurationFailsWithUnknownError", func(t *testing.T) {
		mockClient := kms_configurations.NewMockClientService(t)
		params := &common.DeleteKmsConfigParams{
			Region:         "test-location",
			AccountName:    "test-account",
			XCorrelationID: "test-correlation-id",
		}
		req := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeKmsConfigUUID: "test-id"},
		}
		errorCode := float64(500)
		errorMessage := "unknown error during the delete kms configurations"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().
			V1betaDeleteKmsConfiguration(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := DeleteSDEKmsConfiguration(context.Background(), req, params)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestDescribeSDEJob(t *testing.T) {
	t.Run("WhenJobIsDone", func(t *testing.T) {
		mockClient := async.NewMockClientService(t)
		done := true
		mockResponse := &async.V1betaDescribeOperationOK{
			Payload: &models.OperationV1beta{
				Done: &done,
			},
		}
		mockClient.EXPECT().
			V1betaDescribeOperation(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{Async: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		err := DescribeSDEJob(context.Background(), "op-id", "region", "account", "corr-id")
		assert.NoError(t, err)
	})

	t.Run("WhenJobIsNotDone", func(t *testing.T) {
		mockClient := async.NewMockClientService(t)
		done := false
		mockResponse := &async.V1betaDescribeOperationOK{
			Payload: &models.OperationV1beta{
				Done: &done,
			},
		}
		mockClient.EXPECT().
			V1betaDescribeOperation(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{Async: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		err := DescribeSDEJob(context.Background(), "op-id", "region", "account", "corr-id")
		assert.Error(t, err)
	})

	t.Run("WhenDescribeOperationReturnsError", func(t *testing.T) {
		mockClient := async.NewMockClientService(t)
		mockClient.EXPECT().
			V1betaDescribeOperation(mock.Anything).
			Return(nil, errors2.New("describe error"))
		cvpClient := &cvpapi.Cvp{Async: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		err := DescribeSDEJob(context.Background(), "op-id", "region", "account", "corr-id")
		assert.Error(t, err)
	})
}

func TestConvertCvpClientDeleteKmsConfigErrorToVcpError(t *testing.T) {
	t.Run("WhenConflictError", func(t *testing.T) {
		errorMessage := "Conflict error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationConflict{
			Payload: &models.Error{
				Code:    409,
				Message: errorMessage,
			},
		}
		result := convertCvpClientDeleteKmsConfigErrorToVcpError(mockError)
		require.NotNil(t, result)
		customErr, ok := result.(*vsaerrors.CustomError)
		require.True(t, ok, "Expected CustomError")
		assert.Equal(t, vsaerrors.ErrResourceStateConflictError, customErr.TrackingID)
		assert.Equal(t, errorMessage, customErr.OriginalErr.Error())
	})

	t.Run("WhenBadRequestError", func(t *testing.T) {
		errorMessage := "Bad Request"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationBadRequest{
			Payload: &models.Error{
				Code:    400,
				Message: errorMessage,
			},
		}
		result := convertCvpClientDeleteKmsConfigErrorToVcpError(mockError)
		require.NotNil(t, result)
		customErr, ok := result.(*vsaerrors.CustomError)
		require.True(t, ok, "Expected CustomError")
		assert.Equal(t, vsaerrors.ErrBadRequest, customErr.TrackingID)
		assert.Equal(t, errorMessage, customErr.OriginalErr.Error())
	})

	t.Run("WhenUnauthorizedError", func(t *testing.T) {
		errorMessage := "Unauthorized"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationUnauthorized{
			Payload: &models.Error{
				Code:    401,
				Message: errorMessage,
			},
		}
		result := convertCvpClientDeleteKmsConfigErrorToVcpError(mockError)
		require.NotNil(t, result)
		customErr, ok := result.(*vsaerrors.CustomError)
		require.True(t, ok, "Expected CustomError")
		assert.Equal(t, vsaerrors.ErrUnauthorized, customErr.TrackingID)
		assert.Equal(t, errorMessage, customErr.OriginalErr.Error())
	})

	t.Run("WhenForbiddenError", func(t *testing.T) {
		errorMessage := "Forbidden"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationForbidden{
			Payload: &models.Error{
				Code:    403,
				Message: errorMessage,
			},
		}
		result := convertCvpClientDeleteKmsConfigErrorToVcpError(mockError)
		require.NotNil(t, result)
		customErr, ok := result.(*vsaerrors.CustomError)
		require.True(t, ok, "Expected CustomError")
		assert.Equal(t, vsaerrors.ErrForbidden, customErr.TrackingID)
		assert.Equal(t, errorMessage, customErr.OriginalErr.Error())
	})

	t.Run("WhenTooManyRequestsError", func(t *testing.T) {
		errorMessage := "Too many requests"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationTooManyRequests{
			Payload: &models.Error{
				Code:    429,
				Message: errorMessage,
			},
		}
		result := convertCvpClientDeleteKmsConfigErrorToVcpError(mockError)
		require.NotNil(t, result)
		customErr, ok := result.(*vsaerrors.CustomError)
		require.True(t, ok, "Expected CustomError")
		assert.Equal(t, vsaerrors.ErrTooManyRequests, customErr.TrackingID)
		assert.Equal(t, errorMessage, customErr.OriginalErr.Error())
	})

	t.Run("WhenUnprocessableEntityError", func(t *testing.T) {
		errorMessage := "Unprocessable Entity"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationUnprocessableEntity{
			Payload: &models.Error{
				Code:    422,
				Message: errorMessage,
			},
		}
		result := convertCvpClientDeleteKmsConfigErrorToVcpError(mockError)
		require.NotNil(t, result)
		customErr, ok := result.(*vsaerrors.CustomError)
		require.True(t, ok, "Expected CustomError")
		assert.Equal(t, vsaerrors.ErrUnprocessableEntity, customErr.TrackingID)
		assert.Equal(t, errorMessage, customErr.OriginalErr.Error())
	})

	t.Run("WhenDefaultError", func(t *testing.T) {
		errorMessage := "Default error"
		mockError := &kms_configurations.V1betaDeleteKmsConfigurationDefault{
			Payload: &models.Error{
				Code:    500,
				Message: errorMessage,
			},
		}
		result := convertCvpClientDeleteKmsConfigErrorToVcpError(mockError)
		require.NotNil(t, result)
		customErr, ok := result.(*vsaerrors.CustomError)
		require.True(t, ok, "Expected CustomError")
		assert.Equal(t, vsaerrors.ErrKMSDeleteSDE, customErr.TrackingID)
		assert.Equal(t, errorMessage, customErr.OriginalErr.Error())
	})

	t.Run("WhenUnknownError", func(t *testing.T) {
		unknownError := errors2.New("unknown error type")
		result := convertCvpClientDeleteKmsConfigErrorToVcpError(unknownError)
		require.NotNil(t, result)
		customErr, ok := result.(*vsaerrors.CustomError)
		require.True(t, ok, "Expected CustomError")
		assert.Equal(t, vsaerrors.ErrKMSDeleteSDE, customErr.TrackingID)
		assert.Equal(t, "unknown error during the delete kms configurations", customErr.OriginalErr.Error())
	})
}
