package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/active_directories"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestV1betaListActiveDirectories(t *testing.T) {
	// Create a mock client
	mockClient := active_directories.NewMockClientService(t)

	// Define input parameters
	params := gcpgenserver.V1betaListActiveDirectoriesParams{
		LocationId:     "test-location",
		ProjectNumber:  "12345",
		XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
	}

	// Define mock response
	mockResponse := &active_directories.V1betaListActiveDirectoriesOK{
		Payload: &active_directories.V1betaListActiveDirectoriesOKBody{
			ActiveDirectories: []*models.ActiveDirectoryV1beta{
				{
					ActiveDirectoryID:           "ad-1",
					ResourceID:                  nillable.GetStringPtr("resource-1"),
					Username:                    nillable.GetStringPtr("user1"),
					Password:                    nillable.GetStringPtr("pass1"),
					Domain:                      nillable.GetStringPtr("domain1"),
					DNS:                         nillable.GetStringPtr("dns1"),
					NetBIOS:                     nillable.GetStringPtr("netbios1"),
					OrganizationalUnit:          new(string),
					Site:                        new(string),
					ActiveDirectoryState:        "ACTIVE",
					ActiveDirectoryStateDetails: "Details",
					LdapSigning:                 new(bool),
					AllowLocalNFSUsersWithLdap:  new(bool),
					EncryptDCConnections:        new(bool),
					SecurityOperators:           []string{"operator1"},
					BackupOperators:             []string{"backup1"},
					Administrators:              []string{"admin1"},
					AesEncryption:               new(bool),
				},
			},
		},
	}

	// Set up the mock client behavior
	mockClient.EXPECT().
		V1betaListActiveDirectories(mock.Anything).
		Return(mockResponse, nil)
	cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
	originalCreateClient := createClient
	defer func() { createClient = originalCreateClient }()
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}
	handler := Handler{}
	// Call the method under test
	result, err := handler.V1betaListActiveDirectories(context.Background(), params)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.(*gcpgenserver.V1betaListActiveDirectoriesOK).ActiveDirectories))
	assert.Equal(t, "ad-1", result.(*gcpgenserver.V1betaListActiveDirectoriesOK).ActiveDirectories[0].ActiveDirectoryId.Value)
}

// V1betaCreateActiveDirectory unittests
func TestV1betaCreateActiveDirectory(t *testing.T) {
	t.Run("WhenCreateActiveDirectorySuccessWithRequiredParamsOnly", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.ActiveDirectoryV1beta{
			ResourceId: "ad-1",
			Username:   "user1",
			Password:   "pass1",
			Domain:     "domain1.com",
			DNS:        "10.20.0.1",
			NetBIOS:    "netbios1",
		}

		// Define mock response
		mockResponse := &active_directories.V1betaCreateActiveDirectoryAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateActiveDirectory(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenCreateActiveDirectorySuccessWithOptionalParams", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryV1beta{
			ResourceId:                 "ad-1",
			Username:                   "user1",
			Password:                   "pass1",
			Domain:                     "domain1.com",
			DNS:                        "10.20.0.20",
			NetBIOS:                    "netbios1",
			OrganizationalUnit:         gcpgenserver.NewOptString("OU=Test,DC=domain1,DC=com"),
			Site:                       gcpgenserver.NewOptString("site.com"),
			LdapSigning:                gcpgenserver.NewOptBool(true),
			AllowLocalNFSUsersWithLdap: gcpgenserver.NewOptBool(true),
			EncryptDCConnections:       gcpgenserver.NewOptBool(true),
			BackupOperators:            []string{"backup1"},
			Administrators:             []string{"admin1"},
			SecurityOperators:          []string{"operator1"},
			AesEncryption:              gcpgenserver.NewOptBool(true),
			Description:                gcpgenserver.NewOptString("Test AD"),
			KdcIP:                      gcpgenserver.NewOptString("10.20.0.20"),
			KdcHostname:                gcpgenserver.NewOptString("KdcHostname"),
		}

		// Define mock response
		mockResponse := &active_directories.V1betaCreateActiveDirectoryAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateActiveDirectory(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenCreateActiveDirectoryFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryV1beta{}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaCreateActiveDirectoryBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateActiveDirectoryBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateActiveDirectoryBadRequest).Message)
	})

	t.Run("WhenCreateActiveDirectoryFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryV1beta{}
		// Define mock error
		errorMessage := "Conflict error"
		errorCode := float64(409)
		mockError := &active_directories.V1betaCreateActiveDirectoryConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateActiveDirectoryConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateActiveDirectoryConflict).Message)
	})

	t.Run("WhenCreateActiveDirectoryFailsWithUnprocessableEntry", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryV1beta{}
		// Define mock error
		errorMessage := "Unprocessable error"
		errorCode := float64(422)
		mockError := &active_directories.V1betaCreateActiveDirectoryConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateActiveDirectoryConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateActiveDirectoryConflict).Message)
	})

	t.Run("WhenCreateActiveDirectoryFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryV1beta{}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaCreateActiveDirectoryUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateActiveDirectoryUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateActiveDirectoryUnauthorized).Message)
	})

	t.Run("WhenCreateActiveDirectoryFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryV1beta{}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &active_directories.V1betaCreateActiveDirectoryForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateActiveDirectoryForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateActiveDirectoryForbidden).Message)
	})

	t.Run("WhenCreateActiveDirectoryFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryV1beta{}
		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaCreateActiveDirectoryTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateActiveDirectoryTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateActiveDirectoryTooManyRequests).Message)
	})

	t.Run("WhenCreateActiveDirectoryFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryV1beta{}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &active_directories.V1betaCreateActiveDirectoryDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateActiveDirectoryInternalServerError).Code)
	})

	t.Run("WhenCreateActiveDirectoryFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryV1beta{}
		// Define mock error
		errorMessage := "unknown error during the create active directory"
		errorCode := float64(500)
		mockError := &active_directories.V1betaCreateActiveDirectoryInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateActiveDirectoryInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateActiveDirectoryInternalServerError).Message)
	})
}

// V1betaDeleteActiveDirectory unittests
func TestV1betaDeleteActiveDirectory(t *testing.T) {
	t.Run("WhenDeleteActiveDirectorySuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock response
		mockResponse := &active_directories.V1betaDeleteActiveDirectoryAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaDeleteActiveDirectoryBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryBadRequest).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Conflict error"
		errorCode := float64(409)
		mockError := &active_directories.V1betaDeleteActiveDirectoryConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryConflict).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithUnprocessableEntry", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Unprocessable error"
		errorCode := float64(422)
		mockError := &active_directories.V1betaDeleteActiveDirectoryUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryUnprocessableEntity).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryUnprocessableEntity).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaDeleteActiveDirectoryUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryUnauthorized).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &active_directories.V1betaDeleteActiveDirectoryForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryForbidden).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaDeleteActiveDirectoryTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryTooManyRequests).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &active_directories.V1betaDeleteActiveDirectoryDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryInternalServerError).Code)
	})
}

// V1betaDescribeActiveDirectory unittests
func TestV1betaDescribeActiveDirectory(t *testing.T) {
	t.Run("WhenDescribeActiveDirectorySuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock response
		dns := "10.20.2.2"
		domainName := "test-domain.com"
		netBios := "test-domain"
		userName := "test-user"
		password := "test-password"
		description := "test description"

		mockResponse := &active_directories.V1betaDescribeActiveDirectoryOK{
			Payload: &models.ActiveDirectoryV1beta{
				ActiveDirectoryID:          "ad-1",
				ResourceID:                 nillable.GetStringPtr("resource-id"),
				DNS:                        &dns,
				Domain:                     &domainName,
				NetBIOS:                    &netBios,
				Username:                   &userName,
				Password:                   &password,
				Description:                &description,
				AesEncryption:              nillable.GetBoolPtr(false),
				EncryptDCConnections:       nillable.GetBoolPtr(false),
				LdapSigning:                nillable.GetBoolPtr(false),
				AllowLocalNFSUsersWithLdap: nillable.GetBoolPtr(false),
				KdcIP:                      dns,
				KdcHostname:                "test-hostname",
				Site:                       nillable.GetStringPtr("test-site"),
				OrganizationalUnit:         nillable.GetStringPtr("test-ou"),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, "ad-1", result.(*gcpgenserver.ActiveDirectoryV1beta).ActiveDirectoryId.Value)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaDescribeActiveDirectoryBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeActiveDirectoryBadRequest).Message)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithUnprocessableEntry", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Unprocessable error"
		errorCode := float64(422)
		mockError := &active_directories.V1betaDescribeActiveDirectoryUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryUnprocessableEntity).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeActiveDirectoryUnprocessableEntity).Message)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaDescribeActiveDirectoryUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeActiveDirectoryUnauthorized).Message)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &active_directories.V1betaDescribeActiveDirectoryForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeActiveDirectoryForbidden).Message)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaDescribeActiveDirectoryTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeActiveDirectoryTooManyRequests).Message)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &active_directories.V1betaDescribeActiveDirectoryDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryInternalServerError).Code)
	})
}

// V1betaUpdateActiveDirectory unittests
func TestV1betaUpdateActiveDirectory(t *testing.T) {
	t.Run("WhenUpdateActiveDirectorySuccess", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{
			Username:                   gcpgenserver.NewOptString("user1"),
			Password:                   gcpgenserver.NewOptString("pass1"),
			Domain:                     gcpgenserver.NewOptString("domain1.com"),
			DNS:                        gcpgenserver.NewOptString("10.20.0.20"),
			NetBIOS:                    gcpgenserver.NewOptString("domain1"),
			OrganizationalUnit:         gcpgenserver.NewOptString("OU=Test,DC=domain1,DC=com"),
			Site:                       gcpgenserver.NewOptString("site.com"),
			LdapSigning:                gcpgenserver.NewOptBool(true),
			AllowLocalNFSUsersWithLdap: gcpgenserver.NewOptBool(true),
			EncryptDCConnections:       gcpgenserver.NewOptBool(true),
			BackupOperators:            []string{"backup1"},
			Administrators:             []string{"admin1"},
			SecurityOperators:          []string{"operator1"},
			AesEncryption:              gcpgenserver.NewOptBool(true),
			Description:                gcpgenserver.NewOptString("Test AD"),
			KdcIP:                      gcpgenserver.NewOptString("10.20.0.20"),
			KdcHostname:                gcpgenserver.NewOptString("KdcHostname"),
		}

		// Define mock response
		mockResponse := &active_directories.V1betaUpdateActiveDirectoryAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaUpdateActiveDirectoryBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryBadRequest).Message)
	})
	t.Run("WhenUpdateActiveDirectoryFailsWithNotFound", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorCode := float64(404)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaUpdateActiveDirectoryNotFound{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryNotFound).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryNotFound).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "Conflict error"
		errorCode := float64(409)
		mockError := &active_directories.V1betaUpdateActiveDirectoryConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryConflict).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithUnprocessableEntry", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "Unprocessable error"
		errorCode := float64(422)
		mockError := &active_directories.V1betaUpdateActiveDirectoryConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryConflict).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaUpdateActiveDirectoryUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryUnauthorized).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &active_directories.V1betaUpdateActiveDirectoryForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryForbidden).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaUpdateActiveDirectoryTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryTooManyRequests).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &active_directories.V1betaUpdateActiveDirectoryDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryInternalServerError).Code)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "unknown error during the update active directory"
		errorCode := float64(500)
		mockError := &active_directories.V1betaUpdateActiveDirectoryInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryInternalServerError).Message)
	})
}

// V1betaGetMultipleActiveDirectories unittests
func TestV1betaGetMultipleActiveDirectories(t *testing.T) {
	t.Run("WhenGetMultipleActiveDirectoriesSuccess", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		ads := make([]*models.ActiveDirectoryV1beta, 0)
		resourceID := "AD0"
		dns := "10.20.2.3"
		domainName := "domain1.com"
		netBios := "domain1"
		userName := "user1"
		password := "pass1"
		description := "Test AD"

		ads = append(ads, &models.ActiveDirectoryV1beta{
			ActiveDirectoryID: "AD0",
			ResourceID:        &resourceID,
			DNS:               &dns,
			Domain:            &domainName,
			NetBIOS:           &netBios,
			Username:          &userName,
			Password:          &password,
			Description:       &description,
		})

		// Define mock response
		mockResponse := &active_directories.V1betaGetMultipleActiveDirectoriesOK{
			Payload: &active_directories.V1betaGetMultipleActiveDirectoriesOKBody{
				ActiveDirectories: ads,
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "AD0", result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesOK).ActiveDirectories[0].ActiveDirectoryId.Value)
		assert.Equal(t, 1, len(result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesOK).ActiveDirectories))
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesBadRequest).Message)
	})
	t.Run("WhenGetMultipleActiveDirectoriesFailsWithNotFound", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorCode := float64(404)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesNotFound{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesNotFound).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesNotFound).Message)
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesUnauthorized).Message)
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesForbidden).Message)
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesTooManyRequests).Message)
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesInternalServerError).Code)
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}
		// Define mock error
		errorMessage := "unknown error during the get multiple active directories"
		errorCode := float64(500)
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesInternalServerError).Message)
	})
}
