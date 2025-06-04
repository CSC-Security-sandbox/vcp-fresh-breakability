package api

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	mod "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// V1betaListBackupVaults
func TestV1betaListBackupVaults(t *testing.T) {
	// Create a mock client
	mockClient := backup_vault.NewMockClientService(t)

	// Define input parameters
	params := gcpgenserver.V1betaListBackupVaultsParams{
		LocationId:     "test-location",
		ProjectNumber:  "12345",
		XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
	}

	// Define mock response
	mockResponse := &backup_vault.V1betaListBackupVaultsOK{
		Payload: &backup_vault.V1betaListBackupVaultsOKBody{
			BackupVaults: []*models.BackupVaultV1beta{
				{
					BackupRegion:  nillable.GetStringPtr("backup-region"),
					BackupVaultID: "backup-id",
				},
			},
		},
	}

	// Set up the mock client behavior
	mockClient.EXPECT().
		V1betaListBackupVaults(mock.Anything).
		Return(mockResponse, nil)
	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	originalCreateClient := createClient
	defer func() { createClient = originalCreateClient }()
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}
	handler := Handler{}
	// Call the method under test
	result, err := handler.V1betaListBackupVaults(context.Background(), params)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.(*gcpgenserver.V1betaListBackupVaultsOK).BackupVaults))
	assert.Equal(t, "backup-id", result.(*gcpgenserver.V1betaListBackupVaultsOK).BackupVaults[0].BackupVaultId.Value)
}

// V1betaDeleteBackupVault unittests
func TestV1betaDeleteBackupVault(t *testing.T) {
	t.Run("WhenDeleteBackupVaultSuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock response
		mockResponse := &backup_vault.V1betaDeleteBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupVault(mock.Anything).
			Return(mockResponse, nil, nil)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenDeleteBackupVaultFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backup_vault.V1betaDeleteBackupVaultBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupVault(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupVault(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupVaultBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupVaultBadRequest).Message)
	})

	t.Run("WhenDeleteBackupVaultFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorMessage := "Conflict error"
		errorCode := float64(409)
		mockError := &backup_vault.V1betaDeleteBackupVaultConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupVault(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupVaultConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupVaultConflict).Message)
	})

	t.Run("WhenDeleteBackupVaultFailsWithUnprocessableEntry", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorMessage := "Unprocessable error"
		errorCode := float64(422)
		mockError := &backup_vault.V1betaDeleteBackupVaultUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupVault(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupVaultUnprocessableEntity).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupVaultUnprocessableEntity).Message)
	})

	t.Run("WhenDeleteBackupVaultFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &backup_vault.V1betaDeleteBackupVaultUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupVault(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupVaultUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupVaultUnauthorized).Message)
	})

	t.Run("WhenDeleteBackupVaultFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &backup_vault.V1betaDeleteBackupVaultForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupVault(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupVaultForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupVaultForbidden).Message)
	})

	t.Run("WhenDeleteBackupVaultFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &backup_vault.V1betaDeleteBackupVaultTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupVault(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupVaultTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupVaultTooManyRequests).Message)
	})

	t.Run("WhenDeleteBackupVaultFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &backup_vault.V1betaDeleteBackupVaultDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupVault(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupVaultInternalServerError).Code)
	})
}

// V1betaDescribeBackupVault unittests
func TestV1betaDescribeBackupVault(t *testing.T) {
	t.Run("WhenDescribeBackupVaultSuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		bvRetentionPolicy := models.BackupRetentionPolicyV1beta{
			DailyBackupImmutable:               false,
			MonthlyBackupImmutable:             false,
			ManualBackupImmutable:              false,
			WeeklyBackupImmutable:              false,
			BackupMinimumEnforcedRetentionDays: nillable.GetInt64Ptr(2),
		}

		mockResponse := &backup_vault.V1betaDescribeBackupVaultOK{
			Payload: &models.BackupVaultV1beta{
				ResourceID:             nillable.GetStringPtr(gcpgenserver.NewOptString("bv-1").Value),
				BackupRegion:           nillable.GetStringPtr("br-1"),
				BackupVaultID:          "bvid-1",
				BackupVaultType:        nillable.GetStringPtr("bvtype-1"),
				Description:            nillable.GetStringPtr("Test Description"),
				DestinationBackupVault: nillable.GetStringPtr("dbv-1"),
				SourceBackupVault:      nillable.GetStringPtr("sbv-1"),
				SourceRegion:           nillable.GetStringPtr("sr-1"),
				State:                  "ACTIVE",
				StateDetails:           "DETAILS",
				BackupRetentionPolicy:  &bvRetentionPolicy,
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupVault(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, "bvid-1", result.(*gcpgenserver.BackupVaultV1beta).BackupVaultId.Value)
	})

	t.Run("WhenDescribeBackupVaultFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backup_vault.V1betaDescribeBackupVaultBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupVault(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeBackupVaultBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeBackupVaultBadRequest).Message)
	})

	t.Run("WhenDescribeBackupVaultFailsWithUnprocessableEntry", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorMessage := "Unprocessable error"
		errorCode := float64(422)
		mockError := &backup_vault.V1betaDescribeBackupVaultUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeBackupVaultUnprocessableEntity).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeBackupVaultUnprocessableEntity).Message)
	})

	t.Run("WhenDescribeBackupVaultFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &backup_vault.V1betaDescribeBackupVaultUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeBackupVaultUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeBackupVaultUnauthorized).Message)
	})

	t.Run("WhenDescribeBackupVaultFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &backup_vault.V1betaDescribeBackupVaultForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeBackupVaultForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeBackupVaultForbidden).Message)
	})

	t.Run("WhenDescribeBackupVaultFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &backup_vault.V1betaDescribeBackupVaultTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeBackupVaultTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeBackupVaultTooManyRequests).Message)
	})

	t.Run("WhenDescribeBackupVaultFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &backup_vault.V1betaDescribeBackupVaultDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeBackupVaultInternalServerError).Code)
	})
}

// V1betaUpdateBackupVault unittests
func TestV1betaUpdateBackupVault(t *testing.T) {
	t.Run("WhenUpdateBackupVaultSuccess", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}

		bvRetentionPolicy := gcpgenserver.BackupRetentionPolicyUpdateV1beta{
			DailyBackupImmutable:               gcpgenserver.NewOptBool(false),
			MonthlyBackupImmutable:             gcpgenserver.NewOptBool(false),
			ManualBackupImmutable:              gcpgenserver.NewOptBool(false),
			WeeklyBackupImmutable:              gcpgenserver.NewOptBool(false),
			BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(2),
		}
		// Define request
		req := &gcpgenserver.BackupVaultUpdateV1beta{
			Description:           gcpgenserver.NewOptString("test description"),
			BackupRetentionPolicy: gcpgenserver.NewOptBackupRetentionPolicyUpdateV1beta(bvRetentionPolicy),
		}

		// Define mock response
		mockResponse := &backup_vault.V1betaUpdateBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupVault(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenUpdateBackupVaultFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define request
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backup_vault.V1betaUpdateBackupVaultBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupVaultBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupVaultBadRequest).Message)
	})
	t.Run("WhenUpdateBackupVaultFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define request
		// Define request
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		// Define mock error
		errorMessage := "Conflict error"
		errorCode := float64(409)
		mockError := &backup_vault.V1betaUpdateBackupVaultConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupVaultConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupVaultConflict).Message)
	})

	t.Run("WhenUpdateBackupVaultFailsWithUnprocessableEntry", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define request
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		// Define mock error
		errorMessage := "Unprocessable error"
		errorCode := float64(422)
		mockError := &backup_vault.V1betaUpdateBackupVaultConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupVaultConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupVaultConflict).Message)
	})

	t.Run("WhenUpdateBackupVaultFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define request
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &backup_vault.V1betaUpdateBackupVaultUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupVaultUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupVaultUnauthorized).Message)
	})

	t.Run("WhenUpdateBackupVaultFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define request
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &backup_vault.V1betaUpdateBackupVaultForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupVaultForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupVaultForbidden).Message)
	})

	t.Run("WhenUpdateBackupVaultFailsWithTooManyRequests", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define request
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &backup_vault.V1betaUpdateBackupVaultTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupVaultTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupVaultTooManyRequests).Message)
	})

	t.Run("WhenUpdateBackupVaultFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define request
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &backup_vault.V1betaUpdateBackupVaultDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupVaultInternalServerError).Code)
	})

	t.Run("WhenUpdateBackupVaultFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "bv-1",
		}
		// Define request
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		// Define mock error
		errorMessage := "unknown error during the update backup vault"
		errorCode := float64(500)
		mockError := &backup_vault.V1betaUpdateBackupVaultInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupVault(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupVaultInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupVaultInternalServerError).Message)
	})
}

// V1betaGetMultipleBackupVaults unittests
func TestV1betaGetMultipleBackupVaults(t *testing.T) {
	t.Run("WhenGetMultipleBackupVaultsSuccess", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupVaultsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupVaultUuidListV1beta{
			BackupVaultUuids: []string{"bvid-1"},
		}

		bvs := make([]*models.BackupVaultV1beta, 0)

		bvs = append(bvs, &models.BackupVaultV1beta{
			ResourceID:             nillable.GetStringPtr("bv-1"),
			BackupRegion:           nillable.GetStringPtr("br-1"),
			SourceRegion:           nillable.GetStringPtr("sr-1"),
			BackupVaultID:          "bvid-1",
			BackupVaultType:        nillable.GetStringPtr("bvtype-1"),
			Description:            nillable.GetStringPtr("test description"),
			SourceBackupVault:      nillable.GetStringPtr("sbv-1"),
			DestinationBackupVault: nillable.GetStringPtr("dbv-1"),
		})

		// Define mock response
		mockResponse := &backup_vault.V1betaGetMultipleBackupVaultsOK{
			Payload: &backup_vault.V1betaGetMultipleBackupVaultsOKBody{
				BackupVaults: bvs,
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupVaults(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupVaults(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "bvid-1", result.(*gcpgenserver.V1betaGetMultipleBackupVaultsOK).BackupVaults[0].BackupVaultId.Value)
		assert.Equal(t, 1, len(result.(*gcpgenserver.V1betaGetMultipleBackupVaultsOK).BackupVaults))
	})

	t.Run("WhenGetMultipleBackupVaultsFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupVaultsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupVaultUuidListV1beta{
			BackupVaultUuids: []string{"BV0"},
		}

		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backup_vault.V1betaGetMultipleBackupVaultsBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupVaults(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupVaults(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupVaultsBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupVaultsBadRequest).Message)
	})
	t.Run("WhenGetMultipleBackupVaultsFailsWithNotFound", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupVaultsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupVaultUuidListV1beta{
			BackupVaultUuids: []string{"BV0"},
		}

		// Define mock error
		errorCode := float64(404)
		errorMessage := "Bad Request"
		mockError := &backup_vault.V1betaGetMultipleBackupVaultsNotFound{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupVaults(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupVaults(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupVaultsNotFound).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupVaultsNotFound).Message)
	})

	t.Run("WhenGetMultipleBackupVaultsFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupVaultsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupVaultUuidListV1beta{
			BackupVaultUuids: []string{"BV0"},
		}

		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &backup_vault.V1betaGetMultipleBackupVaultsUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupVaults(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupVaults(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupVaultsUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupVaultsUnauthorized).Message)
	})

	t.Run("WhenGetMultipleBackupVaultsFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupVaultsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupVaultUuidListV1beta{
			BackupVaultUuids: []string{"BV0"},
		}

		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &backup_vault.V1betaGetMultipleBackupVaultsForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupVaults(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupVaults(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupVaultsForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupVaultsForbidden).Message)
	})

	t.Run("WhenGetMultipleBackupVaultsFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupVaultsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupVaultUuidListV1beta{
			BackupVaultUuids: []string{"BV0"},
		}

		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &backup_vault.V1betaGetMultipleBackupVaultsDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupVaults(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupVaults(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupVaultsInternalServerError).Code)
	})

	t.Run("WhenGetMultipleBackupVaultsFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupVaultsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupVaultUuidListV1beta{
			BackupVaultUuids: []string{"BV0"},
		}

		// Define mock error
		errorMessage := "unknown error during the get multiple backup vaults"
		errorCode := float64(500)
		mockError := &backup_vault.V1betaGetMultipleBackupVaultsInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupVaults(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupVaults(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupVaultsInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupVaultsInternalServerError).Message)
	})
}

func ValidBackupVaultParams() *gcpgenserver.BackupVaultCreateV1beta {
	return &gcpgenserver.BackupVaultCreateV1beta{
		ResourceId:  gcpgenserver.NewOptString("vault1"),
		Description: gcpgenserver.NewOptString("Test backup vault description"),
		BackupRetentionPolicy: gcpgenserver.NewOptBackupRetentionPolicyV1beta(
			gcpgenserver.BackupRetentionPolicyV1beta{ // Use the value type instead of a pointer
				BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(30),
				DailyBackupImmutable:               gcpgenserver.NewOptBool(true),
				WeeklyBackupImmutable:              gcpgenserver.NewOptBool(false),
				MonthlyBackupImmutable:             gcpgenserver.NewOptBool(true),
				ManualBackupImmutable:              gcpgenserver.NewOptBool(false),
			},
		),
		BackupRegion: gcpgenserver.NewOptString("us-central1"),
	}
}

func TestReturnsValidBackupVaultParams(t *testing.T) {
	t.Run("ReturnsValidBackupVaultParams", func(t *testing.T) {
		req := ValidBackupVaultParams()
		params := gcpgenserver.V1betaCreateBackupVaultParams{
			LocationId:    "us-central1",
			ProjectNumber: "1234567890",
		}
		region := "us-central1"

		result := createBackupVaultParams(req, params, region)

		assert.NotNil(t, result)
		assert.Equal(t, "vault1", result.Name)
		assert.Equal(t, "Test backup vault description", *result.Description)
		assert.Equal(t, "us-central1", *result.BackupRegion)
		assert.Equal(t, int64(30), *result.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration)
		assert.True(t, *result.BackupRetentionPolicy.IsDailyBackupImmutable)
		assert.False(t, *result.BackupRetentionPolicy.IsWeeklyBackupImmutable)
		assert.True(t, *result.BackupRetentionPolicy.IsMonthlyBackupImmutable)
		assert.False(t, *result.BackupRetentionPolicy.IsAdhocBackupImmutable)
	})
	t.Run("HandlesUnsetOptionalFields", func(t *testing.T) {
		req := &gcpgenserver.BackupVaultCreateV1beta{
			ResourceId: gcpgenserver.NewOptString("vault1"),
		}
		params := gcpgenserver.V1betaCreateBackupVaultParams{
			LocationId:    "us-central1",
			ProjectNumber: "1234567890",
		}
		region := "us-central1"

		result := createBackupVaultParams(req, params, region)

		assert.NotNil(t, result)
		assert.Equal(t, "vault1", result.Name)
		assert.Nil(t, result.Description)
		assert.Nil(t, result.BackupRegion)
		assert.Nil(t, result.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration)
		assert.Nil(t, result.BackupRetentionPolicy.IsDailyBackupImmutable)
		assert.Nil(t, result.BackupRetentionPolicy.IsWeeklyBackupImmutable)
		assert.Nil(t, result.BackupRetentionPolicy.IsMonthlyBackupImmutable)
		assert.Nil(t, result.BackupRetentionPolicy.IsAdhocBackupImmutable)
	})
	t.Run("HandlesInvalidRetentionPolicy", func(t *testing.T) {
		req := &gcpgenserver.BackupVaultCreateV1beta{
			ResourceId: gcpgenserver.NewOptString("vault1"),
			BackupRetentionPolicy: gcpgenserver.NewOptBackupRetentionPolicyV1beta(
				gcpgenserver.BackupRetentionPolicyV1beta{ // Use the value type instead of a pointer
					BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(-1), // Invalid retention days
				},
			),
		}
		params := gcpgenserver.V1betaCreateBackupVaultParams{
			LocationId:    "us-central1",
			ProjectNumber: "1234567890",
		}
		region := "us-central1"

		result := createBackupVaultParams(req, params, region)

		assert.NotNil(t, result)
		assert.Equal(t, "vault1", result.Name)
		assert.Nil(t, result.Description)
		assert.Nil(t, result.BackupRegion)
		assert.Equal(t, int64(-1), *result.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration)
	})
}

func TestReturnsNilWhenOptIsNotSet(t *testing.T) {
	opt := gcpgenserver.OptBackupRetentionPolicyV1beta{}
	result := safeBoolPointer(opt, func() bool { return true })
	assert.Nil(t, result)
}

func Test_CreateBackupVaultV1beta(t *testing.T) {
	t.Run("ReturnsBadRequestWhenRegionParsingFails", func(t *testing.T) {
		params := gcpgenserver.V1betaCreateBackupVaultParams{
			LocationId:    "local",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.BackupVaultCreateV1beta{
			ResourceId:   gcpgenserver.NewOptString("vault1"),
			BackupRegion: gcpgenserver.NewOptString("invalid-region"), // Invalid region to trigger error
			BackupRetentionPolicy: gcpgenserver.NewOptBackupRetentionPolicyV1beta(
				gcpgenserver.BackupRetentionPolicyV1beta{
					BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(30),
					DailyBackupImmutable:               gcpgenserver.NewOptBool(true),
					WeeklyBackupImmutable:              gcpgenserver.NewOptBool(false),
					MonthlyBackupImmutable:             gcpgenserver.NewOptBool(true),
				}),
		}

		handler := Handler{}

		result, err := handler.V1betaCreateBackupVault(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaCreateBackupVaultBadRequest{}, result)
		assert.Equal(t, "LocationID represents neither a region nor a zone", result.(*gcpgenserver.V1betaCreateBackupVaultBadRequest).Message)
	})
	t.Run("ReturnsExistingBackupVaultWhenAlreadyExists", func(t *testing.T) {
		req := &gcpgenserver.BackupVaultCreateV1beta{
			ResourceId: gcpgenserver.NewOptString("existing-vault"),
		}
		params := gcpgenserver.V1betaCreateBackupVaultParams{
			LocationId:    "us-central1",
			ProjectNumber: "1234567890",
		}
		desc := "New backup vault"
		mrd := int64(30)

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, "existing-vault", "1234567890").
			Return(&mod.BackupVaultV1beta{
				Name:        "existing-vault",
				Description: &desc,
				BackupRetentionPolicy: mod.BackupRetentionPolicyparams{
					BackupMinimumEnforcedRetentionDuration: &mrd,
					IsDailyBackupImmutable:                 false,
					IsMonthlyBackupImmutable:               false,
					IsWeeklyBackupImmutable:                false,
					IsAdhocBackupImmutable:                 false,
				},
			}, nil)
		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaCreateBackupVault(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
		assert.True(t, result.(*gcpgenserver.OperationV1beta).Done.Value)
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
	t.Run("ReturnsInternalServerErrorWhenBackupVaultCheckFails", func(t *testing.T) {
		req := &gcpgenserver.BackupVaultCreateV1beta{
			ResourceId: gcpgenserver.NewOptString("vault1"),
		}
		params := gcpgenserver.V1betaCreateBackupVaultParams{
			LocationId:    "us-central1",
			ProjectNumber: "1234567890",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, "vault1", "1234567890").
			Return(nil, fmt.Errorf("unexpected error"))

		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaCreateBackupVault(context.Background(), req, params)

		assert.Error(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaCreateBackupVaultInternalServerError{}, result)

		mockOrchestrator.AssertExpectations(t)
	})
	t.Run("WhenCreatesBackupVaultSuccessful", func(t *testing.T) {
		desc := "New backup vault"
		req := &gcpgenserver.BackupVaultCreateV1beta{
			ResourceId: gcpgenserver.NewOptString("new-vault"),
		}
		params := gcpgenserver.V1betaCreateBackupVaultParams{
			LocationId:    "us-east4",
			ProjectNumber: "1234567890",
		}
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, "new-vault", "1234567890").
			Return(nil, fmt.Errorf("backup vault not found"))

		mockOrchestrator.On("CreateBackupVault", mock.Anything, mock.Anything, params).
			Return(&mod.BackupVaultV1beta{
				Name:        "res",
				Description: &desc,
			}, "operation-id", nil)

		handler := Handler{Orchestrator: mockOrchestrator}

		ctx := context.Background()
		result, err := handler.V1betaCreateBackupVault(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check response type
		if op, ok := result.(*gcpgenserver.OperationV1beta); ok {
			assert.False(t, op.Done.Value)
			assert.Equal(t, "/v1beta/projects/1234567890/locations/us-east4/operations/operation-id", op.Name.Value)
		} else if badRequest, ok := result.(*gcpgenserver.V1betaCreateBackupVaultBadRequest); ok {
			assert.Equal(t, "Invalid request", badRequest.Message) // Adjust error message as needed
		} else {
			t.Fatalf("Unexpected response type: %T", result)
		}
		mockOrchestrator.AssertExpectations(t)
	})
}
