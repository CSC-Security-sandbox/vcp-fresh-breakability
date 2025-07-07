package api

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	mod "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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
					ResourceID:    nillable.GetStringPtr("bv-1"),
					BackupRegion:  nillable.GetStringPtr("backup-region"),
					BackupVaultID: "backup-id",
				},
			},
		},
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	res := []*mod.BackupVaultV1beta{
		{
			Name:                  "bv-1",
			BackupRegion:          nillable.GetStringPtr("backup-region"),
			BackupVaultID:         "backup-id",
			LifeCycleState:        "CREATING",
			LifeCycleStateDetails: "Creation in progress",
		},
	}
	mockOrchestrator.On("ListBackupVaults", mock.Anything, "12345").Return(res, nil)
	handler := Handler{Orchestrator: mockOrchestrator}

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
	// Call the method under test
	result, err := handler.V1betaListBackupVaults(context.Background(), params)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.(*gcpgenserver.V1betaListBackupVaultsOK).BackupVaults))
	assert.Equal(t, "backup-id", result.(*gcpgenserver.V1betaListBackupVaultsOK).BackupVaults[0].BackupVaultId.Value)
}

func TestV1betaListBackupVaultsOrchError(t *testing.T) {
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
					ResourceID:    nillable.GetStringPtr("bv-1"),
					BackupRegion:  nillable.GetStringPtr("backup-region"),
					BackupVaultID: "backup-id",
				},
			},
		},
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	mockOrchestrator.On("ListBackupVaults", mock.Anything, "12345").Return(nil, errors2.New("orchestrator error"))
	handler := Handler{Orchestrator: mockOrchestrator}

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
	// Call the method under test
	result, err := handler.V1betaListBackupVaults(context.Background(), params)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.IsType(t, &gcpgenserver.V1betaListBackupVaultsInternalServerError{}, result)
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
		minEnforcedRetentionDuration := int64(30)

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, "existing-vault", "1234567890").
			Return(&mod.BackupVaultV1beta{
				Name:        "existing-vault",
				Description: &desc,
				BackupRetentionPolicy: mod.BackupRetentionPolicyparams{
					BackupMinimumEnforcedRetentionDuration: &minEnforcedRetentionDuration,
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
	t.Run("WhenCreatesBackupVaultSdeBadRequestError", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
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

		bvName := "new-vault"
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, bvName, "1234567890").
			Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))

		mockClient.EXPECT().
			V1betaCreateBackupVault(mock.Anything).
			Return(nil, &backup_vault.V1betaCreateBackupVaultBadRequest{Payload: &models.Error{
				Code:    400,
				Message: "SDE error: Invalid request",
			},
			})
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{Orchestrator: mockOrchestrator}

		ctx := context.Background()
		result, err := handler.V1betaCreateBackupVault(ctx, req, params)

		// Assert
		assert.Nil(t, err)
		assert.NotNil(t, result)
		mockOrchestrator.AssertExpectations(t)
	})
	t.Run("WhenCreatesBackupVaultSdeUnprocessableError", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
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

		bvName := "new-vault"
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, bvName, "1234567890").
			Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))

		mockClient.EXPECT().
			V1betaCreateBackupVault(mock.Anything).
			Return(nil, &backup_vault.V1betaCreateBackupVaultUnprocessableEntity{Payload: &models.Error{
				Code:    422,
				Message: "SDE error: Unprocessable Entity",
			},
			})
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{Orchestrator: mockOrchestrator}

		ctx := context.Background()
		result, err := handler.V1betaCreateBackupVault(ctx, req, params)

		// Assert
		assert.Nil(t, err)
		assert.NotNil(t, result)
		mockOrchestrator.AssertExpectations(t)
	})
	t.Run("WhenCreatesBackupVaultSdeConflictError", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
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

		bvName := "new-vault"
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, bvName, "1234567890").
			Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))

		mockClient.EXPECT().
			V1betaCreateBackupVault(mock.Anything).
			Return(nil, &backup_vault.V1betaCreateBackupVaultConflict{Payload: &models.Error{
				Code:    409,
				Message: "SDE error: Conflict",
			},
			})
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{Orchestrator: mockOrchestrator}

		ctx := context.Background()
		result, err := handler.V1betaCreateBackupVault(ctx, req, params)

		// Assert
		assert.Nil(t, err)
		assert.NotNil(t, result)
		mockOrchestrator.AssertExpectations(t)
	})
	t.Run("WhenCreatesBackupVaultSdeUnAuthorizedError", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
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

		bvName := "new-vault"
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, bvName, "1234567890").
			Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))

		mockClient.EXPECT().
			V1betaCreateBackupVault(mock.Anything).
			Return(nil, &backup_vault.V1betaCreateBackupVaultUnauthorized{Payload: &models.Error{
				Code:    401,
				Message: "SDE error: UnAuthorized",
			},
			})
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{Orchestrator: mockOrchestrator}

		ctx := context.Background()
		result, err := handler.V1betaCreateBackupVault(ctx, req, params)

		// Assert
		assert.Nil(t, err)
		assert.NotNil(t, result)
		mockOrchestrator.AssertExpectations(t)
	})
	t.Run("WhenCreatesBackupVaultSdeForbiddenError", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
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

		bvName := "new-vault"
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, bvName, "1234567890").
			Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))
		mockClient.EXPECT().
			V1betaCreateBackupVault(mock.Anything).
			Return(nil, &backup_vault.V1betaCreateBackupVaultForbidden{Payload: &models.Error{
				Code:    403,
				Message: "SDE error: Forbidden",
			},
			})
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{Orchestrator: mockOrchestrator}

		ctx := context.Background()
		result, err := handler.V1betaCreateBackupVault(ctx, req, params)

		// Assert
		assert.Nil(t, err)
		assert.NotNil(t, result)
		mockOrchestrator.AssertExpectations(t)
	})
	t.Run("WhenCreatesBackupVaultSdeTooManyRequestError", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
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

		bvName := "new-vault"
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, bvName, "1234567890").
			Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))

		mockClient.EXPECT().
			V1betaCreateBackupVault(mock.Anything).
			Return(nil, &backup_vault.V1betaCreateBackupVaultTooManyRequests{Payload: &models.Error{
				Code:    429,
				Message: "SDE error: TooManyRequest",
			},
			})
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{Orchestrator: mockOrchestrator}

		ctx := context.Background()
		result, err := handler.V1betaCreateBackupVault(ctx, req, params)

		// Assert
		assert.Nil(t, err)
		assert.NotNil(t, result)
		mockOrchestrator.AssertExpectations(t)
	})
	t.Run("WhenCreatesBackupVaultSdeDefaultError", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
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

		bvName := "new-vault"
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, bvName, "1234567890").
			Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))

		mockClient.EXPECT().
			V1betaCreateBackupVault(mock.Anything).
			Return(nil, &backup_vault.V1betaCreateBackupVaultDefault{Payload: &models.Error{
				Code:    500,
				Message: "SDE error: Default",
			},
			})
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{Orchestrator: mockOrchestrator}

		ctx := context.Background()
		result, err := handler.V1betaCreateBackupVault(ctx, req, params)

		// Assert
		assert.Nil(t, err)
		assert.NotNil(t, result)
		mockOrchestrator.AssertExpectations(t)
	})
	t.Run("WhenCreatesBackupVaultConversionError", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
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

		// Define mock response
		mockResponse := &backup_vault.V1betaCreateBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		bvName := "new-vault"
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, bvName, "1234567890").
			Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))

		mockClient.EXPECT().
			V1betaCreateBackupVault(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
			utilsConvertJsonToModel = utils.ConvertJsonToModel
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		utilsConvertJsonToModel = func(data []byte, model interface{}) error {
			return fmt.Errorf("JSON conversion error")
		}

		handler := Handler{Orchestrator: mockOrchestrator}

		ctx := context.Background()
		result, err := handler.V1betaCreateBackupVault(ctx, req, params)

		// Assert
		assert.Error(t, err)
		assert.NotNil(t, result)
		mockOrchestrator.AssertExpectations(t)
	})
	t.Run("WhenCreatesBackupVaultSuccess", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
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

		// Define mock response
		mockResponse := &backup_vault.V1betaCreateBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		bvName := "new-vault"
		mockOrchestrator.On("GetBackupVaultByNameAndOwnerID", mock.Anything, bvName, "1234567890").
			Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))

		mockClient.EXPECT().
			V1betaCreateBackupVault(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
			utilsConvertJsonToModel = utils.ConvertJsonToModel
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		utilsConvertJsonToModel = func(data []byte, model interface{}) error {
			return nil
		}

		handler := Handler{Orchestrator: mockOrchestrator}

		ctx := context.Background()
		result, err := handler.V1betaCreateBackupVault(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)
		mockOrchestrator.AssertExpectations(t)
	})
}

func TestConvertBackupRetentionPolicyToCvpModelForCreate(t *testing.T) {
	t.Run("ReturnsNilWhenPolicyIsNotSet", func(t *testing.T) {
		brPolicy := gcpgenserver.OptBackupRetentionPolicyV1beta{}
		result := convertBackupRetentionPolicyToCvpModelForCreate(brPolicy)
		assert.Nil(t, result)
	})

	t.Run("ReturnsModelWhenAllFieldsAreSet", func(t *testing.T) {
		brPolicy := gcpgenserver.OptBackupRetentionPolicyV1beta{
			Value: gcpgenserver.BackupRetentionPolicyV1beta{
				BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(30),
				DailyBackupImmutable:               gcpgenserver.NewOptBool(true),
				ManualBackupImmutable:              gcpgenserver.NewOptBool(false),
				MonthlyBackupImmutable:             gcpgenserver.NewOptBool(true),
				WeeklyBackupImmutable:              gcpgenserver.NewOptBool(false),
			},
			Set: true,
		}
		result := convertBackupRetentionPolicyToCvpModelForCreate(brPolicy)
		assert.NotNil(t, result)
		assert.Equal(t, int64(30), *result.BackupMinimumEnforcedRetentionDays)
		assert.True(t, result.DailyBackupImmutable)
		assert.False(t, result.ManualBackupImmutable)
		assert.True(t, result.MonthlyBackupImmutable)
		assert.False(t, result.WeeklyBackupImmutable)
	})

	t.Run("ReturnsModelWhenSomeFieldsAreUnset", func(t *testing.T) {
		brPolicy := gcpgenserver.OptBackupRetentionPolicyV1beta{
			Value: gcpgenserver.BackupRetentionPolicyV1beta{
				DailyBackupImmutable: gcpgenserver.NewOptBool(true),
			},
			Set: true,
		}
		result := convertBackupRetentionPolicyToCvpModelForCreate(brPolicy)
		assert.NotNil(t, result)
		assert.Nil(t, result.BackupMinimumEnforcedRetentionDays)
		assert.True(t, result.DailyBackupImmutable)
		assert.False(t, result.ManualBackupImmutable)
		assert.False(t, result.MonthlyBackupImmutable)
		assert.False(t, result.WeeklyBackupImmutable)
	})
}

func TestV1betaUpdateBackupVaultReturnsInvalidLocation(t *testing.T) {
	params := gcpgenserver.V1betaUpdateBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}
	req := &gcpgenserver.BackupVaultUpdateV1beta{}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "", "", &gcpgenserver.Error{Code: 400, Message: "LocationID represents neither a region nor a zone"}
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestV1betaUpdateBackupVaultReturnsNotFound(t *testing.T) {
	params := gcpgenserver.V1betaUpdateBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}
	req := &gcpgenserver.BackupVaultUpdateV1beta{}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "valid-region", "valid-zone", nil
	}
	updateBackupVaultInSDE = func(ctx context.Context, req *gcpgenserver.BackupVaultUpdateV1beta, params gcpgenserver.V1betaUpdateBackupVaultParams, description string) (r gcpgenserver.V1betaUpdateBackupVaultRes, _ error) {
		return nil, errors2.NewBadRequestErr("Update failed in SDE")
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	bvName := "vault-id"
	ctx := context.Background()
	mockOrchestrator.On("GetBackupVaultByUUID", ctx, bvName, "1234567890").
		Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))
	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestV1betaUpdateBackupVaultReturnsError(t *testing.T) {
	params := gcpgenserver.V1betaUpdateBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}
	req := &gcpgenserver.BackupVaultUpdateV1beta{}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "valid-region", "valid-zone", nil
	}
	updateBackupVaultInSDE = func(ctx context.Context, req *gcpgenserver.BackupVaultUpdateV1beta, params gcpgenserver.V1betaUpdateBackupVaultParams, description string) (r gcpgenserver.V1betaUpdateBackupVaultRes, _ error) {
		return nil, errors2.NewBadRequestErr("Update failed in SDE")
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	bvName := "vault-id"
	ctx := context.Background()
	mockOrchestrator.On("GetBackupVaultByUUID", ctx, bvName, "1234567890").
		Return(nil, errors2.NewConflictErr("Backup vault already exists"))
	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestV1betaUpdateBackupVaultReturnsNotFoundSDESuccessful(t *testing.T) {
	params := gcpgenserver.V1betaUpdateBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}
	req := &gcpgenserver.BackupVaultUpdateV1beta{}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "valid-region", "valid-zone", nil
	}
	updateBackupVaultInSDE = func(ctx context.Context, req *gcpgenserver.BackupVaultUpdateV1beta, params gcpgenserver.V1betaUpdateBackupVaultParams, description string) (r gcpgenserver.V1betaUpdateBackupVaultRes, _ error) {
		return &gcpgenserver.OperationV1beta{}, nil
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	bvName := "vault-id"
	ctx := context.Background()
	mockOrchestrator.On("GetBackupVaultByUUID", ctx, bvName, "1234567890").
		Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))
	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}
func TestV1betaUpdateBackupVaultReturnsFoundWithbackupVaultSuccessful(t *testing.T) {
	params := gcpgenserver.V1betaUpdateBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}
	req := &gcpgenserver.BackupVaultUpdateV1beta{
		Description: gcpgenserver.NewOptString("desc"),
		BackupRetentionPolicy: gcpgenserver.NewOptBackupRetentionPolicyUpdateV1beta(
			gcpgenserver.BackupRetentionPolicyUpdateV1beta{
				BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(30),
				DailyBackupImmutable:               gcpgenserver.NewOptBool(true),
				MonthlyBackupImmutable:             gcpgenserver.NewOptBool(false),
				ManualBackupImmutable:              gcpgenserver.NewOptBool(true),
				WeeklyBackupImmutable:              gcpgenserver.NewOptBool(false),
			},
		),
	}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "valid-region", "valid-zone", nil
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	bvName := "vault-id"
	ctx := context.Background()
	resID := "vault-id"
	bvResp := mod.BackupVaultV1beta{
		Name:        resID,
		AccountName: "1234567890",
	}
	mockOrchestrator.On("GetBackupVaultByUUID", ctx, bvName, "1234567890").
		Return(&bvResp, nil)

	mockOrchestrator.On("UpdateBackupVault", ctx, mock.Anything).
		Return(&mod.BackupVaultV1beta{}, "operation-id", nil)

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	assert.Equal(t, "/v1beta/projects/1234567890/locations/valid-location/operations/operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
}

func TestV1betaUpdateBackupVaultReturnsFoundWithbackupVaultSuccessfulWithNoOperation(t *testing.T) {
	params := gcpgenserver.V1betaUpdateBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}
	req := &gcpgenserver.BackupVaultUpdateV1beta{
		Description: gcpgenserver.NewOptString("desc"),
		BackupRetentionPolicy: gcpgenserver.NewOptBackupRetentionPolicyUpdateV1beta(
			gcpgenserver.BackupRetentionPolicyUpdateV1beta{
				BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(30),
				DailyBackupImmutable:               gcpgenserver.NewOptBool(true),
				MonthlyBackupImmutable:             gcpgenserver.NewOptBool(false),
				ManualBackupImmutable:              gcpgenserver.NewOptBool(true),
				WeeklyBackupImmutable:              gcpgenserver.NewOptBool(false),
			},
		),
	}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "valid-region", "valid-zone", nil
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	bvName := "vault-id"
	ctx := context.Background()
	resID := "vault-id"
	bvResp := mod.BackupVaultV1beta{
		Name:        resID,
		AccountName: "1234567890",
	}
	mockOrchestrator.On("GetBackupVaultByUUID", ctx, bvName, "1234567890").
		Return(&bvResp, nil)

	mockOrchestrator.On("UpdateBackupVault", ctx, mock.Anything).
		Return(&mod.BackupVaultV1beta{}, "", nil)

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	assert.Equal(t, "", result.(*gcpgenserver.OperationV1beta).Name.Value)
}

func TestV1betaUpdateBackupVaultReturnsFoundWithbackupVaultJsonFails(t *testing.T) {
	params := gcpgenserver.V1betaUpdateBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}
	req := &gcpgenserver.BackupVaultUpdateV1beta{
		Description: gcpgenserver.NewOptString("desc"),
		BackupRetentionPolicy: gcpgenserver.NewOptBackupRetentionPolicyUpdateV1beta(
			gcpgenserver.BackupRetentionPolicyUpdateV1beta{
				BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(30),
				DailyBackupImmutable:               gcpgenserver.NewOptBool(true),
				MonthlyBackupImmutable:             gcpgenserver.NewOptBool(false),
				ManualBackupImmutable:              gcpgenserver.NewOptBool(true),
				WeeklyBackupImmutable:              gcpgenserver.NewOptBool(false),
			},
		),
	}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "valid-region", "valid-zone", nil
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	bvName := "vault-id"
	ctx := context.Background()
	resID := "vault-id"
	bvResp := mod.BackupVaultV1beta{
		Name:        resID,
		AccountName: "1234567890",
	}
	mockOrchestrator.On("GetBackupVaultByUUID", ctx, bvName, "1234567890").
		Return(&bvResp, nil)

	mockOrchestrator.On("UpdateBackupVault", ctx, mock.Anything).
		Return(&mod.BackupVaultV1beta{}, "operation-id", nil)

	jsonMarshal = func(v interface{}) ([]byte, error) {
		return nil, fmt.Errorf("JSON marshal error")
	}

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestV1betaUpdateBackupVaultReturnsFoundWithbackupVaultFails(t *testing.T) {
	params := gcpgenserver.V1betaUpdateBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}
	req := &gcpgenserver.BackupVaultUpdateV1beta{
		Description: gcpgenserver.NewOptString("desc"),
		BackupRetentionPolicy: gcpgenserver.NewOptBackupRetentionPolicyUpdateV1beta(
			gcpgenserver.BackupRetentionPolicyUpdateV1beta{
				DailyBackupImmutable:   gcpgenserver.NewOptBool(true),
				MonthlyBackupImmutable: gcpgenserver.NewOptBool(false),
			},
		),
	}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "valid-region", "valid-zone", nil
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	bvName := "vault-id"
	ctx := context.Background()
	resID := "vault-id"
	bvResp := mod.BackupVaultV1beta{
		Name:        resID,
		AccountName: "1234567890",
	}
	mockOrchestrator.On("GetBackupVaultByUUID", ctx, bvName, "1234567890").
		Return(&bvResp, nil)

	mockOrchestrator.On("UpdateBackupVault", ctx, mock.Anything).
		Return(nil, "", errors2.NewBadRequestErr("Update failed in SDE"))

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestConvertBackupRetentionPolicyToCvpModelForUpdate(t *testing.T) {
	t.Run("ReturnsNilWhenPolicyIsNotSet", func(t *testing.T) {
		brPolicy := gcpgenserver.OptBackupRetentionPolicyUpdateV1beta{}
		result := convertBackupRetentionPolicyToCvpModelForUpdate(brPolicy)
		assert.Nil(t, result)
	})

	t.Run("ReturnsModelWhenAllFieldsAreSet", func(t *testing.T) {
		brPolicy := gcpgenserver.OptBackupRetentionPolicyUpdateV1beta{
			Value: gcpgenserver.BackupRetentionPolicyUpdateV1beta{
				BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(30),
				DailyBackupImmutable:               gcpgenserver.NewOptBool(true),
				ManualBackupImmutable:              gcpgenserver.NewOptBool(false),
				MonthlyBackupImmutable:             gcpgenserver.NewOptBool(true),
				WeeklyBackupImmutable:              gcpgenserver.NewOptBool(false),
			},
			Set: true,
		}
		result := convertBackupRetentionPolicyToCvpModelForUpdate(brPolicy)
		assert.NotNil(t, result)
		assert.Equal(t, int64(30), *result.BackupMinimumEnforcedRetentionDays)
		assert.True(t, *result.DailyBackupImmutable)
		assert.False(t, *result.ManualBackupImmutable)
		assert.True(t, *result.MonthlyBackupImmutable)
		assert.False(t, *result.WeeklyBackupImmutable)
	})
}

func Test_updateBackupVaultInSDE(tt *testing.T) {
	tt.Run("WhenGetSignedTokenError", func(t *testing.T) {
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:    "valid-location",
			ProjectNumber: "1234567890",
			BackupVaultId: "vault-id",
		}
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		GetSignedToken = func(projectNumber string) (string, error) {
			return "", errors2.New("Failed to get signed token")
		}
		defer func() {
			GetSignedToken = auth.GetSignedJwtToken
		}()

		result, err := _updateBackupVaultInSDE(context.Background(), req, params, "description")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenSuccessful", func(t *testing.T) {
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:    "valid-location",
			ProjectNumber: "1234567890",
			BackupVaultId: "vault-id",
		}
		req := &gcpgenserver.BackupVaultUpdateV1beta{
			BackupRetentionPolicy: gcpgenserver.NewOptBackupRetentionPolicyUpdateV1beta(
				gcpgenserver.BackupRetentionPolicyUpdateV1beta{
					BackupMinimumEnforcedRetentionDays: gcpgenserver.NewOptInt(30),
					DailyBackupImmutable:               gcpgenserver.NewOptBool(true),
					ManualBackupImmutable:              gcpgenserver.NewOptBool(false),
					MonthlyBackupImmutable:             gcpgenserver.NewOptBool(true),
				}),
		}
		GetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockResp := &models.OperationV1beta{Name: "operation-id", Done: nillable.GetBoolPtr(true)}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
			&backup_vault.V1betaUpdateBackupVaultAccepted{
				Payload: mockResp,
			}, nil).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
			GetSignedToken = auth.GetSignedJwtToken
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := _updateBackupVaultInSDE(context.Background(), req, params, "description")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenBadRequest", func(t *testing.T) {
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:    "valid-location",
			ProjectNumber: "1234567890",
			BackupVaultId: "vault-id",
		}
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		GetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaUpdateBackupVaultBadRequest{
			Payload: &models.Error{
				Code:    400,
				Message: "SDE error: Invalid request",
			},
		}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
			nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
			GetSignedToken = auth.GetSignedJwtToken
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := _updateBackupVaultInSDE(context.Background(), req, params, "description")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenDefaultError", func(t *testing.T) {
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:    "valid-location",
			ProjectNumber: "1234567890",
			BackupVaultId: "vault-id",
		}
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		GetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaUpdateBackupVaultInternalServerError{
			Payload: &models.Error{
				Code:    500,
				Message: "SDE error: Invalid request",
			},
		}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
			nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
			GetSignedToken = auth.GetSignedJwtToken
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := _updateBackupVaultInSDE(context.Background(), req, params, "description")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenUnprocessableEntity", func(t *testing.T) {
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:    "valid-location",
			ProjectNumber: "1234567890",
			BackupVaultId: "vault-id",
		}
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		GetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaUpdateBackupVaultUnprocessableEntity{
			Payload: &models.Error{
				Code:    503,
				Message: "SDE error: Invalid request",
			},
		}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
			nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
			GetSignedToken = auth.GetSignedJwtToken
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := _updateBackupVaultInSDE(context.Background(), req, params, "description")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenConflict", func(t *testing.T) {
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:    "valid-location",
			ProjectNumber: "1234567890",
			BackupVaultId: "vault-id",
		}
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		GetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaUpdateBackupVaultConflict{
			Payload: &models.Error{
				Code:    409,
				Message: "SDE error: Invalid request",
			},
		}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
			nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
			GetSignedToken = auth.GetSignedJwtToken
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := _updateBackupVaultInSDE(context.Background(), req, params, "description")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenUpdateBackupVaultUnauthorized", func(t *testing.T) {
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:    "valid-location",
			ProjectNumber: "1234567890",
			BackupVaultId: "vault-id",
		}
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		GetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaUpdateBackupVaultUnauthorized{
			Payload: &models.Error{
				Code:    501,
				Message: "SDE error: Invalid request",
			},
		}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
			nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
			GetSignedToken = auth.GetSignedJwtToken
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := _updateBackupVaultInSDE(context.Background(), req, params, "description")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenUpdateBackupVaultForbidden", func(t *testing.T) {
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:    "valid-location",
			ProjectNumber: "1234567890",
			BackupVaultId: "vault-id",
		}
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		GetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaUpdateBackupVaultForbidden{
			Payload: &models.Error{
				Code:    403,
				Message: "SDE error: Invalid request",
			},
		}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
			nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
			GetSignedToken = auth.GetSignedJwtToken
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := _updateBackupVaultInSDE(context.Background(), req, params, "description")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenUpdateBackupVaultTooManyRequests", func(t *testing.T) {
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:    "valid-location",
			ProjectNumber: "1234567890",
			BackupVaultId: "vault-id",
		}
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		GetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaUpdateBackupVaultTooManyRequests{
			Payload: &models.Error{
				Code:    429,
				Message: "SDE error: Invalid request",
			},
		}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
			nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
			GetSignedToken = auth.GetSignedJwtToken
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := _updateBackupVaultInSDE(context.Background(), req, params, "description")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenUpdateBackupVaultDefault", func(t *testing.T) {
		params := gcpgenserver.V1betaUpdateBackupVaultParams{
			LocationId:    "valid-location",
			ProjectNumber: "1234567890",
			BackupVaultId: "vault-id",
		}
		req := &gcpgenserver.BackupVaultUpdateV1beta{}
		GetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaUpdateBackupVaultDefault{
			Payload: &models.Error{
				Code:    500,
				Message: "SDE error: Invalid request",
			},
		}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
			nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
			GetSignedToken = auth.GetSignedJwtToken
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := _updateBackupVaultInSDE(context.Background(), req, params, "description")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}
