package api

import (
	"context"
	"fmt"
	"testing"
	"time"

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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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

// V1betaDescribeBackupVault unittests
func TestV1betaDescribeBackupVault(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	t.Run("WhenDescribeBackupVaultSuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	t.Run("WhenGetMultipleBackupVaultsSuccess", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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

		mockOrchestrator.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return(
			nil, nil)
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
		handler := Handler{Orchestrator: mockOrchestrator}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupVaults(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "bvid-1", result.(*gcpgenserver.V1betaGetMultipleBackupVaultsOK).BackupVaults[0].BackupVaultId.Value)
		assert.Equal(t, 1, len(result.(*gcpgenserver.V1betaGetMultipleBackupVaultsOK).BackupVaults))
	})
	t.Run("WhenGetMultipleBackupVaultsReturnErrorsVCP", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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

		mockOrchestrator.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return(
			nil, errors2.New("VCP error"))
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
		handler := Handler{Orchestrator: mockOrchestrator}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupVaults(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("WhenGetMultipleBackupVaultsFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_vault.NewMockClientService(t)
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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

func Test_CreateBackupVaultV1beta(t *testing.T) {
	t.Run("WhenNotEnabled", func(t *testing.T) {
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
	})
	t.Run("ReturnsBadRequestWhenRegionParsingFails", func(t *testing.T) {
		params := gcpgenserver.V1betaCreateBackupVaultParams{
			LocationId:    "local",
			ProjectNumber: "project-number",
		}
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
	})
	t.Run("ReturnsExistingBackupVaultWhenAlreadyExists", func(t *testing.T) {
		req := &gcpgenserver.BackupVaultCreateV1beta{
			ResourceId: gcpgenserver.NewOptString("existing-vault"),
		}
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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

		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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

		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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
		origBackupEnabled := backupEnabled
		defer func() { backupEnabled = origBackupEnabled }()
		backupEnabled = true
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

func TestV1betaUpdateBackupVaultNotEnabled(t *testing.T) {
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

func TestV1betaUpdateBackupVaultReturnsInvalidLocation(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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

func TestV1betaUpdateBackupVaultReturnsFoundWithBackupVaultFailsWithBadRequest(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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
		Return(nil, "", errors2.NewUserInputValidationErr("Update failed in SDE"))

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaUpdateBackupVault(context.Background(), req, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestConvertBackupRetentionPolicyToCvpModelForUpdate(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
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
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := _updateBackupVaultInSDE(context.Background(), req, params, "description")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}

func TestReturnsSuccessfulDeletion(tt *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	tt.Run("WhenSuccessful", func(t *testing.T) {
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			BackupVaultId:  "vault-id",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id"),
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockResp := &models.OperationV1beta{Name: "operation-id", Done: nillable.GetBoolPtr(true)}

		mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
			&backup_vault.V1betaDeleteBackupVaultAccepted{
				Payload: mockResp,
			}, nil, nil).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		logger := log.NewLogger()
		result, err := _deleteBackupVaultInSDE(context.Background(), params, logger)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenNotFoundError", func(t *testing.T) {
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			BackupVaultId:  "vault-id",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id"),
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaDeleteBackupVaultNotFound{
			Payload: &models.Error{
				Code:    400,
				Message: "Backup vault not found",
			},
		}

		mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
			nil, nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		logger := log.NewLogger()
		result, err := _deleteBackupVaultInSDE(context.Background(), params, logger)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenForbiddenError", func(t *testing.T) {
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			BackupVaultId:  "vault-id",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id"),
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaDeleteBackupVaultForbidden{
			Payload: &models.Error{
				Code:    403,
				Message: "forbidden",
			},
		}

		mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
			nil, nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		logger := log.NewLogger()
		result, err := _deleteBackupVaultInSDE(context.Background(), params, logger)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenBadRequestError", func(t *testing.T) {
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			BackupVaultId:  "vault-id",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id"),
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaDeleteBackupVaultBadRequest{
			Payload: &models.Error{
				Code:    400,
				Message: "Invalid request",
			},
		}

		mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
			nil, nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		logger := log.NewLogger()
		result, err := _deleteBackupVaultInSDE(context.Background(), params, logger)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenUnauthorizedError", func(t *testing.T) {
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			BackupVaultId:  "vault-id",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id"),
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaDeleteBackupVaultUnauthorized{
			Payload: &models.Error{
				Code:    401,
				Message: "Unauthorized access",
			},
		}

		mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
			nil, nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		logger := log.NewLogger()
		result, err := _deleteBackupVaultInSDE(context.Background(), params, logger)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenTooManyRequestsError", func(t *testing.T) {
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			BackupVaultId:  "vault-id",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id"),
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaDeleteBackupVaultTooManyRequests{
			Payload: &models.Error{
				Code:    429,
				Message: "Too many requests",
			},
		}

		mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
			nil, nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		logger := log.NewLogger()
		result, err := _deleteBackupVaultInSDE(context.Background(), params, logger)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenConflictError", func(t *testing.T) {
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			BackupVaultId:  "vault-id",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id"),
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaDeleteBackupVaultConflict{
			Payload: &models.Error{
				Code:    409,
				Message: "Conflict error",
			},
		}

		mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
			nil, nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		logger := log.NewLogger()
		result, err := _deleteBackupVaultInSDE(context.Background(), params, logger)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenUnprocessableEntityError", func(t *testing.T) {
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			BackupVaultId:  "vault-id",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id"),
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaDeleteBackupVaultUnprocessableEntity{
			Payload: &models.Error{
				Code:    422,
				Message: "Unprocessable entity",
			},
		}

		mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
			nil, nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		logger := log.NewLogger()
		result, err := _deleteBackupVaultInSDE(context.Background(), params, logger)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenDeleteBackupVaultDefault", func(t *testing.T) {
		params := gcpgenserver.V1betaDeleteBackupVaultParams{
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			BackupVaultId:  "vault-id",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id"),
		}

		mockClient := backup_vault.NewMockClientService(t)
		mockError := &backup_vault.V1betaDeleteBackupVaultDefault{
			Payload: &models.Error{
				Code:    500,
				Message: "Internal server error",
			},
		}

		mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
			nil, nil, mockError).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		logger := log.NewLogger()
		result, err := _deleteBackupVaultInSDE(context.Background(), params, logger)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}

func TestV1betaDeleteBackupVaultReturnsInvalidLocation(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	params := gcpgenserver.V1betaDeleteBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "", "", &gcpgenserver.Error{Code: 400, Message: "LocationID represents neither a region nor a zone"}
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestV1betaDeleteBackupVaultNotEnabled(t *testing.T) {
	params := gcpgenserver.V1betaDeleteBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "", "", &gcpgenserver.Error{Code: 400, Message: "LocationID represents neither a region nor a zone"}
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestV1betaDeleteBackupVaultReturnsNotFound(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	params := gcpgenserver.V1betaDeleteBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "valid-region", "valid-zone", nil
	}
	deleteBackupVaultInSDE = func(ctx context.Context, params gcpgenserver.V1betaDeleteBackupVaultParams, logger log.Logger) (r gcpgenserver.V1betaDeleteBackupVaultRes, _ error) {
		return nil, errors2.NewNotFoundErr("backup vault", &params.BackupVaultId)
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	bvName := "vault-id"
	ctx := context.Background()
	mockOrchestrator.On("GetBackupVaultByUUID", ctx, bvName, "1234567890").
		Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))
	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestV1betaDeleteBackupVaultReturnsError(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	params := gcpgenserver.V1betaDeleteBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "valid-region", "valid-zone", nil
	}

	deleteBackupVaultInSDE = func(ctx context.Context, params gcpgenserver.V1betaDeleteBackupVaultParams, logger log.Logger) (r gcpgenserver.V1betaDeleteBackupVaultRes, _ error) {
		return nil, errors2.NewBadRequestErr("Update failed in SDE")
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	bvName := "vault-id"
	ctx := context.Background()
	mockOrchestrator.On("GetBackupVaultByUUID", ctx, bvName, "1234567890").
		Return(nil, errors2.NewConflictErr("Backup vault already exists"))
	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestV1betaDeleteBackupVaultReturnsNotFoundSDESuccessful(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	params := gcpgenserver.V1betaDeleteBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
	}

	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "valid-region", "valid-zone", nil
	}

	deleteBackupVaultInSDE = func(ctx context.Context, params gcpgenserver.V1betaDeleteBackupVaultParams, logger log.Logger) (r gcpgenserver.V1betaDeleteBackupVaultRes, _ error) {
		return &gcpgenserver.OperationV1beta{}, nil
	}

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	bvName := "vault-id"
	ctx := context.Background()
	mockOrchestrator.On("GetBackupVaultByUUID", ctx, bvName, "1234567890").
		Return(nil, errors2.NewNotFoundErr("backup vault", &bvName))
	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestV1betaDeleteBackupVaultReturnsFoundWithbackupVaultSuccessful(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	params := gcpgenserver.V1betaDeleteBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
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

	mockOrchestrator.On("DeleteBackupVault", ctx, mock.Anything).
		Return(&mod.BackupVaultV1beta{}, "operation-id", nil)

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	assert.Equal(t, "/v1beta/projects/1234567890/locations/valid-location/operations/operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
}

func TestV1betaDeleteBackupVaultReturnsFoundWithbackupVaultSuccessfulWithNoOperation(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	params := gcpgenserver.V1betaDeleteBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
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

	mockOrchestrator.On("DeleteBackupVault", ctx, mock.Anything).
		Return(&mod.BackupVaultV1beta{}, "", nil)

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	assert.Equal(t, "", result.(*gcpgenserver.OperationV1beta).Name.Value)
}

func TestV1betaDeleteBackupVaultReturnsFoundWithbackupVaultJsonFails(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	params := gcpgenserver.V1betaDeleteBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
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

	mockOrchestrator.On("DeleteBackupVault", ctx, mock.Anything).
		Return(&mod.BackupVaultV1beta{}, "operation-id", nil)

	jsonMarshal = func(v interface{}) ([]byte, error) {
		return nil, fmt.Errorf("JSON marshal error")
	}

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestV1betaDeleteBackupVaultReturnsFoundWithbackupVaultFails(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	params := gcpgenserver.V1betaDeleteBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
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

	mockOrchestrator.On("DeleteBackupVault", ctx, mock.Anything).
		Return(nil, "", errors2.NewBadRequestErr("Update failed in SDE"))

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestV1betaDeleteBackupVaultReturnsFoundWithbackupVaultBadRequestFails(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	params := gcpgenserver.V1betaDeleteBackupVaultParams{
		LocationId:    "valid-location",
		ProjectNumber: "1234567890",
		BackupVaultId: "vault-id",
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

	mockOrchestrator.On("DeleteBackupVault", ctx, mock.Anything).
		Return(nil, "", errors2.NewUserInputValidationErr("Update failed in SDE"))

	handler := Handler{Orchestrator: mockOrchestrator}

	result, err := handler.V1betaDeleteBackupVault(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestReturnsBackupVaultV1betaWhenAllFieldsAreSet(t *testing.T) {
	beta := &mod.BackupVaultV1beta{
		BackupVaultID:          "vault-id",
		LifeCycleState:         "ACTIVE",
		LifeCycleStateDetails:  "All good",
		CreatedAt:              time.Now(),
		Description:            nillable.GetStringPtr("Test description"),
		Name:                   "resource-id",
		SourceBackupVault:      nillable.GetStringPtr("source-vault"),
		DestinationBackupVault: nillable.GetStringPtr("destination-vault"),
		SourceRegion:           nillable.GetStringPtr("us-central1"),
		BackupRegion:           nillable.GetStringPtr("us-east1"),
		BackupVaultType:        nillable.GetStringPtr("STANDARD"),
		BackupRetentionPolicy: mod.BackupRetentionPolicyparams{
			BackupMinimumEnforcedRetentionDuration: nillable.GetInt64Ptr(30),
			IsDailyBackupImmutable:                 true,
			IsAdhocBackupImmutable:                 false,
			IsMonthlyBackupImmutable:               true,
			IsWeeklyBackupImmutable:                false,
		},
	}

	result := convertCoreModelsToBackupVaultV1beta(beta)

	assert.NotNil(t, result)
	assert.Equal(t, "vault-id", result.BackupVaultId.Value)
	assert.Equal(t, "ACTIVE", string(result.State.Value))
	assert.Equal(t, "All good", result.StateDetails.Value)
	assert.Equal(t, "Test description", result.Description.Value)
	assert.Equal(t, "resource-id", result.ResourceId)
	assert.Equal(t, "source-vault", result.SourceBackupVault.Value)
	assert.Equal(t, "destination-vault", result.DestinationBackupVault.Value)
	assert.Equal(t, "us-central1", result.SourceRegion.Value)
	assert.Equal(t, "us-east1", result.BackupRegion.Value)
	assert.Equal(t, "STANDARD", string(result.BackupVaultType.Value))
	assert.Equal(t, 30, result.BackupRetentionPolicy.Value.BackupMinimumEnforcedRetentionDays.Value)
	assert.True(t, result.BackupRetentionPolicy.Value.DailyBackupImmutable.Value)
	assert.False(t, result.BackupRetentionPolicy.Value.ManualBackupImmutable.Value)
	assert.True(t, result.BackupRetentionPolicy.Value.MonthlyBackupImmutable.Value)
	assert.False(t, result.BackupRetentionPolicy.Value.WeeklyBackupImmutable.Value)
}

func TestReturnsBackupVaultV1betaWithDefaultsWhenOptionalFieldsAreNil(t *testing.T) {
	beta := &mod.BackupVaultV1beta{
		BackupVaultID:         "vault-id",
		LifeCycleState:        "ACTIVE",
		LifeCycleStateDetails: "All good",
		CreatedAt:             time.Now(),
		Name:                  "resource-id",
		BackupRetentionPolicy: mod.BackupRetentionPolicyparams{},
	}

	result := convertCoreModelsToBackupVaultV1beta(beta)

	assert.NotNil(t, result)
	assert.Equal(t, "vault-id", result.BackupVaultId.Value)
	assert.Equal(t, "ACTIVE", string(result.State.Value))
	assert.Equal(t, "All good", result.StateDetails.Value)
	assert.Equal(t, "resource-id", result.ResourceId)
	assert.NotNil(t, result.Description)
	assert.NotNil(t, result.SourceBackupVault)
	assert.NotNil(t, result.DestinationBackupVault)
	assert.NotNil(t, result.SourceRegion)
	assert.NotNil(t, result.BackupRegion)
	assert.NotNil(t, result.BackupVaultType)
	assert.NotNil(t, result.BackupRetentionPolicy.Value.BackupMinimumEnforcedRetentionDays)
	assert.NotNil(t, result.BackupRetentionPolicy.Value.DailyBackupImmutable)
	assert.NotNil(t, result.BackupRetentionPolicy.Value.ManualBackupImmutable)
	assert.NotNil(t, result.BackupRetentionPolicy.Value.MonthlyBackupImmutable)
	assert.NotNil(t, result.BackupRetentionPolicy.Value.WeeklyBackupImmutable)
}
