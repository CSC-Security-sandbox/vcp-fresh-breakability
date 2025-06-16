package api

import (
	"context"
	"fmt"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backups"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// V1betaGetMultipleBackups unittests
func TestV1GetMultipleBackups(t *testing.T) {
	t.Run("WhenGetMultipleBackupsSuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backups.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupVaultId:  "test-backup-vault-id",
		}
		req := &gcpgenserver.BackupUuidListV1beta{
			BackupUuids: []string{"backup-id-1"},
		}

		backup := []*models.BackupV1beta{}
		// Define mock response
		createdAt := strfmt.DateTime(time.Now().UTC())
		description := "description"
		resourceId := "test-resource-id"
		volRegion := "us-east4"
		volUsageBytes := int64(123456)
		backupVaultId := "test-backup-vault-id"
		sourceSnapshotId := "snap-1"
		BackupChainBytes := int64(123)
		SatisfiesPzs := true
		SatisfiesPzi := true

		bv := models.BackupV1beta{
			BackupID:                 "backup-id-1",
			BackupRegion:             &volRegion,
			BackupType:               "adhoc",
			BackupVaultID:            &backupVaultId,
			Created:                  createdAt,
			Description:              &description,
			EnforcedRetentionEndTime: nil,
			ResourceID:               resourceId,
			SourceSnapshot:           &sourceSnapshotId,
			SourceVolume:             "available",
			State:                    "Available for use",
			VolumeID:                 "12345",
			VolumeRegion:             &volRegion,
			VolumeUsageBytes:         &volUsageBytes,
			BackupChainBytes:         &BackupChainBytes,
			SatisfiesPzs:             &SatisfiesPzs,
			SatisfiesPzi:             &SatisfiesPzi,
		}

		backup1 := append(backup, &bv)
		mockResponse := &backups.V1betaGetMultipleBackupsOK{
			Payload: &backups.V1betaGetMultipleBackupsOKBody{Backups: backup1},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackups(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{Backups: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackups(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, "backup-id-1", result.(*gcpgenserver.V1betaGetMultipleBackupsOK).Backups[0].BackupId.Value)
	})
	t.Run("WhenGetMultipleBackupsFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backups.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupUuidListV1beta{}

		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backups.V1betaGetMultipleBackupsBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackups(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Backups: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackups(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupsBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupsBadRequest).Message)
	})
	t.Run("WhenGetMultipleBackupsFailsWithUnAuthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backups.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupUuidListV1beta{}

		// Define mock error
		errorCode := float64(401)
		errorMessage := "UnAuthorized"
		mockError := &backups.V1betaGetMultipleBackupsUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackups(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Backups: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackups(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupsUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupsUnauthorized).Message)
	})
	t.Run("WhenGetMultipleBackupsFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backups.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupUuidListV1beta{}

		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden"
		mockError := &backups.V1betaGetMultipleBackupsForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackups(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Backups: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackups(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupsForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupsForbidden).Message)
	})
}

// Unit tests for V1betaDeleteBackupUnderBackupVault
func TestV1betaDeleteBackupUnderBackupVault(t *testing.T) {
	t.Run("WhenDeleteBackupUnimplemented", func(t *testing.T) {
		// Mock input parameters
		params := gcpgenserver.V1betaDeleteBackupUnderBackupVaultParams{}

		// Mock handler
		handler := Handler{}

		// Call the method under test
		result, err := handler.V1betaDeleteBackupUnderBackupVault(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(500), result.(*gcpgenserver.V1betaDeleteBackupUnderBackupVaultInternalServerError).Code)
	})
}

// Unit tests for V1betaUpdateBackupUnderBackupVault
func TestV1betaUpdateBackupUnderBackupVault(t *testing.T) {
	t.Run("WhenUpdateBackupUnimplemented", func(t *testing.T) {
		// Mock input parameters
		req := &models.BackupUpdateV1beta{}
		params := gcpgenserver.V1betaUpdateBackupParams{}

		// Mock handler
		handler := Handler{}

		// Call the method under test
		result, err := handler.V1betaUpdateBackupUnderBackupVault(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(500), result.(*gcpgenserver.V1betaUpdateBackupInternalServerError).Code)
	})
}
func strPtr(s string) *string {
	return &s
}

func TestConvertToBackupsV1beta(t *testing.T) {
	var int64Ptr = int64(12345)
	t.Run("WhenBackupV1betaIsValid", func(t *testing.T) {
		backup := &models.BackupV1beta{
			ResourceID:               "resource-id",
			VolumeID:                 "volume-id",
			State:                    "READY",
			Created:                  strfmt.DateTime(time.Now()),
			EnforcedRetentionEndTime: nil,
			BackupID:                 "backup-id",
			VolumeUsageBytes:         &int64Ptr,
			SourceVolume:             "source-volume",
			BackupVaultID:            strPtr("backup-vault-id"),
			Description:              strPtr("description"),
			SourceSnapshot:           strPtr("source-snapshot"),
			BackupType:               "FULL",
			BackupChainBytes:         &int64Ptr,
		}

		result := convertToBackupsV1beta(backup)

		assert.Equal(t, "resource-id", result.ResourceId.Value)
		assert.Equal(t, "volume-id", result.VolumeId.Value)
		assert.NotNil(t, result.Created.Value)
		assert.Equal(t, "backup-id", result.BackupId.Value)
		assert.Equal(t, "source-volume", result.SourceVolume.Value)
		assert.Equal(t, "backup-vault-id", result.BackupVaultId.Value)
		assert.Equal(t, "description", result.Description.Value)
		assert.Equal(t, "source-snapshot", result.SourceSnapshot.Value)
	})
}

func TestCreateBackupParams(t *testing.T) {
	t.Run("WhenAllFieldsAreProvided", func(t *testing.T) {
		req := &gcpgenserver.BackupCreateV1beta{
			ResourceId:  "backup-name",
			VolumeId:    "volume-uuid",
			Description: gcpgenserver.NewOptString("backup-description"),
			SnapshotId:  gcpgenserver.NewOptString("snapshot-id"),
		}
		params := gcpgenserver.V1betaCreateBackupParams{
			ProjectNumber: "project-number",
			BackupVaultId: "backup-vault-id",
		}

		result := createBackupParams(req, params)

		assert.Equal(t, "project-number", result.AccountName)
		assert.Equal(t, "backup-vault-id", result.BackupVaultID)
		assert.Equal(t, "volume-uuid", result.VolumeUUID)
		assert.Equal(t, "backup-name", result.BackupName)
		assert.Equal(t, "MANUAL", result.BackupType)
		assert.Equal(t, "backup-description", result.Description)
		assert.Equal(t, "snapshot-id", result.SnapshotID)
	})

	t.Run("WhenOptionalFieldsAreNotSet", func(t *testing.T) {
		req := &gcpgenserver.BackupCreateV1beta{
			ResourceId: "backup-name",
			VolumeId:   "volume-uuid",
		}
		params := gcpgenserver.V1betaCreateBackupParams{
			ProjectNumber: "project-number",
			BackupVaultId: "backup-vault-id",
		}
		result := createBackupParams(req, params)
		assert.Equal(t, "project-number", result.AccountName)
		assert.Equal(t, "backup-vault-id", result.BackupVaultID)
		assert.Equal(t, "volume-uuid", result.VolumeUUID)
		assert.Equal(t, "backup-name", result.BackupName)
		assert.Equal(t, "MANUAL", result.BackupType)
		assert.Empty(t, result.Description)
		assert.Empty(t, result.SnapshotID)
	})
}

func TestV1betaCreateBackup(t *testing.T) {
	t.Run("WhenCreateBackupFailsWithBadRequest", func(t *testing.T) {
		// Mock input parameters
		req := &gcpgenserver.BackupCreateV1beta{}
		params := gcpgenserver.V1betaCreateBackupParams{}

		// Mock handler
		handler := Handler{}

		// Call the method under test
		result, err := handler.V1betaCreateBackup(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaCreateBackupBadRequest).Code)
	})

	t.Run("WhenCreateBackupFailsWithBadRequest", func(t *testing.T) {
		// Mock input parameters
		req := &gcpgenserver.BackupCreateV1beta{}
		params := gcpgenserver.V1betaCreateBackupParams{}

		handler := Handler{}

		// Call the method under test
		result, err := handler.V1betaCreateBackup(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaCreateBackupBadRequest).Code)
	})

	t.Run("WhenCreateBackupFailsInParseAndValidateRegionAndZone", func(t *testing.T) {
		// Mock input parameters
		req := &gcpgenserver.BackupCreateV1beta{
			VolumeId:   "test-volume-id",
			ResourceId: "test-resource-id",
		}
		params := gcpgenserver.V1betaCreateBackupParams{
			LocationId:    "test-location",
			ProjectNumber: "12345",
		}

		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}
		handler := Handler{}

		// Call the method under test
		result, err := handler.V1betaCreateBackup(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaCreateBackupBadRequest).Code)
	})
}

// Test cases for V1betaGetMultipleBackups
func TestV1betaGetMultipleBackups_NotFound(t *testing.T) {
	mockClient := backups.NewMockClientService(t)
	params := gcpgenserver.V1betaGetMultipleBackupsParams{}
	req := &gcpgenserver.BackupUuidListV1beta{}
	mockError := &backups.V1betaGetMultipleBackupsNotFound{
		Payload: &models.Error{Code: 404, Message: "Not Found"},
	}
	mockClient.EXPECT().V1betaGetMultipleBackups(mock.Anything).Return(nil, mockError)
	cvpClient := &cvpapi.Cvp{Backups: mockClient}
	originalCreateClient := createClient
	defer func() { createClient = originalCreateClient }()
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *cvpClient }
	handler := Handler{}
	result, err := handler.V1betaGetMultipleBackups(context.Background(), req, params)
	assert.NoError(t, err)
	assert.Equal(t, float64(404), result.(*gcpgenserver.V1betaGetMultipleBackupsNotFound).Code)
	assert.Equal(t, "Not Found", result.(*gcpgenserver.V1betaGetMultipleBackupsNotFound).Message)
}

func TestV1betaGetMultipleBackups_InternalServerError(t *testing.T) {
	mockClient := backups.NewMockClientService(t)
	params := gcpgenserver.V1betaGetMultipleBackupsParams{}
	req := &gcpgenserver.BackupUuidListV1beta{}
	mockError := &backups.V1betaGetMultipleBackupsInternalServerError{
		Payload: &models.Error{Code: 500, Message: "Internal Server Error"},
	}
	mockClient.EXPECT().V1betaGetMultipleBackups(mock.Anything).Return(nil, mockError)
	cvpClient := &cvpapi.Cvp{Backups: mockClient}
	originalCreateClient := createClient
	defer func() { createClient = originalCreateClient }()
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *cvpClient }
	handler := Handler{}
	result, err := handler.V1betaGetMultipleBackups(context.Background(), req, params)
	assert.NoError(t, err)
	assert.Equal(t, float64(500), result.(*gcpgenserver.V1betaGetMultipleBackupsInternalServerError).Code)
	assert.Equal(t, "Internal Server Error", result.(*gcpgenserver.V1betaGetMultipleBackupsInternalServerError).Message)
}

// Test cases for missing lines in V1betaGetMultipleBackups
func TestV1betaGetMultipleBackups_MissingLines(t *testing.T) {
	t.Run("WhenGetMultipleBackupsFailsWithInternalServerError", func(t *testing.T) {
		// Mock client
		mockClient := backups.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupUuidListV1beta{}

		// Define mock error
		mockError := &backups.V1betaGetMultipleBackupsInternalServerError{
			Payload: &models.Error{
				Code:    500,
				Message: "Internal Server Error",
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackups(mock.Anything).
			Return(nil, mockError)

		cvpClient := &cvpapi.Cvp{Backups: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{}

		// Call the method under test
		result, err := handler.V1betaGetMultipleBackups(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(500), result.(*gcpgenserver.V1betaGetMultipleBackupsInternalServerError).Code)
		assert.Equal(t, "Internal Server Error", result.(*gcpgenserver.V1betaGetMultipleBackupsInternalServerError).Message)
	})

	t.Run("WhenGetMultipleBackupsHandlesEmptyResponse", func(t *testing.T) {
		// Mock client
		mockClient := backups.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupsParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupUuidListV1beta{}

		// Define mock response
		mockResponse := &backups.V1betaGetMultipleBackupsOK{
			Payload: &backups.V1betaGetMultipleBackupsOKBody{Backups: []*models.BackupV1beta{}},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackups(mock.Anything).
			Return(mockResponse, nil)

		cvpClient := &cvpapi.Cvp{Backups: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{}

		// Call the method under test
		result, err := handler.V1betaGetMultipleBackups(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.(*gcpgenserver.V1betaGetMultipleBackupsOK).Backups)
	})
}
func TestV1betaCreateBackup_CVPErrorCases(t *testing.T) {
	type cvpErrCase struct {
		name     string
		err      error
		expected any
	}
	cases := []cvpErrCase{
		{
			name: "BadRequest",
			err: &backups.V1betaCreateBackupBadRequest{
				Payload: &models.Error{Code: 400, Message: "bad request"},
			},
			expected: &gcpgenserver.V1betaCreateBackupBadRequest{},
		},
		{
			name: "Unauthorized",
			err: &backups.V1betaCreateBackupUnauthorized{
				Payload: &models.Error{Code: 401, Message: "unauthorized"},
			},
			expected: &gcpgenserver.V1betaCreateBackupUnauthorized{},
		},
		{
			name: "Forbidden",
			err: &backups.V1betaCreateBackupForbidden{
				Payload: &models.Error{Code: 403, Message: "forbidden"},
			},
			expected: &gcpgenserver.V1betaCreateBackupForbidden{},
		},
		{
			name: "Conflict",
			err: &backups.V1betaCreateBackupConflict{
				Payload: &models.Error{Code: 409, Message: "conflict"},
			},
			expected: &gcpgenserver.V1betaCreateBackupConflict{},
		},
		{
			name: "UnprocessableEntity",
			err: &backups.V1betaCreateBackupUnprocessableEntity{
				Payload: &models.Error{Code: 422, Message: "unprocessable"},
			},
			expected: &gcpgenserver.V1betaCreateBackupUnprocessableEntity{},
		},
		{
			name: "TooManyRequests",
			err: &backups.V1betaCreateBackupTooManyRequests{
				Payload: &models.Error{Code: 429, Message: "too many"},
			},
			expected: &gcpgenserver.V1betaCreateBackupTooManyRequests{},
		},
		{
			name: "InternalServerError",
			err: &backups.V1betaCreateBackupInternalServerError{
				Payload: &models.Error{Code: 500, Message: "internal"},
			},
			expected: &gcpgenserver.V1betaCreateBackupInternalServerError{},
		},
		{
			name:     "DefaultError",
			err:      fmt.Errorf("unexpected error"),
			expected: &gcpgenserver.V1betaCreateBackupInternalServerError{},
		},
	}
	for _, c := range cases {
		t.Run("CVPError_"+c.name, func(t *testing.T) {
			logger := &log.MockLogger{}
			ctx := context.Background()
			ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
			mockClient := backups.NewMockClientService(t)
			mockOrch := orchestrator.NewMockOrchestratorFactory(t)
			req := &gcpgenserver.BackupCreateV1beta{VolumeId: "vol-id", ResourceId: "res-id"}
			params := gcpgenserver.V1betaCreateBackupParams{LocationId: "valid-location-id", ProjectNumber: "proj", BackupVaultId: "vault"}
			originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
			utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
				return "us-east4", "us-east4", nil
			}

			mockOrch.EXPECT().
				GetVolume(ctx, "vol-id").
				Return(nil, fmt.Errorf("not found"))

			mockClient.EXPECT().V1betaCreateBackup(mock.Anything).Return(nil, nil, c.err)
			cvpClient := &cvpapi.Cvp{Backups: mockClient}
			originalCreateClient := createClient
			defer func() {
				createClient = originalCreateClient
				utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			}()
			createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *cvpClient }
			handler := Handler{Orchestrator: mockOrch}
			result, err := handler.V1betaCreateBackup(ctx, req, params)
			assert.NoError(t, err)
			assert.IsType(t, c.expected, result)
		})
	}
}

func TestV1betaCreateBackup_CVPCreateBackupCreatedAndAccepted(t *testing.T) {
	t.Run("ReturnsCreatedBackupWhenCVPBackupCreated", func(t *testing.T) {
		logger := &log.MockLogger{}
		ctx := context.Background()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
		mockClient := backups.NewMockClientService(t)
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		req := &gcpgenserver.BackupCreateV1beta{VolumeId: "vol-id", ResourceId: "res-id"}
		params := gcpgenserver.V1betaCreateBackupParams{LocationId: "valid-location-id", ProjectNumber: "proj", BackupVaultId: "vault"}
		originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
		originalCreateClient := createClient
		defer func() {
			utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			createClient = originalCreateClient
		}()
		utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrch.EXPECT().
			GetVolume(ctx, "vol-id").
			Return(nil, nil)

		cvpBackupCreated := &backups.V1betaCreateBackupCreated{
			Payload: &models.BackupV1beta{BackupID: "backup-id", BackupRegion: strPtr("us-east4"), BackupType: "adhoc", Created: strfmt.DateTime(time.Now().UTC()), VolumeID: "vol-id"},
		}
		mockClient.EXPECT().
			V1betaCreateBackup(mock.Anything).
			Return(cvpBackupCreated, nil, nil)

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{Backups: mockClient}
		}
		handler := Handler{Orchestrator: mockOrch}
		result, err := handler.V1betaCreateBackup(ctx, req, params)

		assert.NoError(t, err)
		assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	})

	t.Run("ReturnsAcceptedBackupWhenCVPBackupAccepted", func(t *testing.T) {
		logger := &log.MockLogger{}
		ctx := context.Background()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
		mockClient := backups.NewMockClientService(t)
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		req := &gcpgenserver.BackupCreateV1beta{VolumeId: "vol-id", ResourceId: "res-id"}
		params := gcpgenserver.V1betaCreateBackupParams{LocationId: "valid-location-id", ProjectNumber: "proj", BackupVaultId: "vault"}
		originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
		originalCreateClient := createClient
		defer func() {
			utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			createClient = originalCreateClient
		}()
		utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrch.EXPECT().
			GetVolume(ctx, "vol-id").
			Return(nil, nil)

		cvpBackupAccepted := &backups.V1betaCreateBackupAccepted{
			Payload: &models.OperationV1beta{Name: "operation1"},
		}
		mockClient.EXPECT().
			V1betaCreateBackup(mock.Anything).
			Return(nil, cvpBackupAccepted, nil)

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{Backups: mockClient}
		}
		handler := Handler{Orchestrator: mockOrch}
		result, err := handler.V1betaCreateBackup(ctx, req, params)

		assert.NoError(t, err)
		assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	})

	t.Run("ReturnsInternalServerErrorWhenUnexpectedFlow", func(t *testing.T) {
		logger := &log.MockLogger{}
		ctx := context.Background()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
		mockClient := backups.NewMockClientService(t)
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		req := &gcpgenserver.BackupCreateV1beta{VolumeId: "vol-id", ResourceId: "res-id"}
		params := gcpgenserver.V1betaCreateBackupParams{LocationId: "valid-location-id", ProjectNumber: "proj", BackupVaultId: "vault"}
		originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
		originalCreateClient := createClient
		defer func() {
			utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			createClient = originalCreateClient
		}()
		utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrch.EXPECT().
			GetVolume(ctx, "vol-id").
			Return(nil, nil)

		mockClient.EXPECT().
			V1betaCreateBackup(mock.Anything).
			Return(nil, nil, nil)

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{Backups: mockClient}
		}

		handler := Handler{Orchestrator: mockOrch}
		result, err := handler.V1betaCreateBackup(ctx, req, params)

		assert.NoError(t, err)
		assert.IsType(t, &gcpgenserver.V1betaCreateBackupInternalServerError{}, result)
		assert.Equal(t, float64(500), result.(*gcpgenserver.V1betaCreateBackupInternalServerError).Code)
	})

	t.Run("ReturnsBadRequestWhenUserInputValidationError", func(t *testing.T) {
		logger := &log.MockLogger{}
		ctx := context.Background()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
		mockClient := backups.NewMockClientService(t)
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		req := &gcpgenserver.BackupCreateV1beta{VolumeId: "vol-id", ResourceId: "res-id"}
		params := gcpgenserver.V1betaCreateBackupParams{LocationId: "valid-location-id", ProjectNumber: "proj", BackupVaultId: "vault"}
		originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
		originalCreateClient := createClient
		defer func() {
			utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			createClient = originalCreateClient
		}()
		utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		vol := &coremodels.Volume{
			BaseModel: coremodels.BaseModel{
				UUID:      "mock-uuid",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				DeletedAt: nil,
			},
			PoolID:                "mock-pool-uuid",
			PoolName:              "mock-pool-name",
			AccountName:           "mock-account-name",
			DisplayName:           "mock-volume-name",
			Description:           "mock-description",
			QuotaInBytes:          1000000,
			LifeCycleState:        "READY",
			LifeCycleStateDetails: "All systems go",
			IsDataProtection:      false,
		}
		mockOrch.EXPECT().
			GetVolume(ctx, "vol-id").
			Return(vol, errors.NewUserInputValidationErr("Invalid input parameters"))

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{Backups: mockClient}
		}

		handler := Handler{Orchestrator: mockOrch}
		result, err := handler.V1betaCreateBackup(ctx, req, params)
		assert.NoError(t, err)
		assert.IsType(t, &gcpgenserver.V1betaCreateBackupBadRequest{}, result)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaCreateBackupBadRequest).Code)
	})
}
