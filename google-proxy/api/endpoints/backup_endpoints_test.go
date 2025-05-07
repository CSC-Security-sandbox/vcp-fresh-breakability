package api

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backups"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
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
		req := &gcpgenserver.BackupUUIDListV1beta{
			BackupUUIDs: []string{"backup-id-1"},
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
		req := &gcpgenserver.BackupUUIDListV1beta{}

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
		req := &gcpgenserver.BackupUUIDListV1beta{}

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
		req := &gcpgenserver.BackupUUIDListV1beta{}

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
