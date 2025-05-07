package api

import (
	"context"
	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// V1betaCreateBackupPolicy unittests
func TestV1betaCreateBackupPolicy(t *testing.T) {
	t.Run("WhenCreateBackupPolicySuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId:         "bp-1",
			Description:        gcpgenserver.NewOptString("testDescription"),
			DailyBackupLimit:   gcpgenserver.NewOptInt(1234),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(1234),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(1234),
		}

		// Define mock response
		mockResponse := &backup_policy.V1betaCreateBackupPolicyAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateBackupPolicy(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenCreateBackupPolicyFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backup_policy.V1betaCreateBackupPolicyBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateBackupPolicy(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateBackupPolicyBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateBackupPolicyBadRequest).Message)
	})

	t.Run("WhenCreateBackupPolicyFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{}
		// Define mock error
		errorCode := float64(409)
		errorMessage := "Conflict errort"
		mockError := &backup_policy.V1betaCreateBackupPolicyConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateBackupPolicy(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateBackupPolicyConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateBackupPolicyConflict).Message)
	})

	t.Run("WhenCreateBackupPolicyFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{}
		// Define mock error
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &backup_policy.V1betaCreateBackupPolicyUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateBackupPolicy(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateBackupPolicyUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateBackupPolicyUnauthorized).Message)
	})

	t.Run("WhenCreateBackupPolicyFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{}
		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &backup_policy.V1betaCreateBackupPolicyForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateBackupPolicy(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateBackupPolicyForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateBackupPolicyForbidden).Message)
	})

	t.Run("WhenCreateBackupPolicyFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &backup_policy.V1betaCreateBackupPolicyDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateBackupPolicy(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateBackupPolicyInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateBackupPolicyInternalServerError).Message)
	})

	t.Run("WhenCreateBackupPolicyFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "unknown error during the create backup policy"
		mockError := &backup_policy.V1betaCreateBackupPolicyInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaCreateBackupPolicy(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateBackupPolicyInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateBackupPolicyInternalServerError).Message)
	})
}

// V1betaDeleteBackupPolicy unittests
func TestV1betaDeleteBackupPolicy(t *testing.T) {
	t.Run("WhenDeleteBackupPolicySuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "bp-1",
		}
		// Define mock response
		mockResponse := &backup_policy.V1betaDeleteBackupPolicyAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(mockResponse, nil, nil)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backup_policy.V1betaDeleteBackupPolicyBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupPolicyBadRequest).Message)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithConflict", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}
		// Define mock error
		errorMessage := "Conflict error"
		errorCode := float64(409)
		mockError := &backup_policy.V1betaDeleteBackupPolicyConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupPolicyConflict).Message)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &backup_policy.V1betaDeleteBackupPolicyUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupPolicyUnauthorized).Message)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &backup_policy.V1betaDeleteBackupPolicyForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupPolicyForbidden).Message)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &backup_policy.V1betaDeleteBackupPolicyDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyInternalServerError).Code)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}
		// Define mock error
		errorMessage := "unknown error during the delete backup policy"
		errorCode := float64(500)
		mockError := &backup_policy.V1betaDeleteBackupPolicyDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyInternalServerError).Code)
	})
}

// V1betaDescribeBackupPolicy unittests
func TestV1betaDescribeBackupPolicy(t *testing.T) {
	t.Run("WhenDescribeBackupPolicySuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "backup-policy-1",
		}
		// Define mock response
		resourceId := "test-resource-id"
		enabled := true
		description := "test-description"
		createdAt := strfmt.DateTime(time.Now().UTC())
		volumeCount := int64(1)
		backupLimit := int64(123)
		bpSchedule := models.BackupPolicyScheduleV1beta{
			DailyBackupLimit:   &backupLimit,
			WeeklyBackupLimit:  &backupLimit,
			MonthlyBackupLimit: &backupLimit,
		}
		backupPolicyV1 := models.BackupPolicyV1beta{
			Enabled:                    &enabled,
			Description:                &description,
			CreatedAt:                  &createdAt,
			State:                      "created",
			VolumeCount:                &volumeCount,
			BackupPolicyScheduleV1beta: bpSchedule,
			ResourceID:                 &resourceId,
			BackupPolicyID:             "backup-policy-1",
		}
		var volumeBackups []*models.VolumeBackupDetailsV1beta
		volumeBackup := models.VolumeBackupDetailsV1beta{
			PolicyEnabled:        &enabled,
			ScheduledBackupCount: 123,
			VolumeName:           "test-volume-name",
		}
		volumeBackups = append(volumeBackups, &volumeBackup)
		mockResponse := &backup_policy.V1betaDescribeBackupPolicyOK{
			Payload: &models.BackupPolicyDetailsV1beta{
				BackupPolicyV1beta: backupPolicyV1,
				VolumeBackups:      volumeBackups,
				VolumeCount:        &volumeCount,
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupPolicy(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupPolicy(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, "backup-policy-1", result.(*gcpgenserver.BackupPolicyDetailsV1beta).BackupPolicyId.Value)
	})

	t.Run("WhenDescribeBackupPolicyFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backup_policy.V1betaDescribeBackupPolicyBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupPolicy(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupPolicy(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeBackupPolicyBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeBackupPolicyBadRequest).Message)
	})

	t.Run("WhenDescribeBackupPolicyFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &backup_policy.V1betaDescribeBackupPolicyUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupPolicy(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupPolicy(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeBackupPolicyUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeBackupPolicyUnauthorized).Message)
	})

	t.Run("WhenDescribeBackupPolicyFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &backup_policy.V1betaDescribeBackupPolicyForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupPolicy(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupPolicy(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeBackupPolicyForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeBackupPolicyForbidden).Message)
	})

	t.Run("WhenDescribeBackupPolicyFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &backup_policy.V1betaDescribeBackupPolicyDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeBackupPolicy(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeBackupPolicy(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeBackupPolicyInternalServerError).Code)
	})
}

// V1betaUpdateBackupPolicy unittests
func TestV1betaUpdateBackupPolicy(t *testing.T) {
	t.Run("WhenUpdateBackupPolicySuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyScheduleV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(1234),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(1234),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(1234),
		}

		// Define mock response
		mockResponse := &backup_policy.V1betaUpdateBackupPolicyAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupPolicy(mock.Anything).
			Return(mockResponse, nil, nil)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenUpdateBackupPolicyFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyScheduleV1beta{}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backup_policy.V1betaUpdateBackupPolicyBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest).Message)
	})

	t.Run("WhenUpdateBackupPolicyFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyScheduleV1beta{}
		// Define mock error
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &backup_policy.V1betaUpdateBackupPolicyUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupPolicyUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupPolicyUnauthorized).Message)
	})

	t.Run("WhenUpdateBackupPolicyFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyScheduleV1beta{}
		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &backup_policy.V1betaUpdateBackupPolicyForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupPolicyForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupPolicyForbidden).Message)
	})

	t.Run("WhenUpdateBackupPolicyFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyScheduleV1beta{}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &backup_policy.V1betaUpdateBackupPolicyDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupPolicyInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupPolicyInternalServerError).Message)
	})

	t.Run("WhenUpdateBackupPolicyFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyScheduleV1beta{}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "unknown error during the update backup policy"
		mockError := &backup_policy.V1betaUpdateBackupPolicyInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateBackupPolicyInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateBackupPolicyInternalServerError).Message)
	})
}

// V1betaListBackupPolicies unittests
func TestV1ListBackupPolicies(t *testing.T) {
	t.Run("WhenListBackupPoliciesSuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		// Define mock response
		createdAt := strfmt.DateTime(time.Now().UTC())
		description := "description"
		enabled := true
		resourceId := "test-resource-id"
		volumeCount := int64(2)
		backupPolicies := []*models.BackupPolicyV1beta{}
		schedule := models.BackupPolicyScheduleV1beta{
			DailyBackupLimit:   &volumeCount,
			WeeklyBackupLimit:  &volumeCount,
			MonthlyBackupLimit: &volumeCount,
		}
		bp := models.BackupPolicyV1beta{
			BackupPolicyScheduleV1beta: schedule,
			BackupPolicyID:             "backup-policy-id-1",
			CreatedAt:                  &createdAt,
			Description:                &description,
			Enabled:                    &enabled,
			ResourceID:                 &resourceId,
			State:                      "active",
			VolumeCount:                &volumeCount,
		}
		backupPolicies = append(backupPolicies, &bp)
		mockResponse := &backup_policy.V1betaListBackupPoliciesOK{
			Payload: &backup_policy.V1betaListBackupPoliciesOKBody{
				BackupPolicies: backupPolicies,
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListBackupPolicies(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListBackupPolicies(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, "backup-policy-id-1", result.(*gcpgenserver.V1betaListBackupPoliciesOK).BackupPolicies[0].BackupPolicyId.Value)
	})

	t.Run("WhenListBackupPoliciesFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backup_policy.V1betaListBackupPoliciesBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListBackupPolicies(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListBackupPolicies(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListBackupPoliciesBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListBackupPoliciesBadRequest).Message)
	})

	t.Run("WhenListBackupPoliciesFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define mock error
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &backup_policy.V1betaListBackupPoliciesUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListBackupPolicies(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListBackupPolicies(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListBackupPoliciesUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListBackupPoliciesUnauthorized).Message)
	})

	t.Run("WhenListBackupPoliciesFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &backup_policy.V1betaListBackupPoliciesForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListBackupPolicies(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListBackupPolicies(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListBackupPoliciesForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListBackupPoliciesForbidden).Message)
	})

	t.Run("WhenListBackupPoliciesFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &backup_policy.V1betaListBackupPoliciesDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListBackupPolicies(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListBackupPolicies(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListBackupPoliciesInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListBackupPoliciesInternalServerError).Message)
	})

	t.Run("WhenListBackupPoliciesFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "unknown error during the list backup policies"
		mockError := &backup_policy.V1betaListBackupPoliciesInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListBackupPolicies(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaListBackupPolicies(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListBackupPoliciesInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListBackupPoliciesInternalServerError).Message)
	})
}

// V1betaGetMultipleBackupPolicies unittests
func TestV1GetMultipleBackupPolicies(t *testing.T) {
	t.Run("WhenGetMultipleBackupPoliciesSuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyIDListV1beta{
			BackupPolicyUUIDs: []string{"backup-policy-id-1"},
		}

		// Define mock response
		createdAt := strfmt.DateTime(time.Now().UTC())
		description := "description"
		enabled := true
		resourceId := "test-resource-id"
		volumeCount := int64(2)
		backupPolicies := []*models.BackupPolicyV1beta{}
		schedule := models.BackupPolicyScheduleV1beta{
			DailyBackupLimit:   &volumeCount,
			WeeklyBackupLimit:  &volumeCount,
			MonthlyBackupLimit: &volumeCount,
		}
		bp := models.BackupPolicyV1beta{
			BackupPolicyScheduleV1beta: schedule,
			BackupPolicyID:             "backup-policy-id-1",
			CreatedAt:                  &createdAt,
			Description:                &description,
			Enabled:                    &enabled,
			ResourceID:                 &resourceId,
			State:                      "active",
			VolumeCount:                &volumeCount,
		}
		backupPolicies = append(backupPolicies, &bp)
		mockResponse := &backup_policy.V1betaGetMultipleBackupPoliciesOK{
			Payload: &backup_policy.V1betaGetMultipleBackupPoliciesOKBody{
				BackupPolicies: backupPolicies,
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupPolicies(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupPolicies(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, "backup-policy-id-1", result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesOK).BackupPolicies[0].BackupPolicyId.Value)
	})

	t.Run("WhenGetMultipleBackupPoliciesFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyIDListV1beta{}

		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backup_policy.V1betaGetMultipleBackupPoliciesBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupPolicies(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupPolicies(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesBadRequest).Message)
	})

	t.Run("WhenGetMultipleBackupPoliciesFailsWithUnauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyIDListV1beta{}

		// Define mock error
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &backup_policy.V1betaGetMultipleBackupPoliciesUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupPolicies(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupPolicies(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesUnauthorized).Message)
	})

	t.Run("WhenGetMultipleBackupPoliciesFailsWithForbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyIDListV1beta{}

		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &backup_policy.V1betaGetMultipleBackupPoliciesForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupPolicies(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupPolicies(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesForbidden).Message)
	})

	t.Run("WhenGetMultipleBackupPoliciesFailsWithDefault", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyIDListV1beta{}

		// Define mock error
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &backup_policy.V1betaGetMultipleBackupPoliciesDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupPolicies(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupPolicies(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesInternalServerError).Message)
	})

	t.Run("WhenGetMultipleBackupPoliciesFailsWithUnknownError", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyIDListV1beta{}

		// Define mock error
		errorCode := float64(500)
		errorMessage := "unknown error during the get multiple backup policies"
		mockError := &backup_policy.V1betaGetMultipleBackupPoliciesDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupPolicies(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupPolicies(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesInternalServerError).Message)
	})
}
