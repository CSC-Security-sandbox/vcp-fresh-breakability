package api

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	mod "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// V1betaCreateBackupPolicy unittests
func TestV1betaCreateBackupPolicy(t *testing.T) {
	t.Run("ReturnsBadRequestWhenRegionParsingFails", func(t *testing.T) {
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:    "local",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId:         "backup-policy",
			Description:        gcpgenserver.NewOptString("test description"),
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(10),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(3),
			Enabled:            gcpgenserver.NewOptBool(false),
		}

		handler := Handler{}

		result, err := handler.V1betaCreateBackupPolicy(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaCreateBackupPolicyBadRequest{}, result)
		assert.Equal(t, "LocationID represents neither a region nor a zone", result.(*gcpgenserver.V1betaCreateBackupPolicyBadRequest).Message)
	})

	t.Run("ReturnsExistingBackupPolicyWhenAlreadyPresentInVCP", func(t *testing.T) {
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:    "local",
			ProjectNumber: "1234567890",
		}
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId:  "existing-policy",
			Description: gcpgenserver.NewOptString("Test new backup policy with already existing backup policy name"),
		}
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), "existing-policy", "1234567890").
			Return(&mod.BackupPolicy{
				ResourceID: "existing-policy",
			}, nil)
		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaCreateBackupPolicy(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
		assert.True(t, result.(*gcpgenserver.OperationV1beta).Done.Value)
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
	})

	t.Run("WhenCreateBackupPolicySuccess", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilErrors.NewNotFoundErr("backup policy", &backupPolicyName))

		dailyBackupLimit := int64(1)
		weeklyBackupLimit := int64(0)
		monthlyBackupLimit := int64(2)
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId:         backupPolicyName,
			Description:        gcpgenserver.NewOptString("testDescription"),
			DailyBackupLimit:   gcpgenserver.NewOptInt(int(dailyBackupLimit)),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(int(weeklyBackupLimit)),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(int(monthlyBackupLimit)),
			Enabled:            gcpgenserver.NewOptBool(true),
		}

		// Define mock response
		jsonResponse := &models.BackupPolicyV1beta{
			ResourceID:  &req.ResourceId,
			Description: &req.Description.Value,
			Enabled:     &req.Enabled.Value,
			State:       "READY",
		}
		backupPolicyJSON, err := json.Marshal(jsonResponse)
		if err != nil {
			t.Fatalf("Failed to marshal mock response: %v", err)
		}
		mockResponse := &backup_policy.V1betaCreateBackupPolicyAccepted{
			Payload: &models.OperationV1beta{
				Name:     "operation-id",
				Done:     nillable.GetBoolPtr(true),
				Response: jsonResponse,
			},
		}
		mockCvpRequest := &backup_policy.V1betaCreateBackupPolicyParams{
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
			Body: &models.BackupPolicyCreateV1beta{
				ResourceNameV1beta: models.ResourceNameV1beta{
					ResourceID: &req.ResourceId,
				},
				DescriptionV1beta: models.DescriptionV1beta{
					Description: &req.Description.Value,
				},
				BackupPolicyScheduleV1beta: models.BackupPolicyScheduleV1beta{
					DailyBackupLimit:   &dailyBackupLimit,
					WeeklyBackupLimit:  &weeklyBackupLimit,
					MonthlyBackupLimit: &monthlyBackupLimit,
				},
				Enabled: &req.Enabled.Value,
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mockCvpRequest).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{Orchestrator: mockOrchestrator}
		// Call the method under test
		result, err := handler.V1betaCreateBackupPolicy(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
		// Check if the response done value is as expected
		assert.Equal(t, string(backupPolicyJSON), result.(*gcpgenserver.OperationV1beta).Response.String())
	})

	t.Run("WhenCreateBackupPolicyFailsWithBadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilErrors.NewNotFoundErr("backup policy", &backupPolicyName))

		// Define request
		dailyBackupLimit := int64(1)
		weeklyBackupLimit := int64(0)
		monthlyBackupLimit := int64(2)
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId:         backupPolicyName,
			Description:        gcpgenserver.NewOptString("testDescription"),
			DailyBackupLimit:   gcpgenserver.NewOptInt(int(dailyBackupLimit)),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(int(weeklyBackupLimit)),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(int(monthlyBackupLimit)),
		}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &backup_policy.V1betaCreateBackupPolicyBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockCvpRequest := &backup_policy.V1betaCreateBackupPolicyParams{
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
			Body: &models.BackupPolicyCreateV1beta{
				ResourceNameV1beta: models.ResourceNameV1beta{
					ResourceID: &req.ResourceId,
				},
				DescriptionV1beta: models.DescriptionV1beta{
					Description: &req.Description.Value,
				},
				BackupPolicyScheduleV1beta: models.BackupPolicyScheduleV1beta{
					DailyBackupLimit:   &dailyBackupLimit,
					WeeklyBackupLimit:  &weeklyBackupLimit,
					MonthlyBackupLimit: &monthlyBackupLimit,
				},
				Enabled: &req.Enabled.Value,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mockCvpRequest).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{Orchestrator: mockOrchestrator}
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
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilErrors.NewNotFoundErr("backup policy", &backupPolicyName))
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId:  backupPolicyName,
			Description: gcpgenserver.NewOptString("testDescription"),
		}
		// Define mock error
		errorCode := float64(409)
		errorMessage := "Conflict error"
		mockError := &backup_policy.V1betaCreateBackupPolicyConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		var dailyBackupLimit, weeklyBackupLimit, monthlyBackupLimit int64
		mockCvpRequest := &backup_policy.V1betaCreateBackupPolicyParams{
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
			Body: &models.BackupPolicyCreateV1beta{
				ResourceNameV1beta: models.ResourceNameV1beta{
					ResourceID: &req.ResourceId,
				},
				DescriptionV1beta: models.DescriptionV1beta{
					Description: &req.Description.Value,
				},
				BackupPolicyScheduleV1beta: models.BackupPolicyScheduleV1beta{
					DailyBackupLimit:   &dailyBackupLimit,
					WeeklyBackupLimit:  &weeklyBackupLimit,
					MonthlyBackupLimit: &monthlyBackupLimit,
				},
				Enabled: &req.Enabled.Value,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mockCvpRequest).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{Orchestrator: mockOrchestrator}
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
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilErrors.NewNotFoundErr("backup policy", &backupPolicyName))
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId: backupPolicyName,
		}
		// Define mock error
		errorCode := float64(401)
		errorMessage := "Unauthorized error"
		mockError := &backup_policy.V1betaCreateBackupPolicyUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		var dailyBackupLimit, weeklyBackupLimit, monthlyBackupLimit int64
		mockCvpRequest := &backup_policy.V1betaCreateBackupPolicyParams{
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
			Body: &models.BackupPolicyCreateV1beta{
				ResourceNameV1beta: models.ResourceNameV1beta{
					ResourceID: &req.ResourceId,
				},
				DescriptionV1beta: models.DescriptionV1beta{
					Description: &req.Description.Value,
				},
				BackupPolicyScheduleV1beta: models.BackupPolicyScheduleV1beta{
					DailyBackupLimit:   &dailyBackupLimit,
					WeeklyBackupLimit:  &weeklyBackupLimit,
					MonthlyBackupLimit: &monthlyBackupLimit,
				},
				Enabled: &req.Enabled.Value,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mockCvpRequest).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{Orchestrator: mockOrchestrator}
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
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilErrors.NewNotFoundErr("backup policy", &backupPolicyName))
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId: backupPolicyName,
		}
		// Define mock error
		errorCode := float64(403)
		errorMessage := "Forbidden error"
		mockError := &backup_policy.V1betaCreateBackupPolicyForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		var dailyBackupLimit, weeklyBackupLimit, monthlyBackupLimit int64
		mockCvpRequest := &backup_policy.V1betaCreateBackupPolicyParams{
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
			Body: &models.BackupPolicyCreateV1beta{
				ResourceNameV1beta: models.ResourceNameV1beta{
					ResourceID: &req.ResourceId,
				},
				DescriptionV1beta: models.DescriptionV1beta{
					Description: &req.Description.Value,
				},
				BackupPolicyScheduleV1beta: models.BackupPolicyScheduleV1beta{
					DailyBackupLimit:   &dailyBackupLimit,
					WeeklyBackupLimit:  &weeklyBackupLimit,
					MonthlyBackupLimit: &monthlyBackupLimit,
				},
				Enabled: &req.Enabled.Value,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mockCvpRequest).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{Orchestrator: mockOrchestrator}
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
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilErrors.NewNotFoundErr("backup policy", &backupPolicyName))
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId: backupPolicyName,
		}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "default error"
		mockError := &backup_policy.V1betaCreateBackupPolicyDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		var dailyBackupLimit, weeklyBackupLimit, monthlyBackupLimit int64
		mockCvpRequest := &backup_policy.V1betaCreateBackupPolicyParams{
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
			Body: &models.BackupPolicyCreateV1beta{
				ResourceNameV1beta: models.ResourceNameV1beta{
					ResourceID: &req.ResourceId,
				},
				DescriptionV1beta: models.DescriptionV1beta{
					Description: &req.Description.Value,
				},
				BackupPolicyScheduleV1beta: models.BackupPolicyScheduleV1beta{
					DailyBackupLimit:   &dailyBackupLimit,
					WeeklyBackupLimit:  &weeklyBackupLimit,
					MonthlyBackupLimit: &monthlyBackupLimit,
				},
				Enabled: &req.Enabled.Value,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mockCvpRequest).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{Orchestrator: mockOrchestrator}
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
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilErrors.NewNotFoundErr("backup policy", &backupPolicyName))
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId: backupPolicyName,
		}
		// Define mock error
		errorCode := float64(500)
		errorMessage := "unknown error during the create backup policy"
		var dailyBackupLimit, weeklyBackupLimit, monthlyBackupLimit int64
		mockCvpRequest := &backup_policy.V1betaCreateBackupPolicyParams{
			LocationID:     params.LocationId,
			ProjectNumber:  params.ProjectNumber,
			XCorrelationID: &params.XCorrelationID.Value,
			Body: &models.BackupPolicyCreateV1beta{
				ResourceNameV1beta: models.ResourceNameV1beta{
					ResourceID: &req.ResourceId,
				},
				DescriptionV1beta: models.DescriptionV1beta{
					Description: &req.Description.Value,
				},
				BackupPolicyScheduleV1beta: models.BackupPolicyScheduleV1beta{
					DailyBackupLimit:   &dailyBackupLimit,
					WeeklyBackupLimit:  &weeklyBackupLimit,
					MonthlyBackupLimit: &monthlyBackupLimit,
				},
				Enabled: &req.Enabled.Value,
			},
		}
		mockResponse := &backup_policy.V1betaCreateBackupPolicyAccepted{
			Payload: &models.OperationV1beta{
				Name:     "operation-id",
				Done:     nillable.GetBoolPtr(true),
				Response: nil,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaCreateBackupPolicy(mockCvpRequest).
			Return(nil, mockResponse)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{Orchestrator: mockOrchestrator}
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
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(1234),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(1234),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(1234),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
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
		req := &gcpgenserver.BackupPolicyUpdateV1beta{}
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
		req := &gcpgenserver.BackupPolicyUpdateV1beta{}
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
		req := &gcpgenserver.BackupPolicyUpdateV1beta{}
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
		req := &gcpgenserver.BackupPolicyUpdateV1beta{}
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
		req := &gcpgenserver.BackupPolicyUpdateV1beta{}
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
func TestV1betaListBackupPolicies(t *testing.T) {
	origBackupEnabled := backupEnabled
	backupEnabled = true
	defer func() { backupEnabled = origBackupEnabled }()

	t.Run("ReturnsBadRequestWhenBackupFeatureDisabled", func(t *testing.T) {
		oldBackupEnabled := backupEnabled
		backupEnabled = false
		defer func() { backupEnabled = oldBackupEnabled }()

		params := gcpgenserver.V1betaListBackupPoliciesParams{
			ProjectNumber: "123",
			LocationId:    "us-west1",
		}
		handler := Handler{}
		result, err := handler.V1betaListBackupPolicies(context.Background(), params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaListBackupPoliciesBadRequest{}, result)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaListBackupPoliciesBadRequest).Code)
		assert.Equal(t, "Backup feature is currently not enabled.", result.(*gcpgenserver.V1betaListBackupPoliciesBadRequest).Message)
	})

	t.Run("ReturnsBadRequestWhenRegionParsingFails", func(t *testing.T) {
		params := gcpgenserver.V1betaListBackupPoliciesParams{
			LocationId:     "local",
			ProjectNumber:  "project-number",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		handler := Handler{}

		result, err := handler.V1betaListBackupPolicies(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaListBackupPoliciesBadRequest{}, result)
		assert.Equal(t, "LocationID represents neither a region nor a zone", result.(*gcpgenserver.V1betaListBackupPoliciesBadRequest).Message)
	})

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

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		// Define mock response
		createdAt := strfmt.DateTime(time.Now().UTC())
		description := "description"
		enabled := true
		resourceId := "test-resource-id"
		volumeCount := int64(2)
		var backupPolicies []*models.BackupPolicyV1beta
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
		bp2 := bp
		bp2.BackupPolicyID = "backup-policy-id-2"
		backupPolicies = append(backupPolicies, &bp)
		backupPolicies = append(backupPolicies, &bp2)
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

		vcpBackupPolicyVolumeCount := make(map[string]int64)
		vcpBackupPolicyVolumeCount["backup-policy-id-1"] = 2
		vcpBackupPolicies := make(map[string]*mod.BackupPolicy)
		vcpBackupPolicies["backup-policy-id-1"] = &mod.BackupPolicy{
			BackupPolicyUUID: "backup-policy-id-1",
			State:            "updating",
		}
		vcpBackupPolicies["backup-policy-id-2"] = &mod.BackupPolicy{
			BackupPolicyUUID: "backup-policy-id-2",
			State:            "active",
		}
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		var backupPolicyIds []string
		mockOrchestrator.EXPECT().
			ListBackupPoliciesAndVolumeCount(mock.Anything, params.ProjectNumber, backupPolicyIds).
			Return(vcpBackupPolicyVolumeCount, vcpBackupPolicies, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		// Call the method under test
		result, err := handler.V1betaListBackupPolicies(context.Background(), params)

		expectedBackupPolicy1 := gcpgenserver.BackupPolicyV1beta{
			BackupPolicyId:     gcpgenserver.NewOptString("backup-policy-id-1"),
			CreatedAt:          gcpgenserver.NewOptDateTime(time.Time(createdAt)),
			Description:        gcpgenserver.NewOptString(description),
			Enabled:            enabled,
			ResourceId:         resourceId,
			State:              gcpgenserver.NewOptBackupPolicyV1betaState("updating"),
			VolumeCount:        gcpgenserver.NewOptInt(int(volumeCount + 2)),
			DailyBackupLimit:   gcpgenserver.NewOptInt(int(volumeCount)),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(int(volumeCount)),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(int(volumeCount)),
		}
		expectedBackupPolicy2 := expectedBackupPolicy1
		expectedBackupPolicy2.BackupPolicyId = gcpgenserver.NewOptString("backup-policy-id-2")
		expectedBackupPolicy2.VolumeCount = gcpgenserver.NewOptInt(int(volumeCount))
		expectedBackupPolicy2.State = gcpgenserver.NewOptBackupPolicyV1betaState("active")
		expectedBackupPolicies := []gcpgenserver.BackupPolicyV1beta{
			expectedBackupPolicy1,
			expectedBackupPolicy2,
		}

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, 2, len(result.(*gcpgenserver.V1betaListBackupPoliciesOK).BackupPolicies))
		assert.Equal(t, expectedBackupPolicies, result.(*gcpgenserver.V1betaListBackupPoliciesOK).BackupPolicies)
	})

	t.Run("WhenVCPListBackupPoliciesAndVolumeCountFails", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Define mock response
		createdAt := strfmt.DateTime(time.Now().UTC())
		description := "description"
		enabled := true
		resourceId := "test-resource-id"
		volumeCount := int64(2)
		var backupPolicies []*models.BackupPolicyV1beta
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
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		var backupPolicyIds []string
		mockOrchestrator.EXPECT().
			ListBackupPoliciesAndVolumeCount(mock.Anything, params.ProjectNumber, backupPolicyIds).
			Return(nil, nil, errors.New("failed to list backup policy volume count"))

		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{Orchestrator: mockOrchestrator}
		// Call the method under test
		result, err := handler.V1betaListBackupPolicies(context.Background(), params)
		errorCode := float64(500)
		errorMessage := "Failed to list backup policies"

		// Assertions
		assert.NoError(t, err)
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListBackupPoliciesInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListBackupPoliciesInternalServerError).Message)
	})

	t.Run("WhenSDEListBackupPoliciesReturnsEmpty", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaListBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Define mock response
		var backupPolicies []*models.BackupPolicyV1beta
		mockResponse := &backup_policy.V1betaListBackupPoliciesOK{
			Payload: &backup_policy.V1betaListBackupPoliciesOKBody{
				BackupPolicies: backupPolicies,
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaListBackupPolicies(mock.Anything).
			Return(mockResponse, nil)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		vcpBackupPolicyVolumeCount := make(map[string]int64)
		var backupPolicyIds []string
		vcpBackupPolicies := make(map[string]*mod.BackupPolicy)
		mockOrchestrator.EXPECT().
			ListBackupPoliciesAndVolumeCount(mock.Anything, params.ProjectNumber, backupPolicyIds).
			Return(vcpBackupPolicyVolumeCount, vcpBackupPolicies, nil)

		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{Orchestrator: mockOrchestrator}
		// Call the method under test
		result, err := handler.V1betaListBackupPolicies(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result.(*gcpgenserver.V1betaListBackupPoliciesOK).BackupPolicies))
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
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
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
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
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
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
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
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
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
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
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
	origBackupEnabled := backupEnabled
	backupEnabled = true
	defer func() { backupEnabled = origBackupEnabled }()

	t.Run("ReturnsBadRequestWhenBackupFeatureDisabled", func(t *testing.T) {
		oldBackupEnabled := backupEnabled
		backupEnabled = false
		defer func() { backupEnabled = oldBackupEnabled }()

		params := gcpgenserver.V1betaGetMultipleBackupPoliciesParams{
			LocationId:    "us-central1",
			ProjectNumber: "123456789",
		}
		req := &gcpgenserver.BackupPolicyIdListV1beta{}

		handler := Handler{}
		result, err := handler.V1betaGetMultipleBackupPolicies(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaGetMultipleBackupPoliciesBadRequest{}, result)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesBadRequest).Code)
		assert.Equal(t, "Backup feature is currently not enabled.", result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesBadRequest).Message)
	})

	t.Run("ReturnsBadRequestWhenRegionParsingFails", func(t *testing.T) {
		params := gcpgenserver.V1betaGetMultipleBackupPoliciesParams{
			LocationId:     "local",
			ProjectNumber:  "project-number",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		req := &gcpgenserver.BackupPolicyIdListV1beta{
			BackupPolicyUuids: []string{"backup-policy-uuid-1"},
		}

		handler := Handler{}

		result, err := handler.V1betaGetMultipleBackupPolicies(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaGetMultipleBackupPoliciesBadRequest{}, result)
		assert.Equal(t, "LocationID represents neither a region nor a zone", result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesBadRequest).Message)
	})

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
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		req := &gcpgenserver.BackupPolicyIdListV1beta{
			BackupPolicyUuids: []string{"backup-policy-id-1"},
		}

		// Define mock response
		createdAt := strfmt.DateTime(time.Now().UTC())
		description := "description"
		enabled := true
		resourceId := "test-resource-id"
		volumeCount := int64(2)
		var backupPolicies []*models.BackupPolicyV1beta
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
		bp2 := bp
		bp2.BackupPolicyID = "backup-policy-id-2"
		backupPolicies = append(backupPolicies, &bp)
		backupPolicies = append(backupPolicies, &bp2)
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

		vcpBackupPolicyVolumeCount := make(map[string]int64)
		vcpBackupPolicyVolumeCount["backup-policy-id-1"] = 2
		vcpBackupPolicies := make(map[string]*mod.BackupPolicy)
		vcpBackupPolicies["backup-policy-id-1"] = &mod.BackupPolicy{
			BackupPolicyUUID: "backup-policy-id-1",
			State:            "updating",
		}
		vcpBackupPolicies["backup-policy-id-2"] = &mod.BackupPolicy{
			BackupPolicyUUID: "backup-policy-id-2",
			State:            "active",
		}
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			ListBackupPoliciesAndVolumeCount(mock.Anything, params.ProjectNumber, req.BackupPolicyUuids).
			Return(vcpBackupPolicyVolumeCount, vcpBackupPolicies, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupPolicies(context.Background(), req, params)

		expectedBackupPolicy1 := gcpgenserver.BackupPolicyV1beta{
			BackupPolicyId:     gcpgenserver.NewOptString("backup-policy-id-1"),
			CreatedAt:          gcpgenserver.NewOptDateTime(time.Time(createdAt)),
			Description:        gcpgenserver.NewOptString(description),
			Enabled:            enabled,
			ResourceId:         resourceId,
			State:              gcpgenserver.NewOptBackupPolicyV1betaState("updating"),
			VolumeCount:        gcpgenserver.NewOptInt(int(volumeCount + 2)),
			DailyBackupLimit:   gcpgenserver.NewOptInt(int(volumeCount)),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(int(volumeCount)),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(int(volumeCount)),
		}
		expectedBackupPolicy2 := expectedBackupPolicy1
		expectedBackupPolicy2.BackupPolicyId = gcpgenserver.NewOptString("backup-policy-id-2")
		expectedBackupPolicy2.VolumeCount = gcpgenserver.NewOptInt(int(volumeCount))
		expectedBackupPolicy2.State = gcpgenserver.NewOptBackupPolicyV1betaState("active")
		expectedBackupPolicies := []gcpgenserver.BackupPolicyV1beta{
			expectedBackupPolicy1,
			expectedBackupPolicy2,
		}

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, 2, len(result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesOK).BackupPolicies))
		assert.Equal(t, expectedBackupPolicies, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesOK).BackupPolicies)
	})

	t.Run("WhenListBackupPoliciesAndVolumeCountFails", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		req := &gcpgenserver.BackupPolicyIdListV1beta{
			BackupPolicyUuids: []string{"backup-policy-id-1"},
		}

		// Define mock response
		createdAt := strfmt.DateTime(time.Now().UTC())
		description := "description"
		enabled := true
		resourceId := "test-resource-id"
		volumeCount := int64(2)
		var backupPolicies []*models.BackupPolicyV1beta
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

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			ListBackupPoliciesAndVolumeCount(mock.Anything, params.ProjectNumber, req.BackupPolicyUuids).
			Return(nil, nil, errors.New("failed to get multiple backup policy volume count"))

		handler := Handler{Orchestrator: mockOrchestrator}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupPolicies(context.Background(), req, params)
		errorCode := float64(500)
		errorMessage := "Failed to get backup policies"

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesInternalServerError).Message)
	})

	t.Run("WhenSDEGetMultipleBackupPoliciesReturnsNoBackupPolicies", func(t *testing.T) {
		// Define request
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleBackupPoliciesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		req := &gcpgenserver.BackupPolicyIdListV1beta{
			BackupPolicyUuids: []string{"backup-policy-id-1"},
		}

		// Define mock response
		var backupPolicies []*models.BackupPolicyV1beta
		mockResponse := &backup_policy.V1betaGetMultipleBackupPoliciesOK{
			Payload: &backup_policy.V1betaGetMultipleBackupPoliciesOKBody{
				BackupPolicies: backupPolicies,
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleBackupPolicies(mock.Anything).
			Return(mockResponse, nil)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		vcpBackupPolicyVolumeCount := make(map[string]int64)
		vcpBackupPolicies := make(map[string]*mod.BackupPolicy)
		mockOrchestrator.EXPECT().
			ListBackupPoliciesAndVolumeCount(mock.Anything, params.ProjectNumber, req.BackupPolicyUuids).
			Return(vcpBackupPolicyVolumeCount, vcpBackupPolicies, nil)

		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{Orchestrator: mockOrchestrator}
		// Call the method under test
		result, err := handler.V1betaGetMultipleBackupPolicies(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesOK).BackupPolicies))
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
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		// Define request
		req := &gcpgenserver.BackupPolicyIdListV1beta{
			BackupPolicyUuids: []string{"backup-policy-id-1"},
		}

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
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		// Define request
		req := &gcpgenserver.BackupPolicyIdListV1beta{
			BackupPolicyUuids: []string{"backup-policy-id-1"},
		}

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
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		// Define request
		req := &gcpgenserver.BackupPolicyIdListV1beta{
			BackupPolicyUuids: []string{"backup-policy-id-1"},
		}

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
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		// Define request
		req := &gcpgenserver.BackupPolicyIdListV1beta{
			BackupPolicyUuids: []string{"backup-policy-id-1"},
		}

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
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		// Define request
		req := &gcpgenserver.BackupPolicyIdListV1beta{
			BackupPolicyUuids: []string{"backup-policy-id-1"},
		}

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

func TestConvertToBackupPolicyV1beta(t *testing.T) {
	t.Run("WhenAllValuesAreSet", func(t *testing.T) {
		now := strfmt.DateTime(time.Now().UTC())
		resourceID := "backup-policy-resource-id"
		description := "test-description"
		enabled := true
		state := "READY"
		volumeCount := int64(3)
		daily := int64(1)
		weekly := int64(2)
		monthly := int64(3)
		backupPolicyUUID := "backup-policy-uuid"

		bp := &models.BackupPolicyV1beta{
			BackupPolicyID: backupPolicyUUID,
			ResourceID:     &resourceID,
			Enabled:        &enabled,
			Description:    &description,
			CreatedAt:      &now,
			State:          state,
			VolumeCount:    &volumeCount,
			BackupPolicyScheduleV1beta: models.BackupPolicyScheduleV1beta{
				DailyBackupLimit:   &daily,
				WeeklyBackupLimit:  &weekly,
				MonthlyBackupLimit: &monthly,
			},
		}

		result := convertToBackupPolicyV1beta(bp)
		expected := gcpgenserver.BackupPolicyV1beta{
			BackupPolicyId:     gcpgenserver.NewOptString(backupPolicyUUID),
			ResourceId:         resourceID,
			Enabled:            true,
			Description:        gcpgenserver.NewOptString(description),
			CreatedAt:          gcpgenserver.NewOptDateTime(time.Time(now)),
			State:              gcpgenserver.NewOptBackupPolicyV1betaState(gcpgenserver.BackupPolicyV1betaState(state)),
			VolumeCount:        gcpgenserver.NewOptInt(3),
			DailyBackupLimit:   gcpgenserver.NewOptInt(1),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(3),
		}
		assert.Equal(t, expected, result)
	})
	t.Run("WhenValuesAreNil", func(t *testing.T) {
		bp := &models.BackupPolicyV1beta{}

		result := convertToBackupPolicyV1beta(bp)
		expected := gcpgenserver.BackupPolicyV1beta{
			BackupPolicyId: gcpgenserver.NewOptString(""),
			ResourceId:     "",
			Enabled:        false,
		}
		assert.Equal(t, expected, result)
	})
}
