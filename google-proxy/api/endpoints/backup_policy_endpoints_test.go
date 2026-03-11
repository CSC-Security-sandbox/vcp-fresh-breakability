package api

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	coreerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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

		oldBackupEnabled := backupEnabled
		defer func() { backupEnabled = oldBackupEnabled }()
		backupEnabled = true

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
		oldBackupEnabled := backupEnabled
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = oldBackupEnabled
			parseAndValidateRegionAndZone = oldValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), "existing-policy", "1234567890").
			Return(&coremodels.BackupPolicy{
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
		oldBackupEnabled := backupEnabled
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = oldBackupEnabled
			parseAndValidateRegionAndZone = oldValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &backupPolicyName))

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
		oldBackupEnabled := backupEnabled
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = oldBackupEnabled
			parseAndValidateRegionAndZone = oldValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &backupPolicyName))

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
		oldBackupEnabled := backupEnabled
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = oldBackupEnabled
			parseAndValidateRegionAndZone = oldValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &backupPolicyName))
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
		oldBackupEnabled := backupEnabled
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = oldBackupEnabled
			parseAndValidateRegionAndZone = oldValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &backupPolicyName))
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
		oldBackupEnabled := backupEnabled
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = oldBackupEnabled
			parseAndValidateRegionAndZone = oldValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &backupPolicyName))
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
		oldBackupEnabled := backupEnabled
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = oldBackupEnabled
			parseAndValidateRegionAndZone = oldValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &backupPolicyName))
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
		assert.Contains(t, result.(*gcpgenserver.V1betaCreateBackupPolicyInternalServerError).Message, errorMessage)
	})

	t.Run("WhenCreateBackupPolicyFailsWithNotImplemented", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		oldBackupEnabled := backupEnabled
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = oldBackupEnabled
			parseAndValidateRegionAndZone = oldValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &backupPolicyName))
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId: backupPolicyName,
		}
		// Define mock error
		errorCode := float64(501)
		errorMessage := "not implemented"
		mockError := &backup_policy.V1betaCreateBackupPolicyNotImplemented{
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
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaCreateBackupPolicyNotImplemented).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaCreateBackupPolicyNotImplemented).Message)
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
		oldBackupEnabled := backupEnabled
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = oldBackupEnabled
			parseAndValidateRegionAndZone = oldValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		backupPolicyName := "backup-policy"
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetBackupPolicyByNameAndOwnerID", context.Background(), backupPolicyName, "1234567890").
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &backupPolicyName))
		// Define request
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId: backupPolicyName,
		}
		// Define mock error - return an Accepted response as error (unknown/unexpected type)
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
		// Unknown error type falls to default, returns InternalServerError with err.Error()
		assert.Equal(t, float64(500), result.(*gcpgenserver.V1betaCreateBackupPolicyInternalServerError).Code)
		assert.Contains(t, result.(*gcpgenserver.V1betaCreateBackupPolicyInternalServerError).Message, "v1betaCreateBackupPolicyAccepted")
	})

	t.Run("ReturnsBadRequestWhenBackupFeatureIsDisabled", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId:         "backup-policy",
			Description:        gcpgenserver.NewOptString("test description"),
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(10),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(3),
			Enabled:            gcpgenserver.NewOptBool(false),
		}

		oldBackupEnabled := backupEnabled
		defer func() { backupEnabled = oldBackupEnabled }()
		backupEnabled = false

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaCreateBackupPolicy(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaCreateBackupPolicyBadRequest{}, result)

		op := result.(*gcpgenserver.V1betaCreateBackupPolicyBadRequest)
		assert.Equal(t, float64(400), op.Code)
		assert.Equal(t, "Backup feature is currently not enabled.", op.Message)
	})
}

// V1betaDeleteBackupPolicy unittests
func TestV1betaDeleteBackupPolicy(t *testing.T) {
	origBackupEnabled := backupEnabled
	backupEnabled = true
	defer func() { backupEnabled = origBackupEnabled }()

	params := gcpgenserver.V1betaDeleteBackupPolicyParams{
		ProjectNumber:  "1234567890",
		LocationId:     "us-central1",
		BackupPolicyId: "test-backup-policy-id",
	}

	t.Run("ReturnsBadRequestWhenUserInputValidationError", func(t *testing.T) {
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(&coremodels.BackupPolicy{ResourceID: "test-backup-policy-id", BackupPolicyUUID: "test-backup-policy-id"}, nil)
		mockOrchestrator.EXPECT().
			DeleteBackupPolicy(mock.Anything, mock.Anything).
			Return(nil, "", utilerrors.NewUserInputValidationErr("invalid input for delete"))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		badRequest, ok := result.(*gcpgenserver.V1betaDeleteBackupPolicyBadRequest)
		assert.True(t, ok)
		assert.Equal(t, float64(400), badRequest.Code)
		assert.Equal(t, "invalid input for delete", badRequest.Message)
	})

	t.Run("ReturnsBadRequestWhenBackupFeatureDisabled", func(t *testing.T) {
		oldBackupEnabled := backupEnabled
		backupEnabled = false
		defer func() { backupEnabled = oldBackupEnabled }()

		handler := Handler{}
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaDeleteBackupPolicyBadRequest{}, result)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaDeleteBackupPolicyBadRequest).Code)
		assert.Equal(t, "Backup feature is currently not enabled.", result.(*gcpgenserver.V1betaDeleteBackupPolicyBadRequest).Message)
	})

	t.Run("ReturnsBadRequestWhenRegionParsingFails", func(t *testing.T) {
		origParse := parseAndValidateRegionAndZone
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{Code: 400, Message: "LocationID represents neither a region nor a zone"}
		}
		defer func() { parseAndValidateRegionAndZone = origParse }()

		handler := Handler{}
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaDeleteBackupPolicyBadRequest{}, result)
		assert.Equal(t, float64(400), result.(*gcpgenserver.V1betaDeleteBackupPolicyBadRequest).Code)
		assert.Equal(t, "LocationID represents neither a region nor a zone", result.(*gcpgenserver.V1betaDeleteBackupPolicyBadRequest).Message)
	})

	t.Run("ReturnsInternalServerErrorWhenGetBackupPolicyFails", func(t *testing.T) {
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(nil, fmt.Errorf("unexpected error"))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaDeleteBackupPolicyInternalServerError{}, result)
		assert.Equal(t, float64(500), result.(*gcpgenserver.V1betaDeleteBackupPolicyInternalServerError).Code)
	})

	t.Run("WhenDeleteBackupPolicySucceedsNotFoundInVCP", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		// Define mock response
		mockResponse := &backup_policy.V1betaDeleteBackupPolicyAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(mockResponse, nil, nil)

		// Set up the mock client behavior
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &params.BackupPolicyId))
		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithBadRequestNotFoundInVCP", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
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
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)

		// Set up the mock client behavior
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &params.BackupPolicyId))
		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupPolicyBadRequest).Message)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithUnauthorizedNotFoundInVCP", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
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
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)

		// Set up the mock client behavior
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &params.BackupPolicyId))
		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupPolicyUnauthorized).Message)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithForbiddenNotFoundInVCP", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
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
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)

		// Set up the mock client behavior
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &params.BackupPolicyId))
		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupPolicyForbidden).Message)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithInternalServerErrorNotFoundInVCP", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		// Define mock error
		errorMessage := "Internal server error"
		errorCode := float64(500)
		mockError := &backup_policy.V1betaDeleteBackupPolicyDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)

		// Set up the mock client behavior
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &params.BackupPolicyId))
		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyInternalServerError).Code)
		assert.Contains(t, result.(*gcpgenserver.V1betaDeleteBackupPolicyInternalServerError).Message, errorMessage)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithUnknownErrorNotFoundInVCP", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		// Define mock error
		errorMessage := "unknown error during the delete backup policy"
		errorCode := float64(500)

		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, nil)

		// Set up the mock client behavior
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &params.BackupPolicyId))
		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupPolicyInternalServerError).Message)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithConflictNotFoundInVCP", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
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
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)

		// Set up the mock client behavior
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &params.BackupPolicyId))
		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupPolicyConflict).Message)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithNotImplementedNotFoundInVCP", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)

		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}
		// Define mock error
		errorMessage := "not implemented"
		errorCode := float64(501)
		mockError := &backup_policy.V1betaDeleteBackupPolicyNotImplemented{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().
			V1betaDeleteBackupPolicy(mock.Anything).
			Return(nil, nil, mockError)

		// Set up the mock client behavior
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()

		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup policy", &params.BackupPolicyId))
		handler := Handler{Orchestrator: mockOrchestrator}

		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteBackupPolicyNotImplemented).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteBackupPolicyNotImplemented).Message)
	})

	t.Run("WhenDeleteBackupPolicySucceedsFoundInVCP", func(t *testing.T) {
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		backupPolicy := &coremodels.BackupPolicy{
			ResourceID:       "test-resource",
			BackupPolicyUUID: "test-backup-policy-id",
		}
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		handler := Handler{Orchestrator: mockOrchestrator}

		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(backupPolicy, nil)

		mockOrchestrator.EXPECT().
			DeleteBackupPolicy(mock.Anything, mock.Anything).
			Return(backupPolicy, "operation-123", nil)

		res, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		assert.NoError(t, err)
		op, ok := res.(*gcpgenserver.OperationV1beta)
		assert.True(t, ok)
		assert.Contains(t, op.Name.Value, "operation-123")
		assert.False(t, op.Done.Value)
		assert.NotNil(t, op.Response)
	})

	t.Run("WhenDeleteBackupPolicyFailsWithUnknownErrorFoundInVCP", func(t *testing.T) {
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		backupPolicy := &coremodels.BackupPolicy{
			ResourceID:       "test-resource",
			BackupPolicyUUID: "test-backup-policy-id",
		}
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		handler := Handler{Orchestrator: mockOrchestrator}

		mockOrchestrator.EXPECT().
			GetBackupPolicyByUUIDAndOwnerID(mock.Anything, params.BackupPolicyId, params.ProjectNumber).
			Return(backupPolicy, nil)

		mockOrchestrator.EXPECT().
			DeleteBackupPolicy(mock.Anything, mock.Anything).
			Return(nil, "", fmt.Errorf("unknown error"))

		res, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.IsType(t, &gcpgenserver.V1betaDeleteBackupPolicyInternalServerError{}, res)
		assert.Equal(t, float64(500), res.(*gcpgenserver.V1betaDeleteBackupPolicyInternalServerError).Code)
		assert.Equal(t, "Failed to delete backup policy", res.(*gcpgenserver.V1betaDeleteBackupPolicyInternalServerError).Message)
	})

	t.Run("ReturnsBadRequestWhenBackupFeatureIsDisabled", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaDeleteBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}

		oldBackupEnabled := backupEnabled
		defer func() { backupEnabled = oldBackupEnabled }()
		backupEnabled = false

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaDeleteBackupPolicy(context.Background(), params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaDeleteBackupPolicyBadRequest{}, result)

		op := result.(*gcpgenserver.V1betaDeleteBackupPolicyBadRequest)
		assert.Equal(t, float64(400), op.Code)
		assert.Equal(t, "Backup feature is currently not enabled.", op.Message)
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

		originalBackupEnabled := backupEnabled
		originalCreateClient := createClient
		defer func() {
			backupEnabled = originalBackupEnabled
			createClient = originalCreateClient
		}()
		backupEnabled = true
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

		originalBackupEnabled := backupEnabled
		originalCreateClient := createClient
		defer func() {
			backupEnabled = originalBackupEnabled
			createClient = originalCreateClient
		}()
		backupEnabled = true
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

		originalBackupEnabled := backupEnabled
		originalCreateClient := createClient
		defer func() {
			backupEnabled = originalBackupEnabled
			createClient = originalCreateClient
		}()
		backupEnabled = true
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

		originalBackupEnabled := backupEnabled
		originalCreateClient := createClient
		defer func() {
			backupEnabled = originalBackupEnabled
			createClient = originalCreateClient
		}()
		backupEnabled = true
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

		originalBackupEnabled := backupEnabled
		originalCreateClient := createClient
		defer func() {
			backupEnabled = originalBackupEnabled
			createClient = originalCreateClient
		}()
		backupEnabled = true
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

	t.Run("WhenDescribeBackupPolicyFailsWithNotImplemented", func(t *testing.T) {
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
		errorCode := float64(501)
		errorMessage := "not implemented"
		mockError := &backup_policy.V1betaDescribeBackupPolicyNotImplemented{
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

		originalBackupEnabled := backupEnabled
		originalCreateClient := createClient
		defer func() {
			backupEnabled = originalBackupEnabled
			createClient = originalCreateClient
		}()
		backupEnabled = true
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
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeBackupPolicyNotImplemented).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeBackupPolicyNotImplemented).Message)
	})

	t.Run("ReturnsBadRequestWhenBackupFeatureIsDisabled", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaDescribeBackupPolicyParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
			BackupPolicyId: "ad-1",
		}

		oldBackupEnabled := backupEnabled
		defer func() { backupEnabled = oldBackupEnabled }()
		backupEnabled = false

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaDescribeBackupPolicy(context.Background(), params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaDescribeBackupPolicyBadRequest{}, result)

		op := result.(*gcpgenserver.V1betaDescribeBackupPolicyBadRequest)
		assert.Equal(t, float64(400), op.Code)
		assert.Equal(t, "Backup feature is currently not enabled.", op.Message)
	})
}

// V1betaUpdateBackupPolicy unittests
func TestV1betaUpdateBackupPolicy(t *testing.T) {
	origBackupEnabled := backupEnabled
	originalValue := utils.IsImmutableBackupEnabled()
	defer func() {
		backupEnabled = origBackupEnabled
		utils.SetImmutableBackupEnabledForTest(originalValue)
	}()
	backupEnabled = true
	utils.SetImmutableBackupEnabledForTest(true)
	t.Run("WhenBackupPolicyExistsInSDE_Success", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(3),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		originalUpdateBackupPolicyInSDE := updateBackupPolicyInSDE
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			updateBackupPolicyInSDE = originalUpdateBackupPolicyInSDE
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}
		updateBackupPolicyInSDE = func(ctx context.Context, req *gcpgenserver.BackupPolicyUpdateV1beta, params gcpgenserver.V1betaUpdateBackupPolicyParams) gcpgenserver.V1betaUpdateBackupPolicyRes {
			return &gcpgenserver.OperationV1beta{}
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(nil, utilerrors.NewNotFoundErr("backup policy", nillable.ToPointer("backup-policy-uuid-1")))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})

	t.Run("WhenBackupPolicyExistsInVCP_Success", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(3),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		originalUpdateBackupPolicyInSDE := updateBackupPolicyInSDE
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			updateBackupPolicyInSDE = originalUpdateBackupPolicyInSDE
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}
		updateBackupPolicyInSDE = func(ctx context.Context, req *gcpgenserver.BackupPolicyUpdateV1beta, params gcpgenserver.V1betaUpdateBackupPolicyParams) gcpgenserver.V1betaUpdateBackupPolicyRes {
			return &gcpgenserver.OperationV1beta{}
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(&coremodels.BackupPolicy{BackupPolicyUUID: "backup-policy-uuid-1", ResourceID: "test-backup-policy"}, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").Return([]string{}, nil)
		mockOrchestrator.On("UpdateBackupPolicy", ctx, mock.Anything).Return(
			&coremodels.BackupPolicy{ResourceID: "test-backup-policy"}, "test-operation-id", nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})

	t.Run("WhenRegionAndZoneParsingFails", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "invalid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(3),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{Code: 500, Message: "could not parse region and zone"}
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.IsType(tt, (*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)(nil), result)

		op := result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)
		assert.Equal(tt, float64(500), op.Code)
		assert.Equal(tt, "could not parse region and zone", op.Message)
	})

	t.Run("WhenBackupPolicyCouldNotBeFetchedFromVCP", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(3),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(nil, fmt.Errorf("could not fetch backup policy from VCP"))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(tt, (*gcpgenserver.V1betaUpdateBackupPolicyInternalServerError)(nil), result)

		op := result.(*gcpgenserver.V1betaUpdateBackupPolicyInternalServerError)
		assert.Equal(tt, float64(500), op.Code)
		assert.Equal(tt, "Internal server error", op.Message)
	})

	t.Run("WhenBackupPolicyUpdateFailsInVCP", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(3),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(&coremodels.BackupPolicy{BackupPolicyUUID: "backup-policy-uuid-1", ResourceID: "test-backup-policy"}, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").Return([]string{}, nil)
		mockOrchestrator.On("UpdateBackupPolicy", ctx, mock.Anything).Return(
			nil, "", fmt.Errorf("could not update backup policy"))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(tt, (*gcpgenserver.V1betaUpdateBackupPolicyInternalServerError)(nil), result)

		op := result.(*gcpgenserver.V1betaUpdateBackupPolicyInternalServerError)
		assert.Equal(tt, float64(500), op.Code)
		assert.Equal(tt, "Internal server error", op.Message)
	})

	t.Run("WhenBackupPolicyUpdateFailsInVCPDueToInvalidInput", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(500),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(300),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(250),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(&coremodels.BackupPolicy{BackupPolicyUUID: "backup-policy-uuid-1", ResourceID: "test-backup-policy"}, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").Return([]string{}, nil)
		mockOrchestrator.On("UpdateBackupPolicy", ctx, mock.Anything).Return(
			nil, "", utilerrors.NewUserInputValidationErr("the total number of backups exceeds the limit of 1000"))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(tt, (*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)(nil), result)

		op := result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)
		assert.Equal(tt, float64(400), op.Code)
		assert.Equal(tt, "the total number of backups exceeds the limit of 1000", op.Message)
	})

	t.Run("WhenBackupPolicyUpdateReturnsBlankOperationId_Success", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(3),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(&coremodels.BackupPolicy{BackupPolicyUUID: "backup-policy-uuid-1", ResourceID: "test-backup-policy"}, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").Return([]string{}, nil)
		mockOrchestrator.On("UpdateBackupPolicy", ctx, mock.Anything).Return(
			&coremodels.BackupPolicy{ResourceID: "test-backup-policy"}, "", nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(tt, (*gcpgenserver.OperationV1beta)(nil), result)
	})

	t.Run("ReturnsBadRequestWhenBackupFeatureIsDisabled", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(3),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		defer func() { backupEnabled = originalBackupEnabled }()
		backupEnabled = false

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &gcpgenserver.V1betaUpdateBackupPolicyBadRequest{}, result)

		op := result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)
		assert.Equal(t, float64(400), op.Code)
		assert.Equal(t, "Backup feature is currently not enabled.", op.Message)
	})

	// Immutable Validation Test Cases
	t.Run("WhenImmutableValidationSucceeds", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(60), // Valid limit for immutable backup vault
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(5),  // 5 weeks = 35 days > 30 days immutable period
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),  // 2 months = 60 days > 30 days immutable period
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockBackupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "backup-policy-uuid-1",
			ResourceID:         "test-backup-policy",
			DailyBackupLimit:   30,
			WeeklyBackupLimit:  2,
			MonthlyBackupLimit: 1,
		}

		// Mock backup vault with immutable settings
		var retentionDays int64 = 30
		mockBackupVault := &coremodels.BackupVaultV1beta{
			BackupVaultID:  "vault-123",
			LifeCycleState: coremodels.LifeCycleStateREADY,
			BackupRetentionPolicy: coremodels.BackupRetentionPolicyparams{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
			},
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(mockBackupPolicy, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").
			Return([]string{"vault-123"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults", ctx, []string{"vault-123"}).Return([]*coremodels.BackupVaultV1beta{mockBackupVault}, nil)
		mockOrchestrator.On("UpdateBackupPolicy", ctx, mock.Anything).Return(
			&coremodels.BackupPolicy{ResourceID: "test-backup-policy"}, "test-operation-id", nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.IsType(tt, (*gcpgenserver.OperationV1beta)(nil), result)
	})

	t.Run("WhenImmutableValidationFailsDueToInsufficientRetention", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(10), // Insufficient for 30-day immutable period
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(1),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockBackupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "backup-policy-uuid-1",
			ResourceID:         "test-backup-policy",
			DailyBackupLimit:   30,
			WeeklyBackupLimit:  2,
			MonthlyBackupLimit: 1,
		}

		var retentionDays int64 = 30
		mockBackupVault := &coremodels.BackupVaultV1beta{
			BackupVaultID:  "vault-123",
			LifeCycleState: coremodels.LifeCycleStateREADY,
			BackupRetentionPolicy: coremodels.BackupRetentionPolicyparams{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
			},
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(mockBackupPolicy, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").
			Return([]string{"vault-123"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults", ctx, []string{"vault-123"}).Return([]*coremodels.BackupVaultV1beta{mockBackupVault}, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.IsType(tt, (*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)(nil), result)

		badRequest := result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Contains(tt, badRequest.Message, "daily backup retention")
		assert.Contains(tt, badRequest.Message, "is less than backup vault immutable period")
	})

	t.Run("WhenImmutableValidationRetriesOnBackupVaultUpdatingState", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(60),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(4),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		origSleep := commonparams.SleepFn
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			commonparams.SleepFn = origSleep
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		// Mock sleep to avoid real delays in test
		sleepCalled := 0
		commonparams.SleepFn = func(d time.Duration) { sleepCalled++ }

		mockBackupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "backup-policy-uuid-1",
			ResourceID:         "test-backup-policy",
			DailyBackupLimit:   30,
			WeeklyBackupLimit:  2,
			MonthlyBackupLimit: 1,
		}

		var retentionDays int64 = 30
		mockBackupVaultUpdating := &coremodels.BackupVaultV1beta{
			BackupVaultID:  "vault-123",
			LifeCycleState: coremodels.LifeCycleStateUpdating, // First call returns updating state
			BackupRetentionPolicy: coremodels.BackupRetentionPolicyparams{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
			},
		}

		mockBackupVaultReady := &coremodels.BackupVaultV1beta{
			BackupVaultID:  "vault-123",
			LifeCycleState: coremodels.LifeCycleStateREADY, // Second call returns ready state
			BackupRetentionPolicy: coremodels.BackupRetentionPolicyparams{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
			},
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(mockBackupPolicy, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").
			Return([]string{"vault-123"}, nil).Times(2)
		mockOrchestrator.On("GetMultipleBackupVaults", ctx, []string{"vault-123"}).Return([]*coremodels.BackupVaultV1beta{mockBackupVaultUpdating}, nil).Once()
		mockOrchestrator.On("GetMultipleBackupVaults", ctx, []string{"vault-123"}).Return([]*coremodels.BackupVaultV1beta{mockBackupVaultReady}, nil).Once()
		mockOrchestrator.On("UpdateBackupPolicy", ctx, mock.Anything).Return(
			&coremodels.BackupPolicy{ResourceID: "test-backup-policy"}, "test-operation-id", nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.IsType(tt, (*gcpgenserver.OperationV1beta)(nil), result)
		assert.Equal(tt, 1, sleepCalled, "Should sleep once for one retry")
	})

	t.Run("WhenImmutableValidationFailsAfterMaxRetries", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(60),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(4),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		origSleep := commonparams.SleepFn
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			commonparams.SleepFn = origSleep
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		// Mock sleep to avoid real delays in test
		sleepCalled := 0
		commonparams.SleepFn = func(d time.Duration) { sleepCalled++ }

		mockBackupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "backup-policy-uuid-1",
			ResourceID:         "test-backup-policy",
			DailyBackupLimit:   30,
			WeeklyBackupLimit:  2,
			MonthlyBackupLimit: 1,
		}

		var retentionDays int64 = 30
		mockBackupVaultUpdating := &coremodels.BackupVaultV1beta{
			BackupVaultID:  "vault-123",
			LifeCycleState: coremodels.LifeCycleStateUpdating, // Always returns updating state
			BackupRetentionPolicy: coremodels.BackupRetentionPolicyparams{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
			},
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(mockBackupPolicy, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").
			Return([]string{"vault-123"}, nil).Times(3) // Max retries = 3
		mockOrchestrator.On("GetMultipleBackupVaults", ctx, []string{"vault-123"}).Return([]*coremodels.BackupVaultV1beta{mockBackupVaultUpdating}, nil).Times(3)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.IsType(tt, (*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)(nil), result)

		badRequest := result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Contains(tt, badRequest.Message, "Immutable backup vault is being updated")
		assert.Equal(tt, 2, sleepCalled, "Should sleep twice for two retries")
	})

	t.Run("WhenNoBackupVaultsAssociated", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(10),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(1),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockBackupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "backup-policy-uuid-1",
			ResourceID:         "test-backup-policy",
			DailyBackupLimit:   30,
			WeeklyBackupLimit:  2,
			MonthlyBackupLimit: 1,
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(mockBackupPolicy, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").
			Return([]string{}, nil) // No backup vaults associated
		mockOrchestrator.On("UpdateBackupPolicy", ctx, mock.Anything).Return(
			&coremodels.BackupPolicy{ResourceID: "test-backup-policy"}, "test-operation-id", nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.IsType(tt, (*gcpgenserver.OperationV1beta)(nil), result)
	})

	t.Run("WhenBackupVaultHasNoImmutableSettings", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(10),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(1),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockBackupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "backup-policy-uuid-1",
			ResourceID:         "test-backup-policy",
			DailyBackupLimit:   30,
			WeeklyBackupLimit:  2,
			MonthlyBackupLimit: 1,
		}

		// Backup vault with no immutable settings
		mockBackupVault := &coremodels.BackupVaultV1beta{
			BackupVaultID:  "vault-123",
			LifeCycleState: coremodels.LifeCycleStateREADY,
			BackupRetentionPolicy: coremodels.BackupRetentionPolicyparams{
				BackupMinimumEnforcedRetentionDuration: nil, // No retention period
				IsDailyBackupImmutable:                 false,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
			},
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(mockBackupPolicy, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").
			Return([]string{"vault-123"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults", ctx, []string{"vault-123"}).Return([]*coremodels.BackupVaultV1beta{mockBackupVault}, nil)
		mockOrchestrator.On("UpdateBackupPolicy", ctx, mock.Anything).Return(
			&coremodels.BackupPolicy{ResourceID: "test-backup-policy"}, "test-operation-id", nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.IsType(tt, (*gcpgenserver.OperationV1beta)(nil), result)
	})

	t.Run("WhenValidationFailsForWeeklyBackupLimits", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(60),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(2), // Too low: 2 weeks * 7 days = 14 days < 30 days immutable period
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockBackupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "backup-policy-uuid-1",
			ResourceID:         "test-backup-policy",
			DailyBackupLimit:   30,
			WeeklyBackupLimit:  4,
			MonthlyBackupLimit: 1,
		}

		var retentionDays int64 = 30
		mockBackupVault := &coremodels.BackupVaultV1beta{
			BackupVaultID:  "vault-123",
			LifeCycleState: coremodels.LifeCycleStateREADY,
			BackupRetentionPolicy: coremodels.BackupRetentionPolicyparams{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 false,
				IsWeeklyBackupImmutable:                true, // Weekly backups are immutable
				IsMonthlyBackupImmutable:               false,
			},
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(mockBackupPolicy, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").
			Return([]string{"vault-123"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults", ctx, []string{"vault-123"}).Return([]*coremodels.BackupVaultV1beta{mockBackupVault}, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.IsType(tt, (*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)(nil), result)

		badRequest := result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Contains(tt, badRequest.Message, "weekly backup retention")
		assert.Contains(tt, badRequest.Message, "is less than backup vault immutable period")
	})

	t.Run("WhenValidationFailsForMonthlyBackupLimits", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(60),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(8),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(1), // Too low: 1 month * 30 days = 30 days = exactly the immutable period (should be >=)
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockBackupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "backup-policy-uuid-1",
			ResourceID:         "test-backup-policy",
			DailyBackupLimit:   30,
			WeeklyBackupLimit:  4,
			MonthlyBackupLimit: 2,
		}

		var retentionDays int64 = 35 // 35-day immutable period
		mockBackupVault := &coremodels.BackupVaultV1beta{
			BackupVaultID:  "vault-123",
			LifeCycleState: coremodels.LifeCycleStateREADY,
			BackupRetentionPolicy: coremodels.BackupRetentionPolicyparams{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 false,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               true, // Monthly backups are immutable
			},
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(mockBackupPolicy, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").
			Return([]string{"vault-123"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults", ctx, []string{"vault-123"}).Return([]*coremodels.BackupVaultV1beta{mockBackupVault}, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.IsType(tt, (*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)(nil), result)

		badRequest := result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Contains(tt, badRequest.Message, "monthly backup retention")
		assert.Contains(tt, badRequest.Message, "is less than backup vault immutable period")
	})

	t.Run("WhenGetBackupVaultUUIDsFailsDuringValidation", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(60),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(4),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockBackupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "backup-policy-uuid-1",
			ResourceID:         "test-backup-policy",
			DailyBackupLimit:   30,
			WeeklyBackupLimit:  2,
			MonthlyBackupLimit: 1,
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(mockBackupPolicy, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(nil, fmt.Errorf("failed to get backup vault UUIDs"))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.IsType(tt, (*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)(nil), result)

		badRequest := result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Contains(tt, badRequest.Message, "failed to get backup vault UUIDs")
	})

	t.Run("WhenGetMultipleBackupVaultsFailsDuringValidation", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(60),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(4),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockBackupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "backup-policy-uuid-1",
			ResourceID:         "test-backup-policy",
			DailyBackupLimit:   30,
			WeeklyBackupLimit:  2,
			MonthlyBackupLimit: 1,
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(mockBackupPolicy, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").
			Return([]string{"vault-123"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults", ctx, []string{"vault-123"}).
			Return(nil, fmt.Errorf("failed to get backup vaults"))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.IsType(tt, (*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)(nil), result) // Should fail when backup vault fetch fails

		badRequest := result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Contains(tt, badRequest.Message, "failed to get multiple backup vaults")
	})

	t.Run("WhenMultipleBackupVaultsWithMixedImmutableSettings", func(tt *testing.T) {
		ctx := context.Background()
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)

		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-uuid-1",
			LocationId:     "valid-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			DailyBackupLimit:   gcpgenserver.NewOptInt(60),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(8),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
			Enabled:            gcpgenserver.NewOptBool(true),
			Description:        gcpgenserver.NewOptString("test-description"),
		}

		originalBackupEnabled := backupEnabled
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = originalBackupEnabled
			parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		}()
		backupEnabled = true
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "valid-region", "valid-zone", nil
		}

		mockBackupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "backup-policy-uuid-1",
			ResourceID:         "test-backup-policy",
			DailyBackupLimit:   30,
			WeeklyBackupLimit:  4,
			MonthlyBackupLimit: 1,
		}

		var retentionDays int64 = 30
		mockBackupVaultImmutable := &coremodels.BackupVaultV1beta{
			BackupVaultID:  "vault-123",
			LifeCycleState: coremodels.LifeCycleStateREADY,
			BackupRetentionPolicy: coremodels.BackupRetentionPolicyparams{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
			},
		}

		mockBackupVaultNonImmutable := &coremodels.BackupVaultV1beta{
			BackupVaultID:  "vault-456",
			LifeCycleState: coremodels.LifeCycleStateREADY,
			BackupRetentionPolicy: coremodels.BackupRetentionPolicyparams{
				BackupMinimumEnforcedRetentionDuration: nil, // No immutable settings
				IsDailyBackupImmutable:                 false,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
			},
		}

		mockOrchestrator.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "backup-policy-uuid-1", "1234567890").
			Return(mockBackupPolicy, nil)
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID", ctx, "backup-policy-uuid-1", "1234567890").
			Return([]string{"vault-123", "vault-456"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults", ctx, []string{"vault-123", "vault-456"}).
			Return([]*coremodels.BackupVaultV1beta{mockBackupVaultImmutable, mockBackupVaultNonImmutable}, nil)
		mockOrchestrator.On("UpdateBackupPolicy", ctx, mock.Anything).Return(
			&coremodels.BackupPolicy{ResourceID: "test-backup-policy"}, "test-operation-id", nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaUpdateBackupPolicy(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.IsType(tt, (*gcpgenserver.OperationV1beta)(nil), result)
	})
}

func Test_updateBackupPolicyInSDE(t *testing.T) {
	t.Run("WhenUpdateBackupPolicyInSDESucceeds", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			Description:        gcpgenserver.NewOptString("test-description"),
			Enabled:            gcpgenserver.NewOptBool(true),
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(3),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
		}
		// Set up the mock client behavior
		mockClient.On("V1betaUpdateBackupPolicy", mock.Anything).
			Return(&backup_policy.V1betaUpdateBackupPolicyAccepted{
				Payload: &models.OperationV1beta{
					Name: "test-operation",
					Done: nillable.ToPointer(true),
				},
			}, nil, nil)
		// Call the method under test
		result := updateBackupPolicyInSDE(context.Background(), req, params)
		// Assertions
		assert.NotNil(t, result)
		assert.IsType(t, (*gcpgenserver.OperationV1beta)(nil), result)

		op := result.(*gcpgenserver.OperationV1beta)
		assert.Equal(t, "test-operation", op.Name.Value)
		assert.True(t, true, op.Done.Value)
	})

	t.Run("UpdateBackupPolicyInSDE_BadRequest", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			Description:        gcpgenserver.NewOptString("test-description"),
			Enabled:            gcpgenserver.NewOptBool(true),
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(3),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
		}
		// Set up the mock client behavior
		mockClient.On("V1betaUpdateBackupPolicy", mock.Anything).
			Return(nil, nil, &backup_policy.V1betaUpdateBackupPolicyBadRequest{
				Payload: &models.Error{
					Code:    400,
					Message: "Bad Request",
				},
			})
		// Call the method under test
		result := updateBackupPolicyInSDE(context.Background(), req, params)
		// Assertions
		assert.NotNil(t, result)
		assert.IsType(t, (*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)(nil), result)

		op := result.(*gcpgenserver.V1betaUpdateBackupPolicyBadRequest)
		assert.Equal(t, float64(400), op.Code)
		assert.Equal(t, "Bad Request", op.Message)
	})

	t.Run("UpdateBackupPolicyInSDE_Unauthorized", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			Description:        gcpgenserver.NewOptString("test-description"),
			Enabled:            gcpgenserver.NewOptBool(true),
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(3),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
		}
		// Set up the mock client behavior
		mockClient.On("V1betaUpdateBackupPolicy", mock.Anything).
			Return(nil, nil, &backup_policy.V1betaUpdateBackupPolicyUnauthorized{
				Payload: &models.Error{
					Code:    401,
					Message: "Unauthorized",
				},
			})
		// Call the method under test
		result := updateBackupPolicyInSDE(context.Background(), req, params)
		// Assertions
		assert.NotNil(t, result)
		assert.IsType(t, (*gcpgenserver.V1betaUpdateBackupPolicyUnauthorized)(nil), result)

		op := result.(*gcpgenserver.V1betaUpdateBackupPolicyUnauthorized)
		assert.Equal(t, float64(401), op.Code)
		assert.Equal(t, "Unauthorized", op.Message)
	})

	t.Run("UpdateBackupPolicyInSDE_Forbidden", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			Description:        gcpgenserver.NewOptString("test-description"),
			Enabled:            gcpgenserver.NewOptBool(true),
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(3),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
		}
		// Set up the mock client behavior
		mockClient.On("V1betaUpdateBackupPolicy", mock.Anything).
			Return(nil, nil, &backup_policy.V1betaUpdateBackupPolicyForbidden{
				Payload: &models.Error{
					Code:    403,
					Message: "Forbidden",
				},
			})
		// Call the method under test
		result := updateBackupPolicyInSDE(context.Background(), req, params)
		// Assertions
		assert.NotNil(t, result)
		assert.IsType(t, (*gcpgenserver.V1betaUpdateBackupPolicyForbidden)(nil), result)

		op := result.(*gcpgenserver.V1betaUpdateBackupPolicyForbidden)
		assert.Equal(t, float64(403), op.Code)
		assert.Equal(t, "Forbidden", op.Message)
	})

	t.Run("UpdateBackupPolicyInSDE_NotFound", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			Description:        gcpgenserver.NewOptString("test-description"),
			Enabled:            gcpgenserver.NewOptBool(true),
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(3),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
		}
		// Set up the mock client behavior
		mockClient.On("V1betaUpdateBackupPolicy", mock.Anything).
			Return(nil, nil, &backup_policy.V1betaUpdateBackupPolicyNotFound{
				Payload: &models.Error{
					Code:    404,
					Message: "Not Found",
				},
			})
		// Call the method under test
		result := updateBackupPolicyInSDE(context.Background(), req, params)
		// Assertions
		assert.NotNil(t, result)
		assert.IsType(t, (*gcpgenserver.V1betaUpdateBackupPolicyNotFound)(nil), result)

		op := result.(*gcpgenserver.V1betaUpdateBackupPolicyNotFound)
		assert.Equal(t, float64(404), op.Code)
		assert.Equal(t, "Not Found", op.Message)
	})

	t.Run("UpdateBackupPolicyInSDE_InternalServerError", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			Description:        gcpgenserver.NewOptString("test-description"),
			Enabled:            gcpgenserver.NewOptBool(true),
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(3),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
		}
		// Set up the mock client behavior
		mockClient.On("V1betaUpdateBackupPolicy", mock.Anything).
			Return(nil, nil, fmt.Errorf("could not update backup policy in SDE"))
		// Call the method under test
		result := updateBackupPolicyInSDE(context.Background(), req, params)
		// Assertions
		assert.NotNil(t, result)
		assert.IsType(t, (*gcpgenserver.V1betaUpdateBackupPolicyInternalServerError)(nil), result)

		op := result.(*gcpgenserver.V1betaUpdateBackupPolicyInternalServerError)
		assert.Equal(t, float64(500), op.Code)
		assert.Equal(t, "could not update backup policy in SDE", op.Message)
	})

	t.Run("UpdateBackupPolicyInSDE_NotImplemented", func(t *testing.T) {
		// Create a mock client
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		// Define input parameters
		params := gcpgenserver.V1betaUpdateBackupPolicyParams{
			BackupPolicyId: "backup-policy-id-1",
			LocationId:     "test-location",
			ProjectNumber:  "1234567890",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.BackupPolicyUpdateV1beta{
			Description:        gcpgenserver.NewOptString("test-description"),
			Enabled:            gcpgenserver.NewOptBool(true),
			DailyBackupLimit:   gcpgenserver.NewOptInt(5),
			WeeklyBackupLimit:  gcpgenserver.NewOptInt(3),
			MonthlyBackupLimit: gcpgenserver.NewOptInt(2),
		}
		// Set up the mock client behavior
		mockClient.On("V1betaUpdateBackupPolicy", mock.Anything).
			Return(nil, nil, &backup_policy.V1betaUpdateBackupPolicyNotImplemented{
				Payload: &models.Error{
					Code:    501,
					Message: "not implemented",
				},
			})
		// Call the method under test
		result := updateBackupPolicyInSDE(context.Background(), req, params)
		// Assertions
		assert.NotNil(t, result)
		assert.IsType(t, (*gcpgenserver.V1betaUpdateBackupPolicyNotImplemented)(nil), result)

		op := result.(*gcpgenserver.V1betaUpdateBackupPolicyNotImplemented)
		assert.Equal(t, float64(501), op.Code)
		assert.Equal(t, "not implemented", op.Message)
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
		vcpBackupPolicies := make(map[string]*coremodels.BackupPolicy)
		vcpBackupPolicies["backup-policy-id-1"] = &coremodels.BackupPolicy{
			BackupPolicyUUID: "backup-policy-id-1",
			State:            "updating",
		}
		vcpBackupPolicies["backup-policy-id-2"] = &coremodels.BackupPolicy{
			BackupPolicyUUID: "backup-policy-id-2",
			State:            "active",
		}
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
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
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		var backupPolicyIds []string
		mockOrchestrator.EXPECT().
			ListBackupPoliciesAndVolumeCount(mock.Anything, params.ProjectNumber, backupPolicyIds).
			Return(nil, nil, fmt.Errorf("failed to list backup policy volume count"))

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
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		vcpBackupPolicyVolumeCount := make(map[string]int64)
		var backupPolicyIds []string
		vcpBackupPolicies := make(map[string]*coremodels.BackupPolicy)
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
		assert.Contains(t, result.(*gcpgenserver.V1betaListBackupPoliciesInternalServerError).Message, errorMessage)
	})

	t.Run("WhenListBackupPoliciesFailsWithNotImplemented", func(t *testing.T) {
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
		errorCode := float64(501)
		errorMessage := "not implemented"
		mockError := &backup_policy.V1betaListBackupPoliciesNotImplemented{
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
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaListBackupPoliciesNotImplemented).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaListBackupPoliciesNotImplemented).Message)
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
		assert.Contains(t, result.(*gcpgenserver.V1betaListBackupPoliciesInternalServerError).Message, errorMessage)
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
		vcpBackupPolicies := make(map[string]*coremodels.BackupPolicy)
		vcpBackupPolicies["backup-policy-id-1"] = &coremodels.BackupPolicy{
			BackupPolicyUUID: "backup-policy-id-1",
			State:            "updating",
		}
		vcpBackupPolicies["backup-policy-id-2"] = &coremodels.BackupPolicy{
			BackupPolicyUUID: "backup-policy-id-2",
			State:            "active",
		}
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
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

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			ListBackupPoliciesAndVolumeCount(mock.Anything, params.ProjectNumber, req.BackupPolicyUuids).
			Return(nil, nil, fmt.Errorf("failed to get multiple backup policy volume count"))

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
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		vcpBackupPolicyVolumeCount := make(map[string]int64)
		vcpBackupPolicies := make(map[string]*coremodels.BackupPolicy)
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
		assert.Contains(t, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesInternalServerError).Message, errorMessage)
	})

	t.Run("WhenGetMultipleBackupPoliciesFailsWithTooManyRequests", func(t *testing.T) {
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
		errorCode := float64(429)
		errorMessage := "Too many requests"
		mockError := &backup_policy.V1betaGetMultipleBackupPoliciesTooManyRequests{
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
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesTooManyRequests).Message)
	})

	t.Run("WhenGetMultipleBackupPoliciesFailsWithNotImplemented", func(t *testing.T) {
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
		errorCode := float64(501)
		errorMessage := "not implemented"
		mockError := &backup_policy.V1betaGetMultipleBackupPoliciesNotImplemented{
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
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesNotImplemented).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesNotImplemented).Message)
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
		assert.Contains(t, result.(*gcpgenserver.V1betaGetMultipleBackupPoliciesInternalServerError).Message, errorMessage)
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

// Comprehensive unit tests for _performBackupVaultValidation function
func Test_performBackupVaultValidation(t *testing.T) {
	ctx := context.Background()

	// Helper function to create backup policy
	createBackupPolicy := func(backupPolicyUUID string) *coremodels.BackupPolicy {
		return &coremodels.BackupPolicy{
			BackupPolicyUUID:   backupPolicyUUID,
			DailyBackupLimit:   7,
			WeeklyBackupLimit:  4,
			MonthlyBackupLimit: 12,
		}
	}

	// Helper function to create backup policy update parameters
	createUpdateParams := func(accountName string) *commonparams.UpdateBackupPolicyParams {
		return &commonparams.UpdateBackupPolicyParams{
			AccountName:        accountName,
			DailyBackupLimit:   nillable.GetInt64Ptr(35), // Higher than retention period of 30 days
			WeeklyBackupLimit:  nillable.GetInt64Ptr(10), // 10 weeks = 70 days > 30 days retention
			MonthlyBackupLimit: nillable.GetInt64Ptr(24), // 24 months = 720 days > 30 days retention
		}
	}

	// Helper function to create backup vault
	createBackupVault := func(id, state string, retentionDuration *int64, dailyImmutable, weeklyImmutable, monthlyImmutable bool) *coremodels.BackupVaultV1beta {
		return &coremodels.BackupVaultV1beta{
			BackupVaultID:  id,
			LifeCycleState: state,
			BackupRetentionPolicy: coremodels.BackupRetentionPolicyparams{
				BackupMinimumEnforcedRetentionDuration: retentionDuration,
				IsDailyBackupImmutable:                 dailyImmutable,
				IsWeeklyBackupImmutable:                weeklyImmutable,
				IsMonthlyBackupImmutable:               monthlyImmutable,
			},
		}
	}

	t.Run("Success - No backup vaults associated", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.NoError(t, err)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Success - Single backup vault with valid immutable settings", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		retentionDays := int64(30)
		backupVault := createBackupVault("vault-1", coremodels.LifeCycleStateREADY, &retentionDays, true, false, false)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1"}).Return([]*coremodels.BackupVaultV1beta{backupVault}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.NoError(t, err)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Success - Multiple backup vaults with valid settings", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		retentionDays := int64(30)
		vault1 := createBackupVault("vault-1", coremodels.LifeCycleStateREADY, &retentionDays, true, false, false)
		vault2 := createBackupVault("vault-2", coremodels.LifeCycleStateREADY, &retentionDays, false, true, true)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1", "vault-2"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1", "vault-2"}).Return([]*coremodels.BackupVaultV1beta{vault1, vault2}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.NoError(t, err)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Success - Skip validation when backup vault has no immutable attributes", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		// Vault with zero retention duration (no immutable settings)
		zeroRetentionDays := int64(0)
		backupVault := createBackupVault("vault-1", coremodels.LifeCycleStateREADY, &zeroRetentionDays, false, false, false)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1"}).Return([]*coremodels.BackupVaultV1beta{backupVault}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.NoError(t, err)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Success - Skip validation when backup vault has nil retention duration", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		// Vault with nil retention duration
		backupVault := createBackupVault("vault-1", coremodels.LifeCycleStateREADY, nil, false, false, false)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1"}).Return([]*coremodels.BackupVaultV1beta{backupVault}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.NoError(t, err)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Error - GetBackupVaultUUIDsFromBackupPolicyUUID fails", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		expectedErr := fmt.Errorf("database connection error")
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{}, expectedErr)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get backup vault UUIDs from backup policy")
		assert.Contains(t, err.Error(), "database connection error")
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Error - GetMultipleBackupVaults fails", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		expectedErr := fmt.Errorf("vault retrieval error")
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1"}).Return(nil, expectedErr)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get multiple backup vaults for validation")
		assert.Contains(t, err.Error(), "vault retrieval error")
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Error - Backup vault in updating state", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		retentionDays := int64(30)
		backupVault := createBackupVault("vault-1", coremodels.LifeCycleStateUpdating, &retentionDays, true, false, false)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1"}).Return([]*coremodels.BackupVaultV1beta{backupVault}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.Error(t, err)
		var customErr *coreerrors.CustomError
		assert.True(t, coreerrors.As(err, &customErr))
		assert.Equal(t, coreerrors.ErrImmutableValidationWithUpdatingBackupVault, customErr.TrackingID)
		assert.Contains(t, err.Error(), "Immutable backup vault is being updated")
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Error - Backup policy validation fails", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		// Create params with daily backup limit that violates immutable settings
		updateParams := &commonparams.UpdateBackupPolicyParams{
			AccountName:      "account-1",
			DailyBackupLimit: nillable.GetInt64Ptr(5), // Low value (5 days) which is less than vault retention (30 days)
		}

		retentionDays := int64(30)
		backupVault := createBackupVault("vault-1", coremodels.LifeCycleStateREADY, &retentionDays, true, false, false)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1"}).Return([]*coremodels.BackupVaultV1beta{backupVault}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "backup vault 'vault-1' validation failed")
		assert.Contains(t, err.Error(), "daily backup retention")
		assert.Contains(t, err.Error(), "is less than backup vault immutable period")
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Error - Multiple backup vaults with one in updating state", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		retentionDays := int64(30)
		vault1 := createBackupVault("vault-1", coremodels.LifeCycleStateREADY, &retentionDays, true, false, false)
		vault2 := createBackupVault("vault-2", coremodels.LifeCycleStateUpdating, &retentionDays, false, true, true)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1", "vault-2"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1", "vault-2"}).Return([]*coremodels.BackupVaultV1beta{vault1, vault2}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Immutable backup vault is being updated")
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Error - Multiple backup vaults with validation failure on second vault", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		// Create params that will pass validation for first vault but fail for second
		updateParams := &commonparams.UpdateBackupPolicyParams{
			AccountName:        "account-1",
			DailyBackupLimit:   nillable.GetInt64Ptr(35), // Valid for first vault
			WeeklyBackupLimit:  nillable.GetInt64Ptr(2),  // Invalid: 2 weeks * 7 days = 14 days < 30 days retention
			MonthlyBackupLimit: nillable.GetInt64Ptr(12),
		}

		retentionDays := int64(30)
		// First vault has no weekly immutable, so will pass validation
		vault1 := createBackupVault("vault-1", coremodels.LifeCycleStateREADY, &retentionDays, true, false, false)
		// Second vault has weekly immutable, so weekly limit will be validated and should fail
		vault2 := createBackupVault("vault-2", coremodels.LifeCycleStateREADY, &retentionDays, false, true, false)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1", "vault-2"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1", "vault-2"}).Return([]*coremodels.BackupVaultV1beta{vault1, vault2}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "backup vault 'vault-2' validation failed")
		assert.Contains(t, err.Error(), "weekly backup retention")
		mockOrchestrator.AssertExpectations(t)
	})

	// Null pointer handling tests
	t.Run("Null pointer handling - Nil backup policy", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		updateParams := createUpdateParams("account-1")

		// This should not cause panic, but should handle gracefully
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Function panicked with nil backup policy: %v", r)
			}
		}()

		err := _performBackupVaultValidation(ctx, nil, updateParams, mockOrchestrator)
		// We expect either an error or panic - both are acceptable for nil inputs
		if err == nil {
			t.Error("Expected an error when backup policy is nil")
		}
	})

	t.Run("Null pointer handling - Nil update params", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")

		// This should not cause panic, but should handle gracefully
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Function panicked with nil update params: %v", r)
			}
		}()

		err := _performBackupVaultValidation(ctx, backupPolicy, nil, mockOrchestrator)
		// We expect either an error or panic - both are acceptable for nil inputs
		if err == nil {
			t.Error("Expected an error when update params are nil")
		}
	})

	t.Run("Edge case - Empty backup policy UUID", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("")
		updateParams := createUpdateParams("account-1")

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "", "account-1").Return([]string{}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.NoError(t, err)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Edge case - Empty account name", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("")

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "").Return([]string{}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.NoError(t, err)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Edge case - Empty backup vault UUID returned", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		// Return empty string in the UUID list
		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{""}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{""}).Return([]*coremodels.BackupVaultV1beta{}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.NoError(t, err)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Edge case - Backup vault with empty backup vault ID", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		retentionDays := int64(30)
		backupVault := createBackupVault("", coremodels.LifeCycleStateUpdating, &retentionDays, true, false, false)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1"}).Return([]*coremodels.BackupVaultV1beta{backupVault}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Immutable backup vault is being updated")
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Edge case - Negative retention duration", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		negativeRetentionDays := int64(-10)
		backupVault := createBackupVault("vault-1", coremodels.LifeCycleStateREADY, &negativeRetentionDays, false, false, false)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1"}).Return([]*coremodels.BackupVaultV1beta{backupVault}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.NoError(t, err) // Should skip validation for negative values
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Mixed scenario - Some vaults with immutable settings, some without", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("policy-1")
		updateParams := createUpdateParams("account-1")

		retentionDays := int64(30)
		zeroRetentionDays := int64(0)

		// Vault with immutable settings
		vault1 := createBackupVault("vault-1", coremodels.LifeCycleStateREADY, &retentionDays, true, false, false)
		// Vault without immutable settings (zero retention)
		vault2 := createBackupVault("vault-2", coremodels.LifeCycleStateREADY, &zeroRetentionDays, false, false, false)
		// Vault without immutable settings (nil retention)
		vault3 := createBackupVault("vault-3", coremodels.LifeCycleStateREADY, nil, false, false, false)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "policy-1", "account-1").Return([]string{"vault-1", "vault-2", "vault-3"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"vault-1", "vault-2", "vault-3"}).Return([]*coremodels.BackupVaultV1beta{vault1, vault2, vault3}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.NoError(t, err) // Should pass - only vault-1 is validated, vault-2 and vault-3 are skipped
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Customer journey - Large scale validation", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("enterprise-policy-1")
		// Update params for enterprise-scale with high retention requirements
		updateParams := &commonparams.UpdateBackupPolicyParams{
			AccountName:        "enterprise-account",
			DailyBackupLimit:   nillable.GetInt64Ptr(400), // 400 days > 365 days retention
			WeeklyBackupLimit:  nillable.GetInt64Ptr(60),  // 60 weeks = 420 days > 365 days retention
			MonthlyBackupLimit: nillable.GetInt64Ptr(15),  // 15 months = 450 days > 365 days retention
		}

		retentionDays := int64(365) // 1 year retention

		// Create multiple vaults representing different business units
		vaults := make([]*coremodels.BackupVaultV1beta, 10)
		vaultUUIDs := make([]string, 10)

		for i := 0; i < 10; i++ {
			vaultID := fmt.Sprintf("bu-%d-vault", i+1)
			vaultUUIDs[i] = vaultID
			vaults[i] = createBackupVault(vaultID, coremodels.LifeCycleStateREADY, &retentionDays, true, i%2 == 0, i%3 == 0)
		}

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "enterprise-policy-1", "enterprise-account").Return(vaultUUIDs, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, vaultUUIDs).Return(vaults, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.NoError(t, err)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Customer journey - Migration scenario with different lifecycle states", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		backupPolicy := createBackupPolicy("migration-policy")
		// Use higher backup limits for 90-day retention period
		updateParams := &commonparams.UpdateBackupPolicyParams{
			AccountName:        "migration-account",
			DailyBackupLimit:   nillable.GetInt64Ptr(95), // 95 days > 90 days retention
			WeeklyBackupLimit:  nillable.GetInt64Ptr(15), // 15 weeks = 105 days > 90 days retention
			MonthlyBackupLimit: nillable.GetInt64Ptr(4),  // 4 months = 120 days > 90 days retention
		}

		retentionDays := int64(90)

		// Simulate different lifecycle states during migration
		vault1 := createBackupVault("old-vault", coremodels.LifeCycleStateREADY, &retentionDays, true, false, false)
		vault2 := createBackupVault("new-vault", coremodels.LifeCycleStateCreating, &retentionDays, false, true, true)
		vault3 := createBackupVault("temp-vault", coremodels.LifeCycleStateUpdating, &retentionDays, true, true, false)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "migration-policy", "migration-account").Return([]string{"old-vault", "new-vault", "temp-vault"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"old-vault", "new-vault", "temp-vault"}).Return([]*coremodels.BackupVaultV1beta{vault1, vault2, vault3}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Immutable backup vault is being updated")
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("Customer journey - Disaster recovery scenario with backup policy changes", func(t *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)

		// Simulate disaster recovery where backup policy limits need to be increased
		backupPolicy := &coremodels.BackupPolicy{
			BackupPolicyUUID:   "dr-policy",
			DailyBackupLimit:   7, // Current limits
			WeeklyBackupLimit:  4,
			MonthlyBackupLimit: 12,
		}

		// Disaster recovery requires increased backup frequency
		updateParams := &commonparams.UpdateBackupPolicyParams{
			AccountName:        "dr-account",
			DailyBackupLimit:   nillable.GetInt64Ptr(400), // 400 days > 365 days retention
			WeeklyBackupLimit:  nillable.GetInt64Ptr(55),  // 55 weeks = 385 days > 365 days retention
			MonthlyBackupLimit: nillable.GetInt64Ptr(15),  // 15 months = 450 days > 365 days retention
		}

		longRetentionDays := int64(365) // 1 year for compliance (more realistic)

		// Critical production vault with strict immutability
		vault1 := createBackupVault("prod-critical-vault", coremodels.LifeCycleStateREADY, &longRetentionDays, true, true, true)
		// Secondary vault with different immutable settings
		vault2 := createBackupVault("prod-secondary-vault", coremodels.LifeCycleStateREADY, &longRetentionDays, true, false, false)

		mockOrchestrator.On("GetBackupVaultUUIDsFromBackupPolicyUUID",
			ctx, "dr-policy", "dr-account").Return([]string{"prod-critical-vault", "prod-secondary-vault"}, nil)
		mockOrchestrator.On("GetMultipleBackupVaults",
			ctx, []string{"prod-critical-vault", "prod-secondary-vault"}).Return([]*coremodels.BackupVaultV1beta{vault1, vault2}, nil)

		err := _performBackupVaultValidation(ctx, backupPolicy, updateParams, mockOrchestrator)

		// Should pass validation as the backup limits are within acceptable ranges for long retention
		assert.NoError(t, err)
		mockOrchestrator.AssertExpectations(t)
	})
}
