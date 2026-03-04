package api

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestV1betaDescribeOperation_BadRequest(t *testing.T) {
	handler := Handler{}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "test-project",
		LocationId:    "invalid-location",
		OperationId:   "op-123",
	}

	// Simulate invalid location to trigger BadRequest
	result, err := handler.V1betaDescribeOperation(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	badRequest, ok := result.(*gcpgenserver.V1betaDescribeOperationBadRequest)
	assert.True(t, ok)
	assert.Equal(t, float64(400), badRequest.Code)
}

func TestV1betaDescribeOperation_LabelerMissing(t *testing.T) {
	handler := Handler{}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "test-project",
		LocationId:    "valid-location",
		OperationId:   "op-123",
	}
	// No labeler in context
	_, err := handler.V1betaDescribeOperation(context.Background(), params)
	assert.NoError(t, err)
	// Further assertions as needed
}

func TestV1betaDescribeOperationReturnsBadRequestForInvalidLocation(t *testing.T) {
	handler := Handler{}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "test-project",
		LocationId:    "invalid-location",
		OperationId:   "op-123",
	}

	result, err := handler.V1betaDescribeOperation(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	badRequest, ok := result.(*gcpgenserver.V1betaDescribeOperationBadRequest)
	assert.True(t, ok)
	assert.Equal(t, float64(400), badRequest.Code)
}

func TestV1betaDescribeOperationReturnsBadRequestForInvalidOperationId(t *testing.T) {
	handler := Handler{}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "test-project",
		LocationId:    "valid-location",
		OperationId:   "invalid-op-id",
	}

	result, err := handler.V1betaDescribeOperation(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	badRequest, ok := result.(*gcpgenserver.V1betaDescribeOperationBadRequest)
	assert.True(t, ok)
	assert.Equal(t, float64(400), badRequest.Code)
}
func TestV1betaDescribeOperation_CVPErrorCases(t *testing.T) {
	type cvpErrCase struct {
		name     string
		err      error
		expected any
	}
	cases := []cvpErrCase{
		{
			name: "UnprocessableEntity",
			err: &async.V1betaDescribeOperationUnprocessableEntity{
				Payload: &cvpmodels.Error{Code: 422, Message: "unprocessable"},
			},
			expected: &gcpgenserver.V1betaDescribeOperationUnprocessableEntity{},
		},
		{
			name: "TooManyRequests",
			err: &async.V1betaDescribeOperationTooManyRequests{
				Payload: &cvpmodels.Error{Code: 429, Message: "too many"},
			},
			expected: &gcpgenserver.V1betaDescribeOperationTooManyRequests{},
		},
		{
			name: "BadRequest",
			err: &async.V1betaDescribeOperationBadRequest{
				Payload: &cvpmodels.Error{Code: 400, Message: "bad request"},
			},
			expected: &gcpgenserver.V1betaDescribeOperationBadRequest{},
		},
		{
			name: "Unauthorized",
			err: &async.V1betaDescribeOperationUnauthorized{
				Payload: &cvpmodels.Error{Code: 401, Message: "unauthorized"},
			},
			expected: &gcpgenserver.V1betaDescribeOperationUnauthorized{},
		},
		{
			name: "Forbidden",
			err: &async.V1betaDescribeOperationForbidden{
				Payload: &cvpmodels.Error{Code: 403, Message: "forbidden"},
			},
			expected: &gcpgenserver.V1betaDescribeOperationForbidden{},
		},
		{
			name: "NotFound",
			err: &async.V1betaDescribeOperationNotFound{
				Payload: &cvpmodels.Error{Code: 404, Message: "not found"},
			},
			expected: &gcpgenserver.V1betaDescribeOperationNotFound{},
		},
		{
			name: "InternalServerError",
			err: &async.V1betaDescribeOperationInternalServerError{
				Payload: &cvpmodels.Error{Code: 500, Message: "internal"},
			},
			expected: &gcpgenserver.V1betaDescribeOperationInternalServerError{},
		},
	}
	for _, c := range cases {
		t.Run("CVPError_"+c.name, func(t *testing.T) {
			ctx := context.Background()
			logger := &log.MockLogger{}
			ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
			mockOrch := factory.NewMockOrchestratorFactory(t)
			mockOrch.On("GetJob", ctx, mock.Anything).Return(nil, nil)
			originalCreateClient := createClient
			originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
			defer func() {
				createClient = originalCreateClient
				utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			}()
			utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
				return "us-east4", "us-east4", nil
			}
			mockAsync := &async.MockClientService{}
			mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, c.err)
			mockCVP := &cvpapi.Cvp{Async: mockAsync}
			createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
			handler := Handler{Orchestrator: mockOrch}
			params := gcpgenserver.V1betaDescribeOperationParams{
				ProjectNumber: "proj",
				LocationId:    "valid-location",
				OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
			}
			result, err := handler.V1betaDescribeOperation(ctx, params)
			assert.NoError(t, err)
			assert.IsType(t, c.expected, result)
		})
	}
}
func TestReturnsBadRequestForJobStateError(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}
	mockAsync := &async.MockClientService{}
	job := &models.Job{
		State:      models.JobsStateERROR,
		TrackingID: 1123,
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, nil)
	mockCVP := &cvpapi.Cvp{Async: mockAsync}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}
	result, err := handler.V1betaDescribeOperation(ctx, params)
	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
}
func TestReturnsOperationForJobStateNew(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}
	mockAsync := &async.MockClientService{}
	job := &models.Job{
		State:      models.JobsStateNEW,
		TrackingID: 1123,
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, nil)
	mockCVP := &cvpapi.Cvp{Async: mockAsync}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}
	result, err := handler.V1betaDescribeOperation(ctx, params)
	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
}
func TestReturnsOperationForJobStateProcessing(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}
	mockAsync := &async.MockClientService{}
	job := &models.Job{
		State:      models.JobsStatePROCESSING,
		TrackingID: 1123,
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, nil)
	mockCVP := &cvpapi.Cvp{Async: mockAsync}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}
	result, err := handler.V1betaDescribeOperation(ctx, params)
	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
}
func TestReturnsOperationForJobStateDone(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}
	mockAsync := &async.MockClientService{}
	job := &models.Job{
		State:      models.JobsStateDONE,
		TrackingID: 1123,
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, nil)
	mockCVP := &cvpapi.Cvp{Async: mockAsync}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}
	result, err := handler.V1betaDescribeOperation(ctx, params)
	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
}
func TestV1betaDescribeOperationError(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)
	mockOrch.On("GetJob", ctx, mock.Anything).Return(nil, nil)
	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}
	mockAsync := &async.MockClientService{}
	mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, nil)
	mockCVP := &cvpapi.Cvp{Async: mockAsync}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}
	result, err := handler.V1betaDescribeOperation(ctx, params)
	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.V1betaDescribeOperationInternalServerError{}, result)
}
func TestV1betaDescribeOperationSuccess(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)
	mockOrch.On("GetJob", ctx, mock.Anything).Return(nil, nil)
	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}
	done := false
	operationResponse := async.V1betaDescribeOperationOK{
		Payload: &cvpmodels.OperationV1beta{
			Done: &done,
			Name: "/v1beta/projects/proj/locations/valid-location/operations/b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
			Response: map[string]interface{}{
				"backupId":         "04c5b0da-5810-09c6-e45c-b863a0054b17",
				"backupType":       "MANUAL",
				"backupVaultId":    "0c325851-2f75-2e29-ef1e-edaa151c6d1f",
				"created":          "0001-01-01T00:00:00.000Z",
				"description":      "",
				"resourceId":       "adhoc-backup-tcase-30618-1908251257121",
				"satisfiesPzi":     false,
				"satisfiesPzs":     false,
				"state":            "CREATING",
				"volumeId":         "5859f5d5-2194-985e-2e8c-0278ef1d667e",
				"volumeUsageBytes": 0,
			},
		},
	}
	mockAsync := &async.MockClientService{}
	mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(&operationResponse, nil)
	mockCVP := &cvpapi.Cvp{Async: mockAsync}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}
	result, err := handler.V1betaDescribeOperation(ctx, params)
	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)

	// Verify the response contains the expected data
	operationResult := result.(*gcpgenserver.OperationV1beta)
	assert.Equal(t, false, operationResult.Done.Value)
	assert.Equal(t, "/v1beta/projects/proj/locations/valid-location/operations/b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a", operationResult.Name.Value)
	assert.NotEmpty(t, operationResult.Response)

	// Verify specific fields in the response
	var responseData map[string]interface{}
	err = json.Unmarshal(operationResult.Response, &responseData)
	assert.NoError(t, err)

	assert.Equal(t, "04c5b0da-5810-09c6-e45c-b863a0054b17", responseData["backupId"])
	assert.Equal(t, "MANUAL", responseData["backupType"])
	assert.Equal(t, "0c325851-2f75-2e29-ef1e-edaa151c6d1f", responseData["backupVaultId"])
	assert.Equal(t, "0001-01-01T00:00:00.000Z", responseData["created"])
	assert.Equal(t, "", responseData["description"])
	assert.Equal(t, "adhoc-backup-tcase-30618-1908251257121", responseData["resourceId"])
	assert.Equal(t, false, responseData["satisfiesPzi"])
	assert.Equal(t, false, responseData["satisfiesPzs"])
	assert.Equal(t, "CREATING", responseData["state"])
	assert.Equal(t, "5859f5d5-2194-985e-2e8c-0278ef1d667e", responseData["volumeId"])
	assert.Equal(t, float64(0), responseData["volumeUsageBytes"])
}

func TestReturnsOperationForJobStateWaitForTemporal(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}
	mockAsync := &async.MockClientService{}
	job := &models.Job{
		State:      models.JobsStateWaitForTemporal,
		TrackingID: 1123,
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockCVP := &cvpapi.Cvp{Async: mockAsync}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}
	result, err := handler.V1betaDescribeOperation(ctx, params)
	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	operationResult := result.(*gcpgenserver.OperationV1beta)
	assert.Equal(t, false, operationResult.Done.Value)
	// unmarshal response
	out := ""
	err = json.Unmarshal(operationResult.Response, &out)
	assert.NoError(t, err)
	assert.Equal(t, "Job is still new", out)
}

func TestReturnsOperationForJobStateErrorWithRestoreVolumeValidation(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}
	mockAsync := &async.MockClientService{}
	job := &models.Job{
		State:        models.JobsStateERROR,
		TrackingID:   7008, // vsaerrors.ErrRestoreVolumeValidation
		ErrorDetails: []byte("Custom restore volume validation error message"),
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, nil)
	mockCVP := &cvpapi.Cvp{Async: mockAsync}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}
	result, err := handler.V1betaDescribeOperation(ctx, params)
	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)

	// Verify the response contains the custom error message from ErrorDetails
	operationResult := result.(*gcpgenserver.OperationV1beta)
	assert.True(t, operationResult.Done.Value)
	assert.Equal(t, "Custom restore volume validation error message", operationResult.Error.Value.Message.Value)
}

func TestReturnsOperationForJobStateErrorWithSnapshotNotAllowedForVolume(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}
	mockAsync := &async.MockClientService{}
	job := &models.Job{
		State:        models.JobsStateERROR,
		TrackingID:   vsaerrors.ErrSnapshotNotAllowedForVolume,
		ErrorDetails: []byte("snapshot creation operation not allowed for this volume"),
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockAsync.EXPECT().V1betaDescribeOperation(mock.Anything).Return(nil, nil)
	mockCVP := &cvpapi.Cvp{Async: mockAsync}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}
	result, err := handler.V1betaDescribeOperation(ctx, params)
	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)

	// Verify the response contains the custom error message from ErrorDetails
	operationResult := result.(*gcpgenserver.OperationV1beta)
	assert.True(t, operationResult.Done.Value)
	assert.Equal(t, float64(400), operationResult.Error.Value.Code.Value)
	assert.Equal(t, "snapshot creation operation not allowed for this volume", operationResult.Error.Value.Message.Value)
}

// Tests for V1betaDescribeInternalOperation

func TestV1betaInternalDescribeOperation_BadRequest(t *testing.T) {
	handler := Handler{}
	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber: "test-project",
		LocationId:    "invalid-location",
		OperationId:   "op-123",
	}

	result, err := handler.V1betaInternalDescribeOperation(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	badRequest, ok := result.(*gcpgenserver.V1betaInternalDescribeOperationBadRequest)
	assert.True(t, ok)
	assert.Equal(t, float64(400), badRequest.Code)
}

func TestV1betaInternalDescribeOperation_VCPJobSuccess_NEW(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}

	// Mock job with NEW state
	job := &models.Job{
		TrackingID: 2001,
		State:      models.JobsStateNEW,
	}

	mockOrch.On("GetJob", mock.Anything, mock.Anything).Return(job, nil)
	handler := Handler{Orchestrator: mockOrch}

	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber: "test-project",
		LocationId:    "us-central1",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}

	result, err := handler.V1betaInternalDescribeOperation(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)

	internalOp, ok := result.(*gcpgenserver.InternalOperationV1beta)
	assert.True(t, ok)
	assert.Equal(t, 2001, internalOp.TrackingId.Value)
	assert.False(t, internalOp.Done.Value) // NEW state should not be done
	assert.Contains(t, internalOp.Name.Value, "internal/operations/b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a")
}

func TestV1betaInternalDescribeOperation_VCPJobSuccess_DONE(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}

	job := &models.Job{
		TrackingID: 2002,
		State:      models.JobsStateDONE,
	}

	operationId := "ba2c8826-2627-057c-42ba-343ee7ab1ebe"
	mockOrch.On("GetJob", ctx, operationId).Return(job, nil)
	handler := Handler{Orchestrator: mockOrch}

	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber: "test-project",
		LocationId:    "us-central1",
		OperationId:   operationId,
	}

	result, err := handler.V1betaInternalDescribeOperation(ctx, params)

	assert.NoError(t, err)
	internalOp := result.(*gcpgenserver.InternalOperationV1beta)
	assert.Equal(t, 2002, internalOp.TrackingId.Value)
	assert.True(t, internalOp.Done.Value) // DONE state should be done
}

func TestV1betaInternalDescribeOperation_VCPJobSuccess_ERROR(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}

	job := &models.Job{
		TrackingID: 4001,
		State:      models.JobsStateERROR,
	}

	operationId := "ba2c8826-2627-057c-42ba-343ee7ab1ebe"
	mockOrch.On("GetJob", ctx, operationId).Return(job, nil)
	handler := Handler{Orchestrator: mockOrch}

	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber: "test-project",
		LocationId:    "us-central1",
		OperationId:   operationId,
	}

	result, err := handler.V1betaInternalDescribeOperation(ctx, params)

	assert.NoError(t, err)
	internalOp := result.(*gcpgenserver.InternalOperationV1beta)
	assert.Equal(t, 4001, internalOp.TrackingId.Value)
	assert.True(t, internalOp.Done.Value) // ERROR state should be done
	assert.True(t, internalOp.Error.Set)  // Should have error set
}

func TestV1betaInternalDescribeOperation_Success(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	handler := &Handler{
		Orchestrator: mockOrchestrator,
	}

	// Add location validation mock like other tests
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}

	operationId := uuid.New().String()
	job := &models.Job{
		BaseModel: models.BaseModel{
			UUID: operationId,
		},
		TrackingID: 2001,
		State:      models.JobsStateDONE,
	}

	mockOrchestrator.EXPECT().GetJob(mock.Anything, operationId).Return(job, nil)

	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber:  "123456789",
		LocationId:     "us-central1",
		OperationId:    operationId,
		XCorrelationID: gcpgenserver.NewOptString("test-correlation"),
	}

	result, err := handler.V1betaInternalDescribeOperation(context.Background(), params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.InternalOperationV1beta{}, result)

	operation := result.(*gcpgenserver.InternalOperationV1beta)
	assert.True(t, operation.Done.Value)
	assert.Equal(t, 2001, operation.TrackingId.Value)
}

func TestV1betaInternalDescribeOperation_JobInProgress(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)

	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}

	handler := &Handler{
		Orchestrator: mockOrchestrator,
	}

	operationId := uuid.New().String()
	job := &models.Job{
		BaseModel: models.BaseModel{
			UUID: operationId,
		},
		CorrelationID: "test-correlation-id",
		TrackingID:    0,
		State:         models.JobsStatePROCESSING,
		WorkflowID:    "CreateVolumeWorkflow_test",
	}

	mockOrchestrator.On("GetJob", ctx, operationId).Return(job, nil)

	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber:  "123456789",
		LocationId:     "us-central1",
		OperationId:    operationId,
		XCorrelationID: gcpgenserver.NewOptString("test-correlation"),
	}

	result, err := handler.V1betaInternalDescribeOperation(ctx, params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.InternalOperationV1beta{}, result)

	operation := result.(*gcpgenserver.InternalOperationV1beta)
	assert.False(t, operation.Done.Value)
	assert.Equal(t, 0, operation.TrackingId.Value)
}

func TestV1betaInternalDescribeOperation_JobError(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)

	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}

	handler := &Handler{
		Orchestrator: mockOrchestrator,
	}

	operationId := uuid.New().String()
	job := &models.Job{
		BaseModel: models.BaseModel{
			UUID: operationId,
		},
		CorrelationID: "test-correlation-id",
		TrackingID:    vsaerrors.ErrDatabaseConnectionClosed,
		State:         models.JobsStateERROR,
		WorkflowID:    "CreateVolumeWorkflow_test",
		ErrorDetails:  []byte("Database connection was closed unexpectedly"),
	}

	mockOrchestrator.On("GetJob", ctx, operationId).Return(job, nil)

	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber:  "123456789",
		LocationId:     "us-central1",
		OperationId:    operationId,
		XCorrelationID: gcpgenserver.NewOptString("test-correlation"),
	}

	result, err := handler.V1betaInternalDescribeOperation(ctx, params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.InternalOperationV1beta{}, result)

	operation := result.(*gcpgenserver.InternalOperationV1beta)
	assert.True(t, operation.Done.Value)
	assert.Equal(t, vsaerrors.ErrDatabaseConnectionClosed, operation.TrackingId.Value)
	assert.True(t, operation.Error.Set)
	assert.NotEmpty(t, operation.Error.Value.Message.Value)
}

func TestV1betaInternalDescribeOperation_RestoreVolumeValidationError(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)

	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}

	handler := &Handler{
		Orchestrator: mockOrchestrator,
	}

	operationId := uuid.New().String()
	customErrorDetails := "Cannot restore volume - size too small"
	job := &models.Job{
		BaseModel: models.BaseModel{
			UUID: operationId,
		},
		CorrelationID: "test-correlation-id",
		TrackingID:    vsaerrors.ErrRestoreVolumeValidation,
		State:         models.JobsStateERROR,
		WorkflowID:    "RestoreVolumeWorkflow_test",
		ErrorDetails:  []byte(customErrorDetails),
	}

	mockOrchestrator.On("GetJob", ctx, operationId).Return(job, nil)

	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber:  "123456789",
		LocationId:     "us-central1",
		OperationId:    operationId,
		XCorrelationID: gcpgenserver.NewOptString("test-correlation"),
	}

	result, err := handler.V1betaInternalDescribeOperation(ctx, params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.InternalOperationV1beta{}, result)

	operation := result.(*gcpgenserver.InternalOperationV1beta)
	assert.True(t, operation.Done.Value)
	assert.Equal(t, vsaerrors.ErrRestoreVolumeValidation, operation.TrackingId.Value)
	assert.True(t, operation.Error.Set)
	assert.Equal(t, customErrorDetails, operation.Error.Value.Message.Value)
}

func TestV1betaInternalDescribeOperation_SnapshotNotAllowedForVolumeError(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	handler := &Handler{
		Orchestrator: mockOrchestrator,
	}

	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)

	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}

	operationId := uuid.New().String()
	customErrorDetails := "snapshot creation operation not allowed for this volume"
	job := &models.Job{
		BaseModel: models.BaseModel{
			UUID: operationId,
		},
		CorrelationID: "test-correlation-id",
		TrackingID:    vsaerrors.ErrSnapshotNotAllowedForVolume,
		State:         models.JobsStateERROR,
		WorkflowID:    "CreateSnapshotWorkflow_test",
		ErrorDetails:  []byte(customErrorDetails),
	}

	mockOrchestrator.On("GetJob", ctx, operationId).Return(job, nil)

	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber:  "123456789",
		LocationId:     "us-central1",
		OperationId:    operationId,
		XCorrelationID: gcpgenserver.NewOptString("test-correlation"),
	}

	result, err := handler.V1betaInternalDescribeOperation(ctx, params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.InternalOperationV1beta{}, result)

	operation := result.(*gcpgenserver.InternalOperationV1beta)
	assert.True(t, operation.Done.Value)
	assert.Equal(t, vsaerrors.ErrSnapshotNotAllowedForVolume, operation.TrackingId.Value)
	assert.True(t, operation.Error.Set)
	assert.Equal(t, float64(400), operation.Error.Value.Code.Value)
	assert.Equal(t, customErrorDetails, operation.Error.Value.Message.Value)
}

func TestV1betaInternalDescribeOperation_InvalidOperationId(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	handler := &Handler{
		Orchestrator: mockOrchestrator,
	}

	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}

	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber:  "123456789",
		LocationId:     "us-central1",
		OperationId:    "invalid-uuid",
		XCorrelationID: gcpgenserver.NewOptString("test-correlation"),
	}

	result, err := handler.V1betaInternalDescribeOperation(context.Background(), params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.V1betaInternalDescribeOperationBadRequest{}, result)

	errorResponse := result.(*gcpgenserver.V1betaInternalDescribeOperationBadRequest)
	assert.Equal(t, float64(400), errorResponse.Code)
	assert.Contains(t, errorResponse.Message, "invalid UUID")
}

func TestV1betaInternalDescribeOperation_InvalidLocation(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	handler := &Handler{
		Orchestrator: mockOrchestrator,
	}

	operationId := uuid.New().String()

	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber:  "123456789",
		LocationId:     "invalid-location",
		OperationId:    operationId,
		XCorrelationID: gcpgenserver.NewOptString("test-correlation"),
	}

	result, err := handler.V1betaInternalDescribeOperation(context.Background(), params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.V1betaInternalDescribeOperationBadRequest{}, result)

	errorResponse := result.(*gcpgenserver.V1betaInternalDescribeOperationBadRequest)
	assert.Equal(t, float64(400), errorResponse.Code)
}

func TestV1betaInternalDescribeOperation_DatabaseError(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	handler := &Handler{
		Orchestrator: mockOrchestrator,
	}

	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}

	operationId := uuid.New().String()
	dbErr := assert.AnError

	mockOrchestrator.EXPECT().GetJob(mock.Anything, operationId).Return(nil, dbErr)

	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber:  "123456789",
		LocationId:     "us-central1",
		OperationId:    operationId,
		XCorrelationID: gcpgenserver.NewOptString("test-correlation"),
	}

	result, err := handler.V1betaInternalDescribeOperation(context.Background(), params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.V1betaInternalDescribeOperationInternalServerError{}, result)

	errorResponse := result.(*gcpgenserver.V1betaInternalDescribeOperationInternalServerError)
	assert.Equal(t, float64(500), errorResponse.Code)
}

func TestV1betaInternalDescribeOperation_JobStatesMapping(t *testing.T) {
	testCases := []struct {
		name         string
		jobState     models.JobState
		expectedDone bool
	}{
		{"New Job", models.JobsStateNEW, false},
		{"Processing Job", models.JobsStatePROCESSING, false},
		{"Waiting for Temporal", models.JobsStateWaitForTemporal, false},
		{"Done Job", models.JobsStateDONE, true},
		{"Error Job", models.JobsStateERROR, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockOrchestrator := factory.NewMockOrchestratorFactory(t)
			handler := &Handler{
				Orchestrator: mockOrchestrator,
			}

			originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
			defer func() {
				utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
			}()
			utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
				return "us-central1", "us-central1-a", nil
			}

			operationId := uuid.New().String()
			job := &models.Job{
				BaseModel: models.BaseModel{
					UUID: operationId,
				},
				CorrelationID: "test-correlation-id",
				TrackingID:    0,
				State:         tc.jobState,
				WorkflowID:    "TestWorkflow",
			}

			mockOrchestrator.EXPECT().GetJob(mock.Anything, operationId).Return(job, nil)

			params := gcpgenserver.V1betaInternalDescribeOperationParams{
				ProjectNumber:  "123456789",
				LocationId:     "us-central1",
				OperationId:    operationId,
				XCorrelationID: gcpgenserver.NewOptString("test-correlation"),
			}

			result, err := handler.V1betaInternalDescribeOperation(context.Background(), params)

			assert.NoError(t, err)
			assert.IsType(t, &gcpgenserver.InternalOperationV1beta{}, result)

			operation := result.(*gcpgenserver.InternalOperationV1beta)
			assert.Equal(t, tc.expectedDone, operation.Done.Value)
			// Only verify trackingId since other fields were removed
		})
	}
}

func TestV1betaDescribeOperation_ResumeVolumeReplication_QuotaRuleFailure(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}

	errorDetails := "Operation was successful but quota rule sync between source and destination failed"
	job := &models.Job{
		State:        models.JobsStateDONE,
		Type:         models.JobTypeResumeVolumeReplication,
		ErrorDetails: []byte(errorDetails),
		TrackingID:   0,
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockCVP := &cvpapi.Cvp{Async: &async.MockClientService{}}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}

	result, err := handler.V1betaDescribeOperation(ctx, params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	operationResult := result.(*gcpgenserver.OperationV1beta)
	assert.True(t, operationResult.Done.Value)
	assert.True(t, operationResult.Metadata.Set)
	assert.Equal(t, "status_message", operationResult.Metadata.Value.Type)

	var metadataValue string
	err = json.Unmarshal(operationResult.Metadata.Value.AnyValue, &metadataValue)
	assert.NoError(t, err)
	assert.Contains(t, metadataValue, errorDetails)
}

func TestV1betaDescribeOperation_ReverseResumeVolumeReplication_QuotaRuleFailure(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}

	errorDetails := "Operation was successful but quota rule sync between source and destination failed"
	job := &models.Job{
		State:        models.JobsStateDONE,
		Type:         models.JobTypeReverseResumeVolumeReplication,
		ErrorDetails: []byte(errorDetails),
		TrackingID:   0,
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockCVP := &cvpapi.Cvp{Async: &async.MockClientService{}}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}

	result, err := handler.V1betaDescribeOperation(ctx, params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	operationResult := result.(*gcpgenserver.OperationV1beta)
	assert.True(t, operationResult.Done.Value)
	assert.True(t, operationResult.Metadata.Set)
	assert.Equal(t, "status_message", operationResult.Metadata.Value.Type)

	var metadataValue string
	err = json.Unmarshal(operationResult.Metadata.Value.AnyValue, &metadataValue)
	assert.NoError(t, err)
	assert.Contains(t, metadataValue, errorDetails)
}

// TestV1betaDescribeOperation_StopVolumeReplication_QuotaRuleFailure tests that the public describe
// handler returns Metadata for the main stop job type when it has quota failure error details.
func TestV1betaDescribeOperation_StopVolumeReplication_QuotaRuleFailure(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}

	errorDetails := "Break operation is successful and destination volume has become RW, but post break quota rule creation operation failed"
	job := &models.Job{
		State:        models.JobsStateDONE,
		Type:         models.JobTypeStopVolumeReplication, // Main stop job type (not internal)
		ErrorDetails: []byte(errorDetails),
		TrackingID:   0,
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockCVP := &cvpapi.Cvp{Async: &async.MockClientService{}}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}

	result, err := handler.V1betaDescribeOperation(ctx, params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	operationResult := result.(*gcpgenserver.OperationV1beta)
	assert.True(t, operationResult.Done.Value)
	assert.True(t, operationResult.Metadata.Set)
	assert.Equal(t, "status_message", operationResult.Metadata.Value.Type)

	var metadataValue string
	err = json.Unmarshal(operationResult.Metadata.Value.AnyValue, &metadataValue)
	assert.NoError(t, err)
	assert.Contains(t, metadataValue, errorDetails)
}

// TestV1betaInternalDescribeOperation_StopInternal_QuotaRuleFailure tests that the internal describe
// handler returns Error (not just Done) for internal stop job type with quota failure.
// This allows the DescribeJob activity to treat it as a failure and propagate to main workflow.
func TestV1betaInternalDescribeOperation_StopInternal_QuotaRuleFailure(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}

	errorDetails := "Break operation is successful and destination volume has become RW, but post break quota rule creation operation failed"
	job := &models.Job{
		State:        models.JobsStateDONE,
		Type:         models.JobTypeStopVolumeReplicationInternal,
		ErrorDetails: []byte(errorDetails),
		TrackingID:   0,
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaInternalDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}

	result, err := handler.V1betaInternalDescribeOperation(ctx, params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.InternalOperationV1beta{}, result)
	operationResult := result.(*gcpgenserver.InternalOperationV1beta)
	assert.True(t, operationResult.Done.Value, "Done should be true")
	assert.True(t, operationResult.Error.Set, "Error should be set for quota failure")
	assert.Equal(t, float64(200), operationResult.Error.Value.Code.Value, "Code should be 200 for partial success")
	assert.Contains(t, operationResult.Error.Value.Message.Value, errorDetails, "Error message should contain quota failure details")
}

func TestV1betaDescribeOperation_Done_NoQuotaRuleFailure(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}

	job := &models.Job{
		State:        models.JobsStateDONE,
		Type:         models.JobTypeResumeVolumeReplication,
		ErrorDetails: []byte(""),
		TrackingID:   0,
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockCVP := &cvpapi.Cvp{Async: &async.MockClientService{}}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}

	result, err := handler.V1betaDescribeOperation(ctx, params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	operationResult := result.(*gcpgenserver.OperationV1beta)
	assert.True(t, operationResult.Done.Value)
	assert.False(t, operationResult.Metadata.Set)
}

func TestV1betaDescribeOperation_InternalResume_NoMetadata(t *testing.T) {
	ctx := context.Background()
	logger := &log.MockLogger{}
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	mockOrch := factory.NewMockOrchestratorFactory(t)

	originalCreateClient := createClient
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		createClient = originalCreateClient
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-east4", "us-east4", nil
	}

	// Internal resume should NOT trigger metadata even with quota error
	errorDetails := "Operation was successful but quota rule sync between source and destination failed"
	job := &models.Job{
		State:        models.JobsStateDONE,
		Type:         models.JobTypeResumeVolumeReplicationInternal,
		ErrorDetails: []byte(errorDetails),
		TrackingID:   0,
	}
	mockOrch.On("GetJob", ctx, mock.Anything).Return(job, nil)
	mockCVP := &cvpapi.Cvp{Async: &async.MockClientService{}}
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *mockCVP }
	handler := Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "proj",
		LocationId:    "valid-location",
		OperationId:   "b3b8c7e2-8c2a-4e2a-9b1a-2e4b6c8d9f0a",
	}

	result, err := handler.V1betaDescribeOperation(ctx, params)

	assert.NoError(t, err)
	assert.IsType(t, &gcpgenserver.OperationV1beta{}, result)
	operationResult := result.(*gcpgenserver.OperationV1beta)
	assert.True(t, operationResult.Done.Value)
	// Internal resume should NOT have metadata set
	assert.False(t, operationResult.Metadata.Set)
}
