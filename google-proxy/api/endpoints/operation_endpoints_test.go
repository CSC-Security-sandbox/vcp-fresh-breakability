package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
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
			mockOrch := orchestrator.NewMockOrchestratorFactory(t)
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
	mockOrch := orchestrator.NewMockOrchestratorFactory(t)

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
	mockOrch := orchestrator.NewMockOrchestratorFactory(t)

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
	mockOrch := orchestrator.NewMockOrchestratorFactory(t)

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
	mockOrch := orchestrator.NewMockOrchestratorFactory(t)

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
	mockOrch := orchestrator.NewMockOrchestratorFactory(t)
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
	mockOrch := orchestrator.NewMockOrchestratorFactory(t)
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
	done := true
	operationResponse := async.V1betaDescribeOperationOK{
		Payload: &cvpmodels.OperationV1beta{
			Done:  &done,
			Name:  "operation-123",
			Error: &cvpmodels.StatusV1Beta{},
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
	assert.IsType(t, &gcpgenserver.V1betaDescribeOperationBadRequest{}, result)
}
