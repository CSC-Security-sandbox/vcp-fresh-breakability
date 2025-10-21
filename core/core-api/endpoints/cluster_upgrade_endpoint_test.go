package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	orchestratorMocks "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	utilsErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// Test V1UpgradeCluster function
func TestV1UpgradeCluster_Success(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test request
	req := &oasgenserver.ClusterUpgradeRequestV1{
		VsaBuildImage:      oasgenserver.NewOptString("vsa-image:latest"),
		MediatorBuildImage: oasgenserver.NewOptString("mediator-image:latest"),
		ForceUpgrade:       oasgenserver.NewOptBool(true),
		Metadata:           oasgenserver.NewOptClusterUpgradeRequestV1Metadata(map[string]string{"key": "value"}),
	}

	// Create test parameters
	params := oasgenserver.V1UpgradeClusterParams{
		ClusterId: "test-cluster-id",
	}

	// Create expected orchestrator parameters
	expectedParams := &commonparams.UpgradeClusterParams{
		ClusterID:          "test-cluster-id",
		VSABuildImage:      "vsa-image:latest",
		MediatorBuildImage: "mediator-image:latest",
		ForceUpgrade:       true,
		Metadata:           map[string]string{"key": "value"},
	}

	// Create mock response
	mockResponse := &models.ClusterUpgradeResponse{
		ClusterID: "test-cluster-id",
		Status:    models.UpgradeStatusPending,
		JobID:     "test-job-id",
		CreatedAt: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	// Set up mock expectation
	mockOrchestrator.On("UpgradeCluster", mock.Anything, expectedParams).Return(mockResponse, "test-job-id", nil)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1UpgradeCluster(ctx, req, params)

	// Assert success
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to success response
	successResponse, ok := result.(*oasgenserver.ClusterUpgradeResponseV1)
	assert.True(t, ok)
	assert.Equal(t, "test-cluster-id", successResponse.ClusterId)
	assert.Equal(t, oasgenserver.ClusterUpgradeResponseV1StatusPENDING, successResponse.Status)
	assert.Equal(t, "test-job-id", successResponse.JobId)
	assert.Equal(t, time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC), successResponse.CreatedAt)
	assert.Equal(t, time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC), successResponse.UpdatedAt)

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1UpgradeCluster_NotFoundError(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test request
	req := &oasgenserver.ClusterUpgradeRequestV1{
		VsaBuildImage:      oasgenserver.NewOptString("vsa-image:latest"),
		MediatorBuildImage: oasgenserver.NewOptString("mediator-image:latest"),
		ForceUpgrade:       oasgenserver.NewOptBool(true),
		Metadata:           oasgenserver.NewOptClusterUpgradeRequestV1Metadata(map[string]string{"key": "value"}),
	}

	// Create test parameters
	params := oasgenserver.V1UpgradeClusterParams{
		ClusterId: "test-cluster-id",
	}

	// Create expected orchestrator parameters
	expectedParams := &commonparams.UpgradeClusterParams{
		ClusterID:          "test-cluster-id",
		VSABuildImage:      "vsa-image:latest",
		MediatorBuildImage: "mediator-image:latest",
		ForceUpgrade:       true,
		Metadata:           map[string]string{"key": "value"},
	}

	// Set up mock to return not found error
	clusterId := params.ClusterId
	mockError := utilsErrors.NewNotFoundErr("Cluster", &clusterId)
	mockOrchestrator.On("UpgradeCluster", mock.Anything, expectedParams).Return(nil, "", mockError)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1UpgradeCluster(ctx, req, params)

	// Assert success with not found error response
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to not found response
	notFoundResponse, ok := result.(*oasgenserver.V1UpgradeClusterNotFound)
	assert.True(t, ok)
	assert.Equal(t, float64(404), notFoundResponse.Code)
	assert.Contains(t, notFoundResponse.Message, "Cluster not found: test-cluster-id")

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1UpgradeCluster_BadRequestError(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test request
	req := &oasgenserver.ClusterUpgradeRequestV1{
		VsaBuildImage:      oasgenserver.NewOptString("vsa-image:latest"),
		MediatorBuildImage: oasgenserver.NewOptString("mediator-image:latest"),
		ForceUpgrade:       oasgenserver.NewOptBool(true),
		Metadata:           oasgenserver.NewOptClusterUpgradeRequestV1Metadata(map[string]string{"key": "value"}),
	}

	// Create test parameters
	params := oasgenserver.V1UpgradeClusterParams{
		ClusterId: "test-cluster-id",
	}

	// Create expected orchestrator parameters
	expectedParams := &commonparams.UpgradeClusterParams{
		ClusterID:          "test-cluster-id",
		VSABuildImage:      "vsa-image:latest",
		MediatorBuildImage: "mediator-image:latest",
		ForceUpgrade:       true,
		Metadata:           map[string]string{"key": "value"},
	}

	// Set up mock to return bad request error
	mockError := errors.New("bad request")
	mockOrchestrator.On("UpgradeCluster", mock.Anything, expectedParams).Return(nil, "", mockError)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1UpgradeCluster(ctx, req, params)

	// Assert success with bad request error response
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to bad request response
	badRequestResponse, ok := result.(*oasgenserver.V1UpgradeClusterBadRequest)
	assert.True(t, ok)
	assert.Equal(t, float64(400), badRequestResponse.Code)
	assert.Contains(t, badRequestResponse.Message, "Invalid request")

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1UpgradeCluster_ConflictError(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test request
	req := &oasgenserver.ClusterUpgradeRequestV1{
		VsaBuildImage:      oasgenserver.NewOptString("vsa-image:latest"),
		MediatorBuildImage: oasgenserver.NewOptString("mediator-image:latest"),
		ForceUpgrade:       oasgenserver.NewOptBool(true),
		Metadata:           oasgenserver.NewOptClusterUpgradeRequestV1Metadata(map[string]string{"key": "value"}),
	}

	// Create test parameters
	params := oasgenserver.V1UpgradeClusterParams{
		ClusterId: "test-cluster-id",
	}

	// Create expected orchestrator parameters
	expectedParams := &commonparams.UpgradeClusterParams{
		ClusterID:          "test-cluster-id",
		VSABuildImage:      "vsa-image:latest",
		MediatorBuildImage: "mediator-image:latest",
		ForceUpgrade:       true,
		Metadata:           map[string]string{"key": "value"},
	}

	// Set up mock to return conflict error
	mockError := errors.New("conflict")
	mockOrchestrator.On("UpgradeCluster", mock.Anything, expectedParams).Return(nil, "", mockError)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1UpgradeCluster(ctx, req, params)

	// Assert success with conflict error response
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to conflict response
	conflictResponse, ok := result.(*oasgenserver.V1UpgradeClusterConflict)
	assert.True(t, ok)
	assert.Equal(t, float64(409), conflictResponse.Code)
	assert.Equal(t, "conflict", conflictResponse.Message)

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1UpgradeCluster_ForbiddenError(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test request
	req := &oasgenserver.ClusterUpgradeRequestV1{
		VsaBuildImage:      oasgenserver.NewOptString("vsa-image:latest"),
		MediatorBuildImage: oasgenserver.NewOptString("mediator-image:latest"),
		ForceUpgrade:       oasgenserver.NewOptBool(true),
		Metadata:           oasgenserver.NewOptClusterUpgradeRequestV1Metadata(map[string]string{"key": "value"}),
	}

	// Create test parameters
	params := oasgenserver.V1UpgradeClusterParams{
		ClusterId: "test-cluster-id",
	}

	// Create expected orchestrator parameters
	expectedParams := &commonparams.UpgradeClusterParams{
		ClusterID:          "test-cluster-id",
		VSABuildImage:      "vsa-image:latest",
		MediatorBuildImage: "mediator-image:latest",
		ForceUpgrade:       true,
		Metadata:           map[string]string{"key": "value"},
	}

	// Set up mock to return forbidden error
	mockError := errors.New("forbidden")
	mockOrchestrator.On("UpgradeCluster", mock.Anything, expectedParams).Return(nil, "", mockError)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1UpgradeCluster(ctx, req, params)

	// Assert success with forbidden error response
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to forbidden response
	forbiddenResponse, ok := result.(*oasgenserver.V1UpgradeClusterForbidden)
	assert.True(t, ok)
	assert.Equal(t, float64(403), forbiddenResponse.Code)
	assert.Equal(t, "forbidden", forbiddenResponse.Message)

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1UpgradeCluster_ServiceUnavailableError(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test request
	req := &oasgenserver.ClusterUpgradeRequestV1{
		VsaBuildImage:      oasgenserver.NewOptString("vsa-image:latest"),
		MediatorBuildImage: oasgenserver.NewOptString("mediator-image:latest"),
		ForceUpgrade:       oasgenserver.NewOptBool(true),
		Metadata:           oasgenserver.NewOptClusterUpgradeRequestV1Metadata(map[string]string{"key": "value"}),
	}

	// Create test parameters
	params := oasgenserver.V1UpgradeClusterParams{
		ClusterId: "test-cluster-id",
	}

	// Create expected orchestrator parameters
	expectedParams := &commonparams.UpgradeClusterParams{
		ClusterID:          "test-cluster-id",
		VSABuildImage:      "vsa-image:latest",
		MediatorBuildImage: "mediator-image:latest",
		ForceUpgrade:       true,
		Metadata:           map[string]string{"key": "value"},
	}

	// Set up mock to return service unavailable error
	mockError := errors.New("Failed to retrieve cluster information")
	mockOrchestrator.On("UpgradeCluster", mock.Anything, expectedParams).Return(nil, "", mockError)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1UpgradeCluster(ctx, req, params)

	// Assert success with service unavailable error response
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to service unavailable response
	serviceUnavailableResponse, ok := result.(*oasgenserver.V1UpgradeClusterInternalServerError)
	assert.True(t, ok)
	assert.Equal(t, float64(503), serviceUnavailableResponse.Code)
	assert.Contains(t, serviceUnavailableResponse.Message, "Service temporarily unavailable")

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1UpgradeCluster_InternalServerError(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test request
	req := &oasgenserver.ClusterUpgradeRequestV1{
		VsaBuildImage:      oasgenserver.NewOptString("vsa-image:latest"),
		MediatorBuildImage: oasgenserver.NewOptString("mediator-image:latest"),
		ForceUpgrade:       oasgenserver.NewOptBool(true),
		Metadata:           oasgenserver.NewOptClusterUpgradeRequestV1Metadata(map[string]string{"key": "value"}),
	}

	// Create test parameters
	params := oasgenserver.V1UpgradeClusterParams{
		ClusterId: "test-cluster-id",
	}

	// Create expected orchestrator parameters
	expectedParams := &commonparams.UpgradeClusterParams{
		ClusterID:          "test-cluster-id",
		VSABuildImage:      "vsa-image:latest",
		MediatorBuildImage: "mediator-image:latest",
		ForceUpgrade:       true,
		Metadata:           map[string]string{"key": "value"},
	}

	// Set up mock to return generic error
	mockError := errors.New("generic error")
	mockOrchestrator.On("UpgradeCluster", mock.Anything, expectedParams).Return(nil, "", mockError)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1UpgradeCluster(ctx, req, params)

	// Assert success with internal server error response
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to internal server error response
	internalServerErrorResponse, ok := result.(*oasgenserver.V1UpgradeClusterInternalServerError)
	assert.True(t, ok)
	assert.Equal(t, float64(500), internalServerErrorResponse.Code)
	assert.Contains(t, internalServerErrorResponse.Message, "Upgrade operation failed")

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

// Test V1GetClusterUpgradeStatus function
func TestV1GetClusterUpgradeStatus_Success(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test parameters
	params := oasgenserver.V1GetClusterUpgradeStatusParams{
		JobId: "test-job-id",
	}

	// Create mock response
	startTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	endTime := time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)
	mockProgress := &models.UpgradeProgress{
		JobID:  "test-job-id",
		Status: models.UpgradeStatusInProgress,
		Clusters: []models.ClusterUpgradeStatus{
			{
				ClusterID:   "test-cluster-id",
				Status:      "IN_PROGRESS",
				StartTime:   &startTime,
				EndTime:     &endTime,
				CurrentStep: "upgrading",
			},
		},
		Errors: []models.UpgradeError{
			{
				Code:      "UPGRADE_ERROR",
				Message:   "test error",
				Type:      "validation",
				Retryable: true,
				ClusterID: "test-cluster-id",
			},
		},
		Warnings: []string{"test warning"},
	}

	// Set up mock expectation
	mockOrchestrator.On("GetClusterUpgradeStatus", mock.Anything, "test-job-id").Return(mockProgress, nil)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1GetClusterUpgradeStatus(ctx, params)

	// Assert success
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to success response
	successResponse, ok := result.(*oasgenserver.UpgradeProgressV1)
	assert.True(t, ok)
	assert.Equal(t, "test-job-id", successResponse.JobId)
	assert.Equal(t, oasgenserver.UpgradeProgressV1StatusINPROGRESS, successResponse.Status)
	assert.Len(t, successResponse.Clusters, 1)
	assert.Len(t, successResponse.Errors, 1)
	assert.Len(t, successResponse.Warnings, 1)

	// Verify cluster status
	cluster := successResponse.Clusters[0]
	assert.Equal(t, "test-cluster-id", cluster.ClusterId)
	assert.Equal(t, oasgenserver.ClusterUpgradeStatusV1StatusINPROGRESS, cluster.Status)
	assert.True(t, cluster.StartTime.Set)
	assert.Equal(t, startTime, cluster.StartTime.Value)
	assert.True(t, cluster.EndTime.Set)
	assert.Equal(t, endTime, cluster.EndTime.Value)
	assert.True(t, cluster.CurrentStep.Set)
	assert.Equal(t, "upgrading", cluster.CurrentStep.Value)

	// Verify error
	error := successResponse.Errors[0]
	assert.Equal(t, "UPGRADE_ERROR", error.Code)
	assert.Equal(t, "test error", error.Message)
	assert.Equal(t, "validation", error.Type)
	assert.True(t, error.Retryable)
	assert.True(t, error.ClusterId.Set)
	assert.Equal(t, "test-cluster-id", error.ClusterId.Value)

	// Verify warning
	assert.Equal(t, "test warning", successResponse.Warnings[0])

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1GetClusterUpgradeStatus_NotFoundError(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test parameters
	params := oasgenserver.V1GetClusterUpgradeStatusParams{
		JobId: "test-job-id",
	}

	// Set up mock to return not found error
	mockError := errors.New("record not found")
	mockOrchestrator.On("GetClusterUpgradeStatus", mock.Anything, "test-job-id").Return(nil, mockError)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1GetClusterUpgradeStatus(ctx, params)

	// Assert success with not found error response
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to not found response
	notFoundResponse, ok := result.(*oasgenserver.V1GetClusterUpgradeStatusNotFound)
	assert.True(t, ok)
	assert.Equal(t, float64(404), notFoundResponse.Code)
	assert.Contains(t, notFoundResponse.Message, "Upgrade job not found: test-job-id")

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1GetClusterUpgradeStatus_BadRequestError(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test parameters
	params := oasgenserver.V1GetClusterUpgradeStatusParams{
		JobId: "test-job-id",
	}

	// Set up mock to return bad request error
	mockError := errors.New("bad request")
	mockOrchestrator.On("GetClusterUpgradeStatus", mock.Anything, "test-job-id").Return(nil, mockError)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1GetClusterUpgradeStatus(ctx, params)

	// Assert success with bad request error response
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to bad request response
	badRequestResponse, ok := result.(*oasgenserver.V1GetClusterUpgradeStatusBadRequest)
	assert.True(t, ok)
	assert.Equal(t, float64(400), badRequestResponse.Code)
	assert.Contains(t, badRequestResponse.Message, "Invalid request")

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1GetClusterUpgradeStatus_InternalServerError(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test parameters
	params := oasgenserver.V1GetClusterUpgradeStatusParams{
		JobId: "test-job-id",
	}

	// Set up mock to return generic error
	mockError := errors.New("generic error")
	mockOrchestrator.On("GetClusterUpgradeStatus", mock.Anything, "test-job-id").Return(nil, mockError)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1GetClusterUpgradeStatus(ctx, params)

	// Assert success with internal server error response
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to internal server error response
	internalServerErrorResponse, ok := result.(*oasgenserver.V1GetClusterUpgradeStatusInternalServerError)
	assert.True(t, ok)
	assert.Equal(t, float64(500), internalServerErrorResponse.Code)
	assert.Contains(t, internalServerErrorResponse.Message, "Failed to get upgrade status")

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

// Test convertClusterStatusesToAPI function
func TestConvertClusterStatusesToAPI(t *testing.T) {
	t.Run("Nil clusters", func(t *testing.T) {
		result := convertClusterStatusesToAPI(nil)
		assert.Nil(t, result)
	})

	t.Run("Empty clusters", func(t *testing.T) {
		result := convertClusterStatusesToAPI([]models.ClusterUpgradeStatus{})
		assert.NotNil(t, result)
		assert.Len(t, result, 0)
	})

	t.Run("Valid clusters", func(t *testing.T) {
		startTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
		endTime := time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)
		clusters := []models.ClusterUpgradeStatus{
			{
				ClusterID:   "cluster-1",
				Status:      "IN_PROGRESS",
				StartTime:   &startTime,
				EndTime:     &endTime,
				CurrentStep: "upgrading",
			},
			{
				ClusterID:   "cluster-2",
				Status:      "COMPLETED",
				StartTime:   nil,
				EndTime:     nil,
				CurrentStep: "",
			},
		}

		result := convertClusterStatusesToAPI(clusters)
		assert.NotNil(t, result)
		assert.Len(t, result, 2)

		// Verify first cluster
		cluster1 := result[0]
		assert.Equal(t, "cluster-1", cluster1.ClusterId)
		assert.Equal(t, oasgenserver.ClusterUpgradeStatusV1StatusINPROGRESS, cluster1.Status)
		assert.True(t, cluster1.StartTime.Set)
		assert.Equal(t, startTime, cluster1.StartTime.Value)
		assert.True(t, cluster1.EndTime.Set)
		assert.Equal(t, endTime, cluster1.EndTime.Value)
		assert.True(t, cluster1.CurrentStep.Set)
		assert.Equal(t, "upgrading", cluster1.CurrentStep.Value)

		// Verify second cluster
		cluster2 := result[1]
		assert.Equal(t, "cluster-2", cluster2.ClusterId)
		assert.Equal(t, oasgenserver.ClusterUpgradeStatusV1StatusCOMPLETED, cluster2.Status)
		assert.False(t, cluster2.StartTime.Set)
		assert.False(t, cluster2.EndTime.Set)
		assert.False(t, cluster2.CurrentStep.Set)
	})
}

// Test convertUpgradeErrorsToAPI function
func TestConvertUpgradeErrorsToAPI(t *testing.T) {
	t.Run("Nil errors", func(t *testing.T) {
		result := convertUpgradeErrorsToAPI(nil)
		assert.Nil(t, result)
	})

	t.Run("Empty errors", func(t *testing.T) {
		result := convertUpgradeErrorsToAPI([]models.UpgradeError{})
		assert.NotNil(t, result)
		assert.Len(t, result, 0)
	})

	t.Run("Valid errors", func(t *testing.T) {
		errors := []models.UpgradeError{
			{
				Code:      "ERROR_1",
				Message:   "First error",
				Type:      "validation",
				Retryable: true,
				ClusterID: "cluster-1",
			},
			{
				Code:      "ERROR_2",
				Message:   "Second error",
				Type:      "system",
				Retryable: false,
				ClusterID: "",
			},
		}

		result := convertUpgradeErrorsToAPI(errors)
		assert.NotNil(t, result)
		assert.Len(t, result, 2)

		// Verify first error
		error1 := result[0]
		assert.Equal(t, "ERROR_1", error1.Code)
		assert.Equal(t, "First error", error1.Message)
		assert.Equal(t, "validation", error1.Type)
		assert.True(t, error1.Retryable)
		assert.True(t, error1.ClusterId.Set)
		assert.Equal(t, "cluster-1", error1.ClusterId.Value)

		// Verify second error
		error2 := result[1]
		assert.Equal(t, "ERROR_2", error2.Code)
		assert.Equal(t, "Second error", error2.Message)
		assert.Equal(t, "system", error2.Type)
		assert.False(t, error2.Retryable)
		assert.False(t, error2.ClusterId.Set)
	})
}

// Test V1ListAvailableVersions function
func TestV1ListAvailableVersions_Success(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test parameters
	params := oasgenserver.V1ListAvailableVersionsParams{}

	// Create mock response
	mockResponse := &models.ListAvailableVersionsResponse{
		Versions: []models.AvailableVersion{
			{
				OntapVersion: "9.17.1",
				VSAImagePath: "gcr.io/vsa-image:9.17.1",
				VSAName:      "vsa-9.17.1",
				MediatorName: "mediator-9.17.1",
				IsCurrent:    true,
				IsActive:     true,
			},
			{
				OntapVersion: "9.16.1",
				VSAImagePath: "gcr.io/vsa-image:9.16.1",
				VSAName:      "vsa-9.16.1",
				MediatorName: "mediator-9.16.1",
				IsCurrent:    false,
				IsActive:     true,
			},
		},
		Current: "9.17.1",
	}

	// Set up mock expectation
	mockOrchestrator.On("ListAvailableVersions", mock.Anything).Return(mockResponse, nil)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1ListAvailableVersions(ctx, params)

	// Assert success
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to success response
	successResponse, ok := result.(*oasgenserver.ListAvailableVersionsResponseV1)
	assert.True(t, ok)
	assert.Len(t, successResponse.Versions, 2)
	assert.Equal(t, "9.17.1", successResponse.Current)

	// Verify first version
	version1 := successResponse.Versions[0]
	assert.Equal(t, "9.17.1", version1.OntapVersion)
	assert.Equal(t, "gcr.io/vsa-image:9.17.1", version1.VsaImagePath)
	assert.Equal(t, "vsa-9.17.1", version1.VsaName)
	assert.Equal(t, "mediator-9.17.1", version1.MediatorName)
	assert.True(t, version1.IsCurrent)
	assert.True(t, version1.IsActive)

	// Verify second version
	version2 := successResponse.Versions[1]
	assert.Equal(t, "9.16.1", version2.OntapVersion)
	assert.Equal(t, "gcr.io/vsa-image:9.16.1", version2.VsaImagePath)
	assert.Equal(t, "vsa-9.16.1", version2.VsaName)
	assert.Equal(t, "mediator-9.16.1", version2.MediatorName)
	assert.False(t, version2.IsCurrent)
	assert.True(t, version2.IsActive)

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1ListAvailableVersions_Error(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test parameters
	params := oasgenserver.V1ListAvailableVersionsParams{}

	// Set up mock to return error
	mockError := errors.New("database error")
	mockOrchestrator.On("ListAvailableVersions", mock.Anything).Return(nil, mockError)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1ListAvailableVersions(ctx, params)

	// Assert success with error response
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to internal server error response
	internalServerErrorResponse, ok := result.(*oasgenserver.V1ListAvailableVersionsInternalServerError)
	assert.True(t, ok)
	assert.Equal(t, float64(500), internalServerErrorResponse.Code)
	assert.Equal(t, "Failed to retrieve available versions", internalServerErrorResponse.Message)

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

// Test error handling patterns
func TestErrorHandlingPattern(t *testing.T) {
	t.Run("Not Found Error Detection", func(t *testing.T) {
		err := errors.New("record not found")

		// Test the string matching logic used in the handler
		assert.True(t, strings.Contains(err.Error(), "not found"))
		assert.True(t, strings.Contains(err.Error(), "record not found"))
	})

	t.Run("Bad Request Error Detection", func(t *testing.T) {
		err := errors.New("bad request")

		// Test the string matching logic used in the handler
		assert.True(t, strings.Contains(err.Error(), "bad request"))
		assert.False(t, strings.Contains(err.Error(), "invalid request"))
	})

	t.Run("Service Unavailable Error Detection", func(t *testing.T) {
		err := errors.New("Failed to retrieve cluster information")

		// Test the string matching logic used in the handler
		assert.False(t, strings.Contains(err.Error(), "unavailable"))
		assert.True(t, strings.Contains(err.Error(), "Failed to retrieve cluster information"))
	})

	t.Run("Conflict Error Detection", func(t *testing.T) {
		err := errors.New("conflict")

		// Test the string matching logic used in the handler
		assert.True(t, strings.Contains(err.Error(), "conflict"))
	})

	t.Run("Forbidden Error Detection", func(t *testing.T) {
		err := errors.New("forbidden")

		// Test the string matching logic used in the handler
		assert.True(t, strings.Contains(err.Error(), "forbidden"))
	})
}
