package api

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestV1CreateExpertModeVolume(t *testing.T) {
	t.Run("SuccessWithSvmUUID", func(t *testing.T) {
		// Setup
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolUUID := "550e8400-e29b-41d4-a716-446655440000"
		volumeName := "my-expert-volume"
		svmUUID := "660e8400-e29b-41d4-a716-446655440001"
		sizeInBytes := 1099511627776.0
		projectNumber := "123456789"

		req := newExpertModeVolumeRequest(poolUUID, oasgenserver.ExpertModeVolumeV1ActionCreate, volumeName, sizeInBytes, oasgenserver.ExpertModeVolumeV1StyleFlexvol, &svmUUID, nil, projectNumber)
		params := oasgenserver.V1CreateExpertModeVolumeParams{}

		expectedParams := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    poolUUID,
			Action:      string(oasgenserver.ExpertModeVolumeV1ActionCreate),
			VolumeName:  volumeName,
			SizeInBytes: int64(sizeInBytes),
			Style:       string(oasgenserver.ExpertModeVolumeV1StyleFlexvol),
			SvmUuid:     svmUUID,
			SvmName:     "",
			AccountName: projectNumber,
		}

		// Set up expectations
		mockOrch.EXPECT().CreateExpertModeVolume(mock.Anything, expectedParams).Return(nil)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateExpertModeVolume(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &oasgenserver.V1CreateExpertModeVolumeOK{}, result)

		// Verify mock expectations
		mockOrch.AssertExpectations(t)
	})

	t.Run("SuccessWithSvmName", func(t *testing.T) {
		// Setup
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolUUID := "550e8400-e29b-41d4-a716-446655440000"
		volumeName := "my-expert-volume"
		svmName := "my-svm"
		sizeInBytes := 1099511627776.0
		projectNumber := "123456789"

		req := newExpertModeVolumeRequest(poolUUID, oasgenserver.ExpertModeVolumeV1ActionCreate, volumeName, sizeInBytes, oasgenserver.ExpertModeVolumeV1StyleFlexgroup, nil, &svmName, projectNumber)
		params := oasgenserver.V1CreateExpertModeVolumeParams{}

		expectedParams := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    poolUUID,
			Action:      string(oasgenserver.ExpertModeVolumeV1ActionCreate),
			VolumeName:  volumeName,
			SizeInBytes: int64(sizeInBytes),
			Style:       string(oasgenserver.ExpertModeVolumeV1StyleFlexgroup),
			SvmUuid:     "",
			SvmName:     svmName,
			AccountName: projectNumber,
		}

		// Set up expectations
		mockOrch.EXPECT().CreateExpertModeVolume(mock.Anything, expectedParams).Return(nil)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateExpertModeVolume(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &oasgenserver.V1CreateExpertModeVolumeOK{}, result)

		// Verify mock expectations
		mockOrch.AssertExpectations(t)
	})

	t.Run("SuccessWithCorrelationID", func(t *testing.T) {
		// Setup
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolUUID := "550e8400-e29b-41d4-a716-446655440000"
		volumeName := "my-expert-volume"
		svmUUID := "660e8400-e29b-41d4-a716-446655440001"
		sizeInBytes := 1099511627776.0
		correlationID := "test-correlation-id-12345"
		projectNumber := "123456789"

		req := newExpertModeVolumeRequest(poolUUID, oasgenserver.ExpertModeVolumeV1ActionCreate, volumeName, sizeInBytes, oasgenserver.ExpertModeVolumeV1StyleFlexvol, &svmUUID, nil, projectNumber)
		params := oasgenserver.V1CreateExpertModeVolumeParams{}

		expectedParams := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    poolUUID,
			Action:      string(oasgenserver.ExpertModeVolumeV1ActionCreate),
			VolumeName:  volumeName,
			SizeInBytes: int64(sizeInBytes),
			Style:       string(oasgenserver.ExpertModeVolumeV1StyleFlexvol),
			SvmUuid:     svmUUID,
			SvmName:     "",
			AccountName: projectNumber,
		}

		// Set up expectations
		mockOrch.EXPECT().CreateExpertModeVolume(mock.Anything, expectedParams).Return(nil)

		// Execute with correlation ID in context
		ctx := context.WithValue(context.Background(), "X-Correlation-ID", correlationID)
		result, err := handler.V1CreateExpertModeVolume(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &oasgenserver.V1CreateExpertModeVolumeOK{}, result)

		// Verify mock expectations
		mockOrch.AssertExpectations(t)
	})

	t.Run("BadRequestError_SvmNotFound", func(t *testing.T) {
		// Setup
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolUUID := "550e8400-e29b-41d4-a716-446655440000"
		volumeName := "my-expert-volume"
		svmUUID := "non-existent-svm-uuid"
		sizeInBytes := 1099511627776.0
		projectNumber := "123456789"

		req := newExpertModeVolumeRequest(poolUUID, oasgenserver.ExpertModeVolumeV1ActionCreate, volumeName, sizeInBytes, oasgenserver.ExpertModeVolumeV1StyleFlexvol, &svmUUID, nil, projectNumber)
		params := oasgenserver.V1CreateExpertModeVolumeParams{}

		expectedParams := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    poolUUID,
			Action:      string(oasgenserver.ExpertModeVolumeV1ActionCreate),
			VolumeName:  volumeName,
			SizeInBytes: int64(sizeInBytes),
			Style:       string(oasgenserver.ExpertModeVolumeV1StyleFlexvol),
			SvmUuid:     svmUUID,
			SvmName:     "",
			AccountName: projectNumber,
		}

		// Set up expectations - orchestrator returns bad request error
		svmNotFoundErr := customerrors.NewBadRequestErr("SVM with UUID 'non-existent-svm-uuid' not found in pool")
		mockOrch.EXPECT().CreateExpertModeVolume(mock.Anything, expectedParams).Return(svmNotFoundErr)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateExpertModeVolume(ctx, req, params)

		// Assert
		assert.NoError(t, err) // Handler converts error to response
		assert.NotNil(t, result)

		badRequest, ok := result.(*oasgenserver.V1CreateExpertModeVolumeBadRequest)
		assert.True(t, ok)
		assert.Equal(t, float64(http.StatusBadRequest), badRequest.Code)
		assert.Contains(t, badRequest.Message, "SVM with UUID")
		assert.Contains(t, badRequest.Message, "not found")

		// Verify mock expectations
		mockOrch.AssertExpectations(t)
	})

	t.Run("BadRequestError_InsufficientCapacity", func(t *testing.T) {
		// Setup
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolUUID := "550e8400-e29b-41d4-a716-446655440000"
		volumeName := "my-expert-volume"
		svmUUID := "660e8400-e29b-41d4-a716-446655440001"
		sizeInBytes := 1099511627776.0
		projectNumber := "123456789"

		req := newExpertModeVolumeRequest(poolUUID, oasgenserver.ExpertModeVolumeV1ActionCreate, volumeName, sizeInBytes, oasgenserver.ExpertModeVolumeV1StyleFlexvol, &svmUUID, nil, projectNumber)
		params := oasgenserver.V1CreateExpertModeVolumeParams{}

		expectedParams := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    poolUUID,
			Action:      string(oasgenserver.ExpertModeVolumeV1ActionCreate),
			VolumeName:  volumeName,
			SizeInBytes: int64(sizeInBytes),
			Style:       string(oasgenserver.ExpertModeVolumeV1StyleFlexvol),
			SvmUuid:     svmUUID,
			SvmName:     "",
			AccountName: projectNumber,
		}

		// Set up expectations - orchestrator returns bad request error
		insufficientCapacityErr := customerrors.NewBadRequestErr("insufficient pool capacity: requested 1099511627776 bytes, available 500000000000 bytes")
		mockOrch.EXPECT().CreateExpertModeVolume(mock.Anything, expectedParams).Return(insufficientCapacityErr)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateExpertModeVolume(ctx, req, params)

		// Assert
		assert.NoError(t, err) // Handler converts error to response
		assert.NotNil(t, result)

		badRequest, ok := result.(*oasgenserver.V1CreateExpertModeVolumeBadRequest)
		assert.True(t, ok)
		assert.Equal(t, float64(http.StatusBadRequest), badRequest.Code)
		assert.Contains(t, badRequest.Message, "insufficient pool capacity")

		// Verify mock expectations
		mockOrch.AssertExpectations(t)
	})

	t.Run("BadRequestError_MissingSvmIdentifier", func(t *testing.T) {
		// Setup
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data - neither svmUUID nor svmName provided
		poolUUID := "550e8400-e29b-41d4-a716-446655440000"
		volumeName := "my-expert-volume"
		sizeInBytes := 1099511627776.0
		projectNumber := "123456789"

		req := newExpertModeVolumeRequest(poolUUID, oasgenserver.ExpertModeVolumeV1ActionCreate, volumeName, sizeInBytes, oasgenserver.ExpertModeVolumeV1StyleFlexvol, nil, nil, projectNumber)
		params := oasgenserver.V1CreateExpertModeVolumeParams{}

		expectedParams := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    poolUUID,
			Action:      string(oasgenserver.ExpertModeVolumeV1ActionCreate),
			VolumeName:  volumeName,
			SizeInBytes: int64(sizeInBytes),
			Style:       string(oasgenserver.ExpertModeVolumeV1StyleFlexvol),
			SvmUuid:     "",
			SvmName:     "",
			AccountName: projectNumber,
		}

		// Set up expectations - orchestrator returns bad request error
		missingSvmErr := customerrors.NewBadRequestErr("neither svmName nor svmUUID has been passed")
		mockOrch.EXPECT().CreateExpertModeVolume(mock.Anything, expectedParams).Return(missingSvmErr)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateExpertModeVolume(ctx, req, params)

		// Assert
		assert.NoError(t, err) // Handler converts error to response
		assert.NotNil(t, result)

		badRequest, ok := result.(*oasgenserver.V1CreateExpertModeVolumeBadRequest)
		assert.True(t, ok)
		assert.Equal(t, float64(http.StatusBadRequest), badRequest.Code)
		assert.Contains(t, badRequest.Message, "neither svmName nor svmUUID")

		// Verify mock expectations
		mockOrch.AssertExpectations(t)
	})

	t.Run("InternalServerError_PoolNotFound", func(t *testing.T) {
		// Setup
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolUUID := "non-existent-pool-uuid"
		volumeName := "my-expert-volume"
		svmUUID := "660e8400-e29b-41d4-a716-446655440001"
		sizeInBytes := 1099511627776.0
		projectNumber := "123456789"

		req := newExpertModeVolumeRequest(poolUUID, oasgenserver.ExpertModeVolumeV1ActionCreate, volumeName, sizeInBytes, oasgenserver.ExpertModeVolumeV1StyleFlexvol, &svmUUID, nil, projectNumber)
		params := oasgenserver.V1CreateExpertModeVolumeParams{}

		expectedParams := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    poolUUID,
			Action:      string(oasgenserver.ExpertModeVolumeV1ActionCreate),
			VolumeName:  volumeName,
			SizeInBytes: int64(sizeInBytes),
			Style:       string(oasgenserver.ExpertModeVolumeV1StyleFlexvol),
			SvmUuid:     svmUUID,
			SvmName:     "",
			AccountName: projectNumber,
		}

		// Set up expectations - orchestrator returns generic error (not BadRequest)
		poolNotFoundErr := errors.New("pool not found")
		mockOrch.EXPECT().CreateExpertModeVolume(mock.Anything, expectedParams).Return(poolNotFoundErr)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateExpertModeVolume(ctx, req, params)

		// Assert
		assert.NoError(t, err) // Handler converts error to response
		assert.NotNil(t, result)

		internalError, ok := result.(*oasgenserver.V1CreateExpertModeVolumeInternalServerError)
		assert.True(t, ok)
		assert.Equal(t, float64(http.StatusInternalServerError), internalError.Code)
		assert.Equal(t, "pool not found", internalError.Message)

		// Verify mock expectations
		mockOrch.AssertExpectations(t)
	})

	t.Run("InternalServerError_DatabaseError", func(t *testing.T) {
		// Setup
		mockOrch := orchestrator.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Test data
		poolUUID := "550e8400-e29b-41d4-a716-446655440000"
		volumeName := "my-expert-volume"
		svmUUID := "660e8400-e29b-41d4-a716-446655440001"
		sizeInBytes := 1099511627776.0
		projectNumber := "123456789"

		req := newExpertModeVolumeRequest(poolUUID, oasgenserver.ExpertModeVolumeV1ActionCreate, volumeName, sizeInBytes, oasgenserver.ExpertModeVolumeV1StyleFlexvol, &svmUUID, nil, projectNumber)
		params := oasgenserver.V1CreateExpertModeVolumeParams{}

		expectedParams := &commonparams.CreateExpertModeVolumeParams{
			PoolUUID:    poolUUID,
			Action:      string(oasgenserver.ExpertModeVolumeV1ActionCreate),
			VolumeName:  volumeName,
			SizeInBytes: int64(sizeInBytes),
			Style:       string(oasgenserver.ExpertModeVolumeV1StyleFlexvol),
			SvmUuid:     svmUUID,
			SvmName:     "",
			AccountName: projectNumber,
		}

		// Set up expectations - orchestrator returns database error
		databaseErr := errors.New("database connection failed")
		mockOrch.EXPECT().CreateExpertModeVolume(mock.Anything, expectedParams).Return(databaseErr)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateExpertModeVolume(ctx, req, params)

		// Assert
		assert.NoError(t, err) // Handler converts error to response
		assert.NotNil(t, result)

		internalError, ok := result.(*oasgenserver.V1CreateExpertModeVolumeInternalServerError)
		assert.True(t, ok)
		assert.Equal(t, float64(http.StatusInternalServerError), internalError.Code)
		assert.Equal(t, "database connection failed", internalError.Message)

		// Verify mock expectations
		mockOrch.AssertExpectations(t)
	})
}

// Helper function to create expert mode volume requests
func newExpertModeVolumeRequest(poolUUID string, action oasgenserver.ExpertModeVolumeV1Action, name string, sizeInBytes float64, style oasgenserver.ExpertModeVolumeV1Style, svmUUID *string, svmName *string, projectNumber string) *oasgenserver.ExpertModeVolumeV1 {
	req := &oasgenserver.ExpertModeVolumeV1{
		PoolUUID:      poolUUID,
		Action:        action,
		VolumeName:    name,
		SizeInBytes:   sizeInBytes,
		Style:         style,
		SvmUuid:       oasgenserver.OptString{},
		SvmName:       oasgenserver.OptString{},
		ProjectNumber: projectNumber,
	}

	if svmUUID != nil {
		req.SvmUuid = oasgenserver.NewOptString(*svmUUID)
	}

	if svmName != nil {
		req.SvmName = oasgenserver.NewOptString(*svmName)
	}

	return req
}
