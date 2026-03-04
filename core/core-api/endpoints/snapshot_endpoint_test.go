package api

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestV1CreateSnapshot(t *testing.T) {
	t.Run("WhenSuccessfulWithSyncModeCompleted", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		// Test data
		projectNumber := "123456789"
		locationId := "us-east1"
		volumeId := "550e8400-e29b-41d4-a716-446655440000"
		snapshotName := "my-snapshot"
		description := "Test snapshot description"
		jobUUID := "job-uuid-123"

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: snapshotName,
		}
		req.Description = oasgenserver.NewOptString(description)
		req.IsAppConsistent = oasgenserver.NewOptBool(false)

		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: projectNumber,
			LocationId:    locationId,
			VolumeId:      volumeId,
		}

		expectedParams := &commonparams.CreateSnapshotParams{
			SnapshotBaseParams: commonparams.SnapshotBaseParams{
				AccountName: projectNumber,
				VolumeID:    volumeId,
			},
			Name:            snapshotName,
			Description:     description,
			IsAppConsistent: false,
		}

		// Mock snapshot response (sync mode completed - snapshot is READY)
		mockSnapshot := &coremodels.Snapshot{
			BaseModel: coremodels.BaseModel{
				UUID:      "snapshot-uuid-123",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			Name:                  snapshotName,
			Description:           description,
			VolumeUUID:            volumeId,
			VolumeName:            "my-volume",
			LifeCycleState:        coremodels.LifeCycleStateREADY,
			LifeCycleStateDetails: "Available for use",
			SizeInBytes:           1099511627776,
		}

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, expectedParams).Return(mockSnapshot, jobUUID, nil)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Should return OperationV1 with done=true (200 response)
		operationResponse, ok := result.(*oasgenserver.OperationV1)
		assert.True(t, ok, "Expected OperationV1 response when snapshot is READY")
		assert.True(t, operationResponse.Done.Or(false), "Operation should be done when snapshot is READY")
		expectedOperationID := "/v1beta/projects/" + projectNumber + "/locations/" + locationId + "/operations/" + jobUUID
		assert.Equal(t, expectedOperationID, operationResponse.Name.Or(""))

		// Verify the response contains the snapshot data
		assert.NotNil(t, operationResponse.Response)
		var snapshotData map[string]interface{}
		err = json.Unmarshal(operationResponse.Response, &snapshotData)
		assert.NoError(t, err)
		assert.Equal(t, snapshotName, snapshotData["resourceId"])
		assert.Equal(t, "snapshot-uuid-123", snapshotData["snapshotId"])
		assert.Equal(t, volumeId, snapshotData["volumeId"])
		assert.Equal(t, "my-volume", snapshotData["volumeResourceId"])
		assert.Equal(t, coremodels.LifeCycleStateREADY, snapshotData["snapshotState"])
	})

	t.Run("WhenSuccessfulWithSyncModeInProgress", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		// Test data
		projectNumber := "123456789"
		locationId := "us-east1"
		volumeId := "550e8400-e29b-41d4-a716-446655440000"
		snapshotName := "my-snapshot"
		jobUUID := "job-uuid-123"

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: snapshotName,
		}
		req.IsAppConsistent = oasgenserver.NewOptBool(true)

		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: projectNumber,
			LocationId:    locationId,
			VolumeId:      volumeId,
		}

		expectedParams := &commonparams.CreateSnapshotParams{
			SnapshotBaseParams: commonparams.SnapshotBaseParams{
				AccountName: projectNumber,
				VolumeID:    volumeId,
			},
			Name:            snapshotName,
			Description:     "",
			IsAppConsistent: true,
		}

		// Mock snapshot response (sync mode in progress - snapshot is CREATING)
		mockSnapshot := &coremodels.Snapshot{
			BaseModel: coremodels.BaseModel{
				UUID:      "snapshot-uuid-123",
				CreatedAt: time.Now(),
			},
			Name:                  snapshotName,
			VolumeUUID:            volumeId,
			LifeCycleState:        coremodels.LifeCycleStateCreating,
			LifeCycleStateDetails: "Creation in progress",
		}

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, expectedParams).Return(mockSnapshot, jobUUID, nil)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Should return OperationV1 with done=false (202 response)
		operationResponse, ok := result.(*oasgenserver.OperationV1)
		assert.True(t, ok, "Expected OperationV1 response when snapshot is CREATING")
		assert.False(t, operationResponse.Done.Or(true), "Operation should not be done")
		expectedOperationID := "/v1beta/projects/" + projectNumber + "/locations/" + locationId + "/operations/" + jobUUID
		assert.Equal(t, expectedOperationID, operationResponse.Name.Or(""))
	})

	t.Run("WhenOrchestratorReturnsUserInputValidationError", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: "invalid-snapshot-name",
		}
		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			VolumeId:      "volume-uuid",
		}

		validationError := errors.NewUserInputValidationErr("Invalid snapshot name")

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "", validationError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		badRequest, ok := result.(*oasgenserver.V1CreateSnapshotBadRequest)
		assert.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, int(badRequest.Code))
		assert.Contains(t, badRequest.Message, "Invalid snapshot name")
	})

	t.Run("WhenOrchestratorReturnsNotFoundError", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: "my-snapshot",
		}
		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			VolumeId:      "non-existent-volume",
		}

		notFoundError := errors.NewNotFoundErr("volume", nil)

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "", notFoundError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		badRequest, ok := result.(*oasgenserver.V1CreateSnapshotBadRequest)
		assert.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, int(badRequest.Code))
	})

	t.Run("WhenOrchestratorReturnsConflictError", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: "existing-snapshot",
		}
		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			VolumeId:      "volume-uuid",
		}

		conflictError := errors.NewConflictErr("snapshot already exists")

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "", conflictError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		conflictResponse, ok := result.(*oasgenserver.V1CreateSnapshotConflict)
		assert.True(t, ok)
		assert.Equal(t, http.StatusConflict, int(conflictResponse.Code))
		assert.Contains(t, conflictResponse.Message, "snapshot already exists")
	})

	t.Run("WhenOrchestratorReturnsInternalError", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: "my-snapshot",
		}
		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			VolumeId:      "volume-uuid",
		}

		internalError := stderrors.New("database connection failed")

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "", internalError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		internalErrorResponse, ok := result.(*oasgenserver.V1CreateSnapshotInternalServerError)
		assert.True(t, ok)
		assert.Equal(t, http.StatusInternalServerError, int(internalErrorResponse.Code))
		assert.Contains(t, internalErrorResponse.Message, "database connection failed")
	})

	t.Run("WhenONTAPRWVolumeErrorReturned_ThenReturn400", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: "my-snapshot",
		}
		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			VolumeId:      "volume-uuid",
		}

		// ONTAP error when trying to create snapshot on non-RW volume - use BadRequestErr
		rwVolumeError := errors.NewBadRequestErr("snapshot creation operation not allowed for this volume")

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "", rwVolumeError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		badRequestResponse, ok := result.(*oasgenserver.V1CreateSnapshotBadRequest)
		assert.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, int(badRequestResponse.Code))
		assert.Contains(t, badRequestResponse.Message, "snapshot creation operation not allowed for this volume")
	})

	t.Run("WhenVCPErrorWithSnapshotNotAllowedReturned_ThenReturn400", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: "my-snapshot",
		}
		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			VolumeId:      "volume-uuid",
		}

		// VCPError with ErrSnapshotNotAllowedForVolume tracking ID (HTTP 400)
		rwVolumeError := vsaerrors.NewVCPError(vsaerrors.ErrSnapshotNotAllowedForVolume, stderrors.New("snapshot creation operation not allowed for this volume"))

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "", rwVolumeError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		badRequestResponse, ok := result.(*oasgenserver.V1CreateSnapshotBadRequest)
		assert.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, int(badRequestResponse.Code))
		assert.Contains(t, badRequestResponse.Message, "snapshot creation operation not allowed for this volume")
	})

	t.Run("WhenDescriptionNotSet", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: "my-snapshot",
		}
		// Description not set

		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			VolumeId:      "volume-uuid",
		}

		expectedParams := &commonparams.CreateSnapshotParams{
			SnapshotBaseParams: commonparams.SnapshotBaseParams{
				AccountName: "123456789",
				VolumeID:    "volume-uuid",
			},
			Name:            "my-snapshot",
			Description:     "",
			IsAppConsistent: false,
		}

		mockSnapshot := &coremodels.Snapshot{
			BaseModel: coremodels.BaseModel{
				UUID: "snapshot-uuid",
			},
			Name:           "my-snapshot",
			VolumeUUID:     "volume-uuid",
			LifeCycleState: coremodels.LifeCycleStateREADY,
		}

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, expectedParams).Return(mockSnapshot, "job-uuid", nil)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("WhenIsAppConsistentNotSet", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: "my-snapshot",
		}
		// IsAppConsistent not set

		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			VolumeId:      "volume-uuid",
		}

		expectedParams := &commonparams.CreateSnapshotParams{
			SnapshotBaseParams: commonparams.SnapshotBaseParams{
				AccountName: "123456789",
				VolumeID:    "volume-uuid",
			},
			Name:            "my-snapshot",
			Description:     "",
			IsAppConsistent: false, // Should default to false
		}

		mockSnapshot := &coremodels.Snapshot{
			BaseModel: coremodels.BaseModel{
				UUID: "snapshot-uuid",
			},
			Name:           "my-snapshot",
			VolumeUUID:     "volume-uuid",
			LifeCycleState: coremodels.LifeCycleStateREADY,
		}

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, expectedParams).Return(mockSnapshot, "job-uuid", nil)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("WhenVCPErrorWithInsufficientSpaceReturned_ThenReturn400", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: "my-snapshot",
		}
		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			VolumeId:      "volume-uuid",
		}

		// VCPError with ErrSnapshotInsufficientSpace tracking ID (HTTP 400)
		insufficientSpaceError := vsaerrors.NewVCPError(vsaerrors.ErrSnapshotInsufficientSpace, stderrors.New("No space left on device"))

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "", insufficientSpaceError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		badRequestResponse, ok := result.(*oasgenserver.V1CreateSnapshotBadRequest)
		assert.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, int(badRequestResponse.Code))
		// Verify customer-friendly message is returned
		assert.Contains(t, badRequestResponse.Message, "Insufficient storage space")
	})

	t.Run("WhenVCPErrorWithMaximumLimitExceededReturned_ThenReturn400", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: "my-snapshot",
		}
		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			VolumeId:      "volume-uuid",
		}

		// VCPError with ErrSnapshotMaximumLimitExceeded tracking ID (HTTP 400)
		maxLimitError := vsaerrors.NewVCPError(vsaerrors.ErrSnapshotMaximumLimitExceeded, stderrors.New("Cannot exceed maximum number of snapshots"))

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "", maxLimitError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		badRequestResponse, ok := result.(*oasgenserver.V1CreateSnapshotBadRequest)
		assert.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, int(badRequestResponse.Code))
		// Verify customer-friendly message is returned
		assert.Contains(t, badRequestResponse.Message, "Error creating snapshot - Maximum snapshot limit reached for this volume. Please delete existing snapshots and try again.")
	})

	t.Run("WhenVCPErrorWithConflictReturned_ThenReturn409", func(t *testing.T) {
		// Setup
		mockOrch := factory.NewMockOrchestratorFactory(t)
		handler := NewHandler(mockOrch)

		// Mock parseAndValidateRegionAndZone to return success
		originalParseAndValidate := parseAndValidateRegionAndZone
		defer func() {
			parseAndValidateRegionAndZone = originalParseAndValidate
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east1", "", nil
		}

		req := &oasgenserver.VolumeSnapshotCreateV1{
			ResourceId: "my-snapshot",
		}
		params := oasgenserver.V1CreateSnapshotParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			VolumeId:      "volume-uuid",
		}

		// VCPError with ErrCreateSnapshotConflict tracking ID (HTTP 409)
		conflictError := vsaerrors.NewVCPError(vsaerrors.ErrCreateSnapshotConflict, stderrors.New("snapshot already exists"))

		// Set up expectations
		mockOrch.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "", conflictError)

		// Execute
		ctx := context.Background()
		result, err := handler.V1CreateSnapshot(ctx, req, params)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)

		conflictResponse, ok := result.(*oasgenserver.V1CreateSnapshotConflict)
		assert.True(t, ok)
		assert.Equal(t, http.StatusConflict, int(conflictResponse.Code))
		// Verify customer-friendly message is returned
		assert.Contains(t, conflictResponse.Message, "snapshot already exists")
	})
}
