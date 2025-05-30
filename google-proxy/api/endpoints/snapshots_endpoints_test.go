package api

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/snapshots"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestHandler_V1betaGetMultipleSnapshots(t *testing.T) {
	t.Run("WhenGetMultipleSnapshotsFailsWithBadRequest", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)

		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:     "location-id",
			ProjectNumber:  "project-number",
			VolumeId:       "volume-resource-id",
			XCorrelationID: gcpserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"uri1", "uri2"},
		}

		errorMessage := "BadRequest error"
		errorCode := float64(400)
		mockError := &snapshots.V1betaGetMultipleSnapshotsBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpserver.V1betaGetMultipleSnapshotsBadRequest).Code)
		assert.Equal(tt, errorMessage, result.(*gcpserver.V1betaGetMultipleSnapshotsBadRequest).Message)
	})

	t.Run("WhenGetMultipleSnapshotsFailsWithUnauthorized", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)

		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:     "location-id",
			ProjectNumber:  "project-number",
			VolumeId:       "volume-resource-id",
			XCorrelationID: gcpserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"uri1", "uri2"},
		}

		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &snapshots.V1betaGetMultipleSnapshotsUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpserver.V1betaGetMultipleSnapshotsUnauthorized).Code)
		assert.Equal(tt, errorMessage, result.(*gcpserver.V1betaGetMultipleSnapshotsUnauthorized).Message)
	})

	t.Run("WhenGetMultipleSnapshotsFailsWithForbidden", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)

		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:     "location-id",
			ProjectNumber:  "project-number",
			VolumeId:       "volume-resource-id",
			XCorrelationID: gcpserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"uri1", "uri2"},
		}

		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &snapshots.V1betaGetMultipleSnapshotsForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpserver.V1betaGetMultipleSnapshotsForbidden).Code)
		assert.Equal(tt, errorMessage, result.(*gcpserver.V1betaGetMultipleSnapshotsForbidden).Message)
	})

	t.Run("WhenGetMultipleSnapshotsFailsWithTooManyRequests", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)

		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:     "location-id",
			ProjectNumber:  "project-number",
			VolumeId:       "volume-resource-id",
			XCorrelationID: gcpserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"uri1", "uri2"},
		}

		errorMessage := "Too many requests"
		errorCode := float64(429)
		mockError := &snapshots.V1betaGetMultipleSnapshotsTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpserver.V1betaGetMultipleSnapshotsTooManyRequests).Code)
		assert.Equal(tt, errorMessage, result.(*gcpserver.V1betaGetMultipleSnapshotsTooManyRequests).Message)
	})

	t.Run("WhenGetMultipleReplicationsSucceeds", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)

		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:     "location-id",
			ProjectNumber:  "project-number",
			VolumeId:       "volume-resource-id",
			XCorrelationID: gcpserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"uri1", "uri2"},
		}

		description := "description"
		mockResponse := &snapshots.V1betaGetMultipleSnapshotsOK{
			Payload: &snapshots.V1betaGetMultipleSnapshotsOKBody{
				Snapshots: []*models.SnapshotV1beta{
					{
						SnapshotID:  "snapshot-id-1",
						Description: &description,
						VolumeID:    "vol-id",
					},
				},
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		successResult, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(tt, ok)
		assert.Equal(tt, "snapshot-id-1", successResult.Snapshots[0].SnapshotId.Value)
	})
}

func TestV1betaCreateSnapshot(t *testing.T) {
	t.Run("WhenRegionParsingError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaCreateSnapshotParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "test-volume-id",
		}
		req := &gcpserver.VolumeSnapshotCreateV1beta{
			ResourceId:      "test-snapshot-id",
			Description:     gcpserver.NewOptString("test-description"),
			IsAppConsistent: gcpserver.NewOptBool(false),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "", "", &gcpserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		result, _ := handler.V1betaCreateSnapshot(context.Background(), req, params)

		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpserver.V1betaCreateSnapshotBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", result.(*gcpserver.V1betaCreateSnapshotBadRequest).Message)
	})

	t.Run("WhenCreateSnapshotReturnsError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaCreateSnapshotParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "test-volume-id",
		}
		req := &gcpserver.VolumeSnapshotCreateV1beta{
			ResourceId:      "test-snapshot-id",
			Description:     gcpserver.NewOptString("test-description"),
			IsAppConsistent: gcpserver.NewOptBool(false),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "us-east4", "us-east4", nil
		}

		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "operation-id", errors.New("error"))

		result, _ := handler.V1betaCreateSnapshot(context.Background(), req, params)

		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpserver.V1betaCreateSnapshotInternalServerError).Code)
	})

	t.Run("WhenCreateSnapshotReturnsNotFoundError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaCreateSnapshotParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "test-volume-id",
		}
		req := &gcpserver.VolumeSnapshotCreateV1beta{
			ResourceId:      "test-snapshot-id",
			Description:     gcpserver.NewOptString("test-description"),
			IsAppConsistent: gcpserver.NewOptBool(false),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "us-east4", "us-east4", nil
		}

		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "operation-id", errors.NewNotFoundErr("snapshot", nil))

		result, err := handler.V1betaCreateSnapshot(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpserver.V1betaCreateSnapshotBadRequest).Code)
	})

	t.Run("WhenCreateSnapshotReturnsConflictError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaCreateSnapshotParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "test-volume-id",
		}
		req := &gcpserver.VolumeSnapshotCreateV1beta{
			ResourceId:      "test-snapshot-id",
			Description:     gcpserver.NewOptString("test-description"),
			IsAppConsistent: gcpserver.NewOptBool(false),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "us-east4", "us-east4", nil
		}

		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "operation-id", errors.NewConflictErr("conflict"))

		result, err := handler.V1betaCreateSnapshot(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(409), result.(*gcpserver.V1betaCreateSnapshotConflict).Code)
	})

	t.Run("WhenCreateSnapshotReturnsConflictError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaCreateSnapshotParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "test-volume-id",
		}
		req := &gcpserver.VolumeSnapshotCreateV1beta{
			ResourceId:      "test-snapshot-id",
			Description:     gcpserver.NewOptString("test-description"),
			IsAppConsistent: gcpserver.NewOptBool(false),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "us-east4", "us-east4", nil
		}

		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, "operation-id", errors.NewUserInputValidationErr("validation error"))

		result, err := handler.V1betaCreateSnapshot(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpserver.V1betaCreateSnapshotBadRequest).Code)
	})

	t.Run("WhenCreateSnapshotSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaCreateSnapshotParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "test-volume-id",
		}
		req := &gcpserver.VolumeSnapshotCreateV1beta{
			ResourceId:      "test-snapshot-id",
			Description:     gcpserver.NewOptString("test-description"),
			IsAppConsistent: gcpserver.NewOptBool(false),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "us-east4", "us-east4", nil
		}

		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + "operation-id"
		mockOrchestrator.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(&coremodels.Snapshot{BaseModel: coremodels.BaseModel{UUID: "new-snapshot-uuid"}}, "operation-id", nil)

		result, err := handler.V1betaCreateSnapshot(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, operationID, result.(*gcpserver.OperationV1beta).Name.Value)
	})
}

func Test_convertToSnapshotsV1Beta(t *testing.T) {
	t.Run("WhenConvertToSnapshotsV1BetaSucceeds", func(tt *testing.T) {
		snapshotV1betas := &models.SnapshotV1beta{
			SnapshotID:  "snapshot-id-1",
			Description: nillable.ToPointer("description"),
			VolumeID:    "vol-id",
		}

		result := convertToSnapshotsV1Beta(snapshotV1betas)

		assert.Equal(t, result.SnapshotId.Value, snapshotV1betas.SnapshotID)
		assert.Equal(t, result.Description.Value, *snapshotV1betas.Description)
		assert.Equal(t, result.VolumeId.Value, snapshotV1betas.VolumeID)
	})
}

func Test_convertModelToVCPSnapshot(t *testing.T) {
	t.Run("All fields are mapped correctly", func(tt *testing.T) {
		snapshot := &coremodels.Snapshot{
			BaseModel: coremodels.BaseModel{
				UUID:      "uuid-1",
				CreatedAt: time.Now(),
			},
			Name:                  "snap-1",
			VolumeUUID:            "vol-uuid-1",
			VolumeName:            "vol-name-1",
			LifeCycleState:        coremodels.LifeCycleStateREADY,
			LifeCycleStateDetails: "details",
			Description:           "desc",
		}
		result := convertModelToVCPSnapshot(snapshot)
		assert.Equal(tt, snapshot.Name, result.ResourceId)
		assert.Equal(tt, snapshot.UUID, result.SnapshotId.Value)
		assert.Equal(tt, snapshot.VolumeUUID, result.VolumeId.Value)
		assert.Equal(tt, snapshot.VolumeName, result.VolumeResourceId.Value)
		assert.Equal(tt, time.Time(snapshot.CreatedAt), result.Created.Value)
		assert.Equal(tt, string(snapshot.LifeCycleState), string(result.SnapshotState.Value))
		assert.Equal(tt, snapshot.LifeCycleStateDetails, result.SnapshotStateDetails.Value)
		assert.Equal(tt, snapshot.Description, result.Description.Value)
	})

	t.Run("Nil snapshot returns nil", func(tt *testing.T) {
		result := convertModelToVCPSnapshot(nil)
		assert.Nil(tt, result)
	})
}

func TestHandler_V1betaDescribeSnapshot(t *testing.T) {
	t.Run("WhenSnapshotNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaDescribeSnapshotParams{
			SnapshotId: "non-existent-snapshot-id",
		}

		mockOrchestrator.EXPECT().GetSnapshot(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("Snapshot", nil))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDescribeSnapshot(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(404), result.(*gcpserver.V1betaDescribeSnapshotNotFound).Code)
		assert.Equal(tt, "Snapshot not found", result.(*gcpserver.V1betaDescribeSnapshotNotFound).Message)
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaDescribeSnapshotParams{
			SnapshotId: "snapshot-id",
		}

		mockOrchestrator.EXPECT().GetSnapshot(mock.Anything, mock.Anything).Return(nil, errors.New("Internal server error"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDescribeSnapshot(context.Background(), params)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		errorResult, ok := result.(*gcpserver.V1betaDescribeSnapshotInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), errorResult.Code)
		assert.Equal(tt, "Internal server error", errorResult.Message)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaDescribeSnapshotParams{
			SnapshotId: "snapshot-id",
		}

		createdAt := time.Now()
		snapshot := &coremodels.Snapshot{
			BaseModel: coremodels.BaseModel{
				UUID:      "snapshot-id",
				CreatedAt: createdAt,
			},
			Name:                  "snapshot-name",
			Description:           "snapshot-description",
			VolumeUUID:            "volume-id",
			VolumeName:            "volume-name",
			LifeCycleState:        coremodels.LifeCycleStateREADY,
			LifeCycleStateDetails: coremodels.LifeCycleStateAvailableDetails,
		}

		mockOrchestrator.EXPECT().GetSnapshot(mock.Anything, mock.Anything).Return(snapshot, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDescribeSnapshot(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		snapshotResult, ok := result.(*gcpserver.SnapshotV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "snapshot-id", snapshotResult.SnapshotId.Value)
		assert.Equal(tt, "snapshot-name", snapshotResult.ResourceId)
		assert.Equal(tt, "snapshot-description", snapshotResult.Description.Value)
		assert.Equal(tt, "volume-id", snapshotResult.VolumeId.Value)
		assert.Equal(tt, "volume-name", snapshotResult.VolumeResourceId.Value)
		assert.Equal(tt, createdAt, snapshotResult.Created.Value)
		assert.Equal(tt, gcpserver.SnapshotV1betaSnapshotStateREADY, snapshotResult.SnapshotState.Value)
		assert.Equal(tt, coremodels.LifeCycleStateAvailableDetails, snapshotResult.SnapshotStateDetails.Value)
	})
}

func TestHandler_V1betaListSnapshot(t *testing.T) {
	t.Run("WhenListSnapshotsSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaListSnapshotParams{
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		testSnapshots := []*coremodels.Snapshot{
			{
				BaseModel:             coremodels.BaseModel{UUID: "snap-uuid-1", CreatedAt: time.Now()},
				Name:                  "snap1",
				VolumeUUID:            "vol-uuid-1",
				VolumeName:            "vol-name-1",
				LifeCycleState:        coremodels.LifeCycleStateREADY,
				LifeCycleStateDetails: "details1",
				Description:           "desc1",
			},
			{
				BaseModel:             coremodels.BaseModel{UUID: "snap-uuid-2", CreatedAt: time.Now()},
				Name:                  "snap2",
				VolumeUUID:            "vol-uuid-2",
				VolumeName:            "vol-name-2",
				LifeCycleState:        coremodels.LifeCycleStateREADY,
				LifeCycleStateDetails: "details2",
				Description:           "desc2",
			},
		}
		mockOrchestrator.EXPECT().ListSnapshots(mock.Anything, mock.Anything).Return(testSnapshots, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaListSnapshot(context.Background(), params)

		assert.NoError(tt, err)
		okResult, ok := result.(*gcpserver.V1betaListSnapshotOK)
		assert.True(tt, ok)
		assert.Len(tt, okResult.Snapshots, 2)
		assert.Equal(tt, "snap1", okResult.Snapshots[0].ResourceId)
		assert.Equal(tt, "snap2", okResult.Snapshots[1].ResourceId)
	})

	t.Run("WhenListSnapshotsNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaListSnapshotParams{
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		mockOrchestrator.EXPECT().ListSnapshots(mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("Snapshot", nil))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaListSnapshot(context.Background(), params)

		assert.NoError(tt, err)
		notFound, ok := result.(*gcpserver.V1betaListSnapshotNotFound)
		assert.True(tt, ok)
		assert.Equal(tt, float64(404), notFound.Code)
	})

	t.Run("WhenListSnapshotsInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaListSnapshotParams{
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		mockOrchestrator.EXPECT().ListSnapshots(mock.Anything, mock.Anything).Return(nil, errors.New("internal error"))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaListSnapshot(context.Background(), params)

		assert.Error(tt, err)
		internal, ok := result.(*gcpserver.V1betaListSnapshotInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internal.Code)
	})
}

func TestV1betaDeleteSnapshot(t *testing.T) {
	t.Run("WhenLocationValidationFailsInDeleteSnapshot", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaDeleteSnapshotParams{
			ProjectNumber: "project-number",
			LocationId:    "invalid-location-id",
			VolumeId:      "volume-id",
			SnapshotId:    "snapshot-id",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "", "", &gcpserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		result, _ := handler.V1betaDeleteSnapshot(context.Background(), params)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpserver.V1betaDeleteSnapshotBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", result.(*gcpserver.V1betaDeleteSnapshotBadRequest).Message)
	})
	t.Run("WhenDeleteSnapshotReturnsError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaDeleteSnapshotParams{
			ProjectNumber: "project-number",
			LocationId:    "location-id",
			VolumeId:      "volume-id",
			SnapshotId:    "snapshot-id",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().DeleteSnapshot(mock.Anything, mock.Anything).Return(nil, "operation-id", errors.New("error"))
		result, _ := handler.V1betaDeleteSnapshot(context.Background(), params)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpserver.V1betaDeleteSnapshotInternalServerError).Code)
	})

	t.Run("WhenDeleteSnapshotReturnsNotFoundError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaDeleteSnapshotParams{
			ProjectNumber: "project-number",
			LocationId:    "location-id",
			VolumeId:      "volume-id",
			SnapshotId:    "snapshot-id",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().DeleteSnapshot(mock.Anything, mock.Anything).Return(nil, "operation-id", errors.NewNotFoundErr("snapshot", nil))
		result, err := handler.V1betaDeleteSnapshot(context.Background(), params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "Snapshot not found", result.(*gcpserver.V1betaDeleteSnapshotBadRequest).Message)
	})

	t.Run("WhenDeleteSnapshotReturnsConflictError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaDeleteSnapshotParams{
			ProjectNumber: "project-number",
			LocationId:    "location-id",
			VolumeId:      "volume-id",
			SnapshotId:    "snapshot-id",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.EXPECT().DeleteSnapshot(mock.Anything, mock.Anything).Return(nil, "operation-id", errors.NewConflictErr("conflict"))
		result, err := handler.V1betaDeleteSnapshot(context.Background(), params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(409), result.(*gcpserver.V1betaDeleteSnapshotConflict).Code)
	})

	t.Run("WhenDeleteSnapshotSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaDeleteSnapshotParams{
			ProjectNumber: "project-number",
			LocationId:    "location-id",
			VolumeId:      "volume-id",
			SnapshotId:    "snapshot-id",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "us-east4", "us-east4", nil
		}

		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + "operation-id"
		mockOrchestrator.EXPECT().DeleteSnapshot(mock.Anything, mock.Anything).Return(&coremodels.Snapshot{BaseModel: coremodels.BaseModel{UUID: "deleted-snapshot-uuid"}}, "operation-id", nil)

		result, err := handler.V1betaDeleteSnapshot(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, operationID, result.(*gcpserver.OperationV1beta).Name.Value)
	})
}
