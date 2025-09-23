package api

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/snapshots"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestHandler_V1betaGetMultipleSnapshots(t *testing.T) {
	t.Run("WhenRegionParsingError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "bad-location",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "", "", &gcpserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		handler := Handler{Orchestrator: mockOrchestrator}
		result, _ := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpserver.V1betaGetMultipleSnapshotsBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", result.(*gcpserver.V1betaGetMultipleSnapshotsBadRequest).Message)
	})

	t.Run("WhenSnapshotUuidsIsNil", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: nil,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		handler := Handler{Orchestrator: mockOrchestrator}
		result, _ := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpserver.V1betaGetMultipleSnapshotsBadRequest).Code)
		assert.Equal(tt, "SnapshotUUIDs cannot be empty", result.(*gcpserver.V1betaGetMultipleSnapshotsBadRequest).Message)
	})

	t.Run("WhenOrchestratorReturnsError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return(nil, errors.New("internal error"))
		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)
		assert.NoError(tt, err)
		internal, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internal.Code)
	})

	t.Run("WhenGetMultipleSnapshotsFailsWithBadRequest", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		oldValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "us-central1", "us-central1", nil
		}

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

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpserver.V1betaGetMultipleSnapshotsBadRequest).Code)
		assert.Equal(tt, errorMessage, result.(*gcpserver.V1betaGetMultipleSnapshotsBadRequest).Message)
	})

	t.Run("WhenSnapshotsFoundInVCP", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1", "snap-uuid-2"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockSnapshots := []*coremodels.Snapshot{
			{
				BaseModel: coremodels.BaseModel{UUID: "snap-uuid-1"},
				Name:      "snapshot-1",
			},
			{
				BaseModel: coremodels.BaseModel{UUID: "snap-uuid-2"},
				Name:      "snapshot-2",
			},
		}
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return(mockSnapshots, nil)
		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		okResult, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResult.Snapshots, 2)
		assert.Equal(tt, "snap-uuid-1", okResult.Snapshots[0].SnapshotId.Value)
		assert.Equal(tt, "snap-uuid-2", okResult.Snapshots[1].SnapshotId.Value)
	})

	t.Run("WhenSnapshotsNotFoundInVCP", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1", "snap-uuid-2"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return(nil, nil)

		mockClient := snapshots.NewMockClientService(tt)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(&snapshots.V1betaGetMultipleSnapshotsOK{
			Payload: &snapshots.V1betaGetMultipleSnapshotsOKBody{
				Snapshots: []*models.SnapshotV1beta{},
			},
		}, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		okResult, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResult.Snapshots, 0)
	})

	t.Run("WhenSnapshotsNotFoundInVCPAndFoundInCVP", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1", "snap-uuid-2"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		// Mock VCP response: no snapshots found
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return(nil, nil)

		// Mock CVP response: snapshots found
		mockClient := snapshots.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		cvpSnapshots := []*models.SnapshotV1beta{
			{
				SnapshotID:  "cvp-snap-uuid-1",
				Description: nillable.ToPointer("CVP snapshot 1"),
				VolumeID:    "volume-id",
				ResourceID:  nillable.ToPointer("cvp-resource-id-1"),
			},
			{
				SnapshotID:  "cvp-snap-uuid-2",
				Description: nillable.ToPointer("CVP snapshot 2"),
				VolumeID:    "volume-id",
				ResourceID:  nillable.ToPointer("cvp-resource-id-2"),
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(&snapshots.V1betaGetMultipleSnapshotsOK{
			Payload: &snapshots.V1betaGetMultipleSnapshotsOKBody{
				Snapshots: cvpSnapshots,
			},
		}, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		okResult, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(t, ok)
		assert.Len(t, okResult.Snapshots, 2)
		assert.Equal(t, "cvp-snap-uuid-1", okResult.Snapshots[0].SnapshotId.Value)
		assert.Equal(t, "cvp-snap-uuid-2", okResult.Snapshots[1].SnapshotId.Value)
		assert.Equal(t, "CVP snapshot 1", okResult.Snapshots[0].Description.Value)
		assert.Equal(t, "CVP snapshot 2", okResult.Snapshots[1].Description.Value)
	})

	t.Run("WhenOrchestratorGetMultipleSnapshotsFails_ReturnsInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1", "snap-uuid-2"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		// Mock orchestrator to return an error
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return(nil, errors.New("database connection failed"))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return InternalServerError with proper error message
		internalServerError, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalServerError.Code)
		assert.Equal(tt, "Internal server error", internalServerError.Message)
	})

	t.Run("WhenOrchestratorGetMultipleSnapshotsFails_ErrorNotReturned", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1", "snap-uuid-2"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		// Mock orchestrator to return an error
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return(nil, errors.New("database connection failed"))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		// Current behavior: err is NOT propagated from orchestrator
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return InternalServerError
		internalServerError, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalServerError.Code)
	})

	t.Run("WhenSomeSnapshotsNotFoundInVCP_TriggersCVPFallback", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1", "snap-uuid-2", "snap-uuid-3"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		// Mock VCP to return only some snapshots (snap-uuid-1 and snap-uuid-2 found, snap-uuid-3 missing)
		vcpSnapshots := []*coremodels.Snapshot{
			{
				BaseModel: coremodels.BaseModel{UUID: "snap-uuid-1"},
				Name:      "snapshot-1",
			},
			{
				BaseModel: coremodels.BaseModel{UUID: "snap-uuid-2"},
				Name:      "snapshot-2",
			},
		}
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return(vcpSnapshots, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return OK response with VCP snapshots only (since CVP fallback will be mocked)
		okResp, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Snapshots, 2)
		// Verify the snapshots returned are from VCP
		assert.Equal(tt, "snap-uuid-1", okResp.Snapshots[0].SnapshotId.Value)
		assert.Equal(tt, "snap-uuid-2", okResp.Snapshots[1].SnapshotId.Value)
	})

	t.Run("WhenAllSnapshotsFoundInVCP_NoCVPFallback", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1", "snap-uuid-2"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		// Mock VCP to return all requested snapshots
		vcpSnapshots := []*coremodels.Snapshot{
			{
				BaseModel: coremodels.BaseModel{UUID: "snap-uuid-1"},
				Name:      "snapshot-1",
			},
			{
				BaseModel: coremodels.BaseModel{UUID: "snap-uuid-2"},
				Name:      "snapshot-2",
			},
		}
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return(vcpSnapshots, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return OK response with all VCP snapshots, no CVP fallback needed
		okResp, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Snapshots, 2)
		// Verify all snapshots are returned
		assert.Equal(tt, "snap-uuid-1", okResp.Snapshots[0].SnapshotId.Value)
		assert.Equal(tt, "snap-uuid-2", okResp.Snapshots[1].SnapshotId.Value)
	})

	t.Run("WhenOrchestratorGetMultipleSnapshotsReturnsEmpty_TriggersCVPFallback", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1", "snap-uuid-2"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		// Mock VCP to return empty snapshots (triggering CVP fallback)
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return([]*coremodels.Snapshot{}, nil)

		// Mock CVP client to avoid nil pointer during fallback and return empty list
		mockSnapshotsClient := snapshots.NewMockClientService(tt)
		cvpClient := &cvpapi.Cvp{Snapshots: mockSnapshotsClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *cvpClient }
		mockSnapshotsClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(&snapshots.V1betaGetMultipleSnapshotsOK{
			Payload: &snapshots.V1betaGetMultipleSnapshotsOKBody{Snapshots: []*models.SnapshotV1beta{}},
		}, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return OK response (CVP fallback will be handled by getMultipleSnapshotsFromCVP)
		okResp, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Snapshots, 0) // CVP fallback will be mocked to return empty
	})

	t.Run("WhenOrchestratorGetMultipleSnapshotsReturnsNil_TriggersCVPFallback", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1", "snap-uuid-2"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		// Mock VCP to return nil (triggering CVP fallback)
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return(nil, nil)

		// Mock CVP client to avoid nil pointer during fallback and return empty list
		mockSnapshotsClient2 := snapshots.NewMockClientService(tt)
		cvpClient2 := &cvpapi.Cvp{Snapshots: mockSnapshotsClient2}
		originalCreateClient2 := createClient
		defer func() { createClient = originalCreateClient2 }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp { return *cvpClient2 }
		mockSnapshotsClient2.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(&snapshots.V1betaGetMultipleSnapshotsOK{
			Payload: &snapshots.V1betaGetMultipleSnapshotsOKBody{Snapshots: []*models.SnapshotV1beta{}},
		}, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return OK response (CVP fallback will be handled by getMultipleSnapshotsFromCVP)
		okResp, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Snapshots, 0) // CVP fallback will be mocked to return empty
	})

	t.Run("WhenAccountCreationSucceeds_ProceedsWithSnapshotRetrieval", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "new-account-number", // New account that will be created
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		// Mock VCP to return snapshots (account creation will be handled by getOrCreateAccount)
		vcpSnapshots := []*coremodels.Snapshot{
			{
				BaseModel: coremodels.BaseModel{UUID: "snap-uuid-1"},
				Name:      "snapshot-1",
			},
		}
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return(vcpSnapshots, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return OK response with snapshots
		okResp, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Snapshots, 1)
		assert.Equal(tt, "snap-uuid-1", okResp.Snapshots[0].SnapshotId.Value)
	})

	t.Run("WhenAccountCreationFails_ReturnsInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:    "location-id",
			ProjectNumber: "invalid-account-number",
			VolumeId:      "volume-id",
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"snap-uuid-1"},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpserver.Error) {
			return "region", "zone", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		// Mock orchestrator to return an error during account creation/retrieval
		mockOrchestrator.EXPECT().GetMultipleSnapshots(mock.Anything, params.VolumeId, mock.Anything, req.SnapshotUuids).Return(nil, errors.New("account creation failed"))

		handler := Handler{Orchestrator: mockOrchestrator}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Should return InternalServerError
		internalServerError, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalServerError.Code)
		assert.Equal(tt, "Internal server error", internalServerError.Message)
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
			ResourceID:  nillable.ToPointer("resource-id-1"),
		}

		result := convertToSnapshotsV1Beta(snapshotV1betas)

		assert.Equal(t, result.SnapshotId.Value, snapshotV1betas.SnapshotID)
		assert.Equal(t, result.Description.Value, *snapshotV1betas.Description)
		assert.Equal(t, result.VolumeId.Value, snapshotV1betas.VolumeID)
		assert.Equal(tt, result.ResourceId, *snapshotV1betas.ResourceID)
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
			StorageClass:          "SOFTWARE",
			SizeInBytes:           1234,
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
		assert.Equal(tt, snapshot.StorageClass, string(result.StorageClass.Value))
		assert.Equal(tt, snapshot.SizeInBytes, uint64(result.UsedBytes.Value))
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

func TestHandler_V1betaUpdateSnapshot(t *testing.T) {
	t.Run("WhenSnapshotUpdateSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaUpdateSnapshotParams{
			SnapshotId:    "snapshot-id",
			ProjectNumber: "project-number",
			LocationId:    "location-id",
		}
		req := &gcpserver.VolumeSnapshotUpdateV1beta{
			Description: "snapshot-description",
		}
		snapshot := &coremodels.Snapshot{
			BaseModel: coremodels.BaseModel{
				UUID:      "snapshot-id",
				CreatedAt: time.Now(),
			},
			Name:                  "snapshot-name",
			Description:           "snapshot-description",
			VolumeUUID:            "volume-id",
			VolumeName:            "volume-name",
			LifeCycleState:        coremodels.LifeCycleStateREADY,
			LifeCycleStateDetails: coremodels.LifeCycleStateAvailableDetails,
		}
		mockOrchestrator.EXPECT().UpdateSnapshot(mock.Anything, mock.Anything).Return(snapshot, "", nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaUpdateSnapshot(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		op, ok := result.(*gcpserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Contains(tt, op.Name.Value, uuid.UUID{}.String())
		assert.True(tt, op.Done.Value)
	})

	t.Run("WhenSnapshotReturnsBadRequestOnEmptySnapshotID", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaUpdateSnapshotParams{
			SnapshotId:    "",
			ProjectNumber: "project-number",
			LocationId:    "location-id",
		}
		req := &gcpserver.VolumeSnapshotUpdateV1beta{
			Description: "snapshot-description",
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaUpdateSnapshot(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badReq, ok := result.(*gcpserver.V1betaUpdateSnapshotBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Equal(tt, "Snapshot ID is required", badReq.Message)
	})

	t.Run("WhenSnapshotUpdateReturnsNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaUpdateSnapshotParams{
			SnapshotId:    "snapshot-id",
			ProjectNumber: "project-number",
			LocationId:    "location-id",
		}
		req := &gcpserver.VolumeSnapshotUpdateV1beta{}
		mockOrchestrator.EXPECT().UpdateSnapshot(mock.Anything, mock.Anything).Return(nil, "", errors.NewNotFoundErr("Snapshot not found", nil))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaUpdateSnapshot(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		notFound, ok := result.(*gcpserver.V1betaUpdateSnapshotNotFound)
		assert.True(tt, ok)
		assert.Equal(tt, float64(404), notFound.Code)
		assert.Equal(tt, "Snapshot not found", notFound.Message)
	})

	t.Run("WhenSnapshotUpdateReturnsBadRequest", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaUpdateSnapshotParams{
			SnapshotId:    "snapshot-id",
			ProjectNumber: "project-number",
			LocationId:    "location-id",
		}
		req := &gcpserver.VolumeSnapshotUpdateV1beta{}
		mockOrchestrator.EXPECT().UpdateSnapshot(mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("bad request"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaUpdateSnapshot(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badReq, ok := result.(*gcpserver.V1betaUpdateSnapshotBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Equal(tt, "bad request", badReq.Message)
	})

	t.Run("WhenSnapshotUpdateReturnsConflict", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaUpdateSnapshotParams{
			SnapshotId:    "snapshot-id",
			ProjectNumber: "project-number",
			LocationId:    "location-id",
		}
		req := &gcpserver.VolumeSnapshotUpdateV1beta{}
		mockOrchestrator.EXPECT().UpdateSnapshot(mock.Anything, mock.Anything).Return(nil, "", errors.NewConflictErr("conflict"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaUpdateSnapshot(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		conflict, ok := result.(*gcpserver.V1betaUpdateSnapshotConflict)
		assert.True(tt, ok)
		assert.Equal(tt, float64(409), conflict.Code)
		assert.Equal(tt, "conflict", conflict.Message)
	})

	t.Run("WhenSnapshotUpdateReturnsInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpserver.V1betaUpdateSnapshotParams{
			SnapshotId:    "snapshot-id",
			ProjectNumber: "project-number",
			LocationId:    "location-id",
		}
		req := &gcpserver.VolumeSnapshotUpdateV1beta{}
		mockOrchestrator.EXPECT().UpdateSnapshot(mock.Anything, mock.Anything).Return(nil, "", errors.New("internal error"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaUpdateSnapshot(context.Background(), req, params)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		internal, ok := result.(*gcpserver.V1betaUpdateSnapshotInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internal.Code)
		assert.Equal(tt, "internal error", internal.Message)
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
		assert.Equal(tt, "Snapshot not found", result.(*gcpserver.V1betaDeleteSnapshotNotFound).Message)
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

func Test_getMultipleSnapshotsFromCVP(t *testing.T) {
	mockClient := snapshots.NewMockClientService(t)
	ctx := context.Background()
	params := gcpserver.V1betaGetMultipleSnapshotsParams{
		LocationId:     "location-id",
		ProjectNumber:  "project-number",
		VolumeId:       "volume-id",
		XCorrelationID: gcpserver.NewOptString("corr-id"),
	}
	req := &gcpserver.SnapshotIdListV1beta{
		SnapshotUuids: []string{"snap-uuid-1"},
	}

	cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
	originalClient := createClient
	defer func() {
		createClient = originalClient
	}()
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	t.Run("SuccessWithCVPResponse", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)
		snap := &models.SnapshotV1beta{
			SnapshotID:  "cvp-snap-id",
			Description: nillable.ToPointer("desc"),
			VolumeID:    "cvp-vol-id",
			ResourceID:  nillable.ToPointer("cvp-res-id"),
		}
		resp := &snapshots.V1betaGetMultipleSnapshotsOK{
			Payload: &snapshots.V1betaGetMultipleSnapshotsOKBody{
				Snapshots: []*models.SnapshotV1beta{snap},
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(resp, nil)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		res, err := _getMultipleSnapshotsFromCVP(ctx, req, params, nil)
		assert.NoError(tt, err)
		ok, okType := res.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(tt, okType)
		assert.Len(tt, ok.Snapshots, 1)
		assert.Equal(tt, "cvp-snap-id", ok.Snapshots[0].SnapshotId.Value)
		assert.Equal(tt, "cvp-res-id", ok.Snapshots[0].ResourceId)
	})

	t.Run("NotFoundError", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)
		mockErr := &snapshots.V1betaGetMultipleSnapshotsNotFound{
			Payload: &models.Error{Code: 404, Message: "not found"},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		res, err := _getMultipleSnapshotsFromCVP(ctx, req, params, nil)
		assert.NoError(tt, err)
		notFound, ok := res.(*gcpserver.V1betaGetMultipleSnapshotsNotFound)
		assert.True(tt, ok)
		assert.Equal(tt, float64(404), notFound.Code)
	})

	t.Run("BadRequestError", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)
		mockErr := &snapshots.V1betaGetMultipleSnapshotsBadRequest{
			Payload: &models.Error{Code: 400, Message: "bad request"},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleSnapshotsFromCVP(ctx, req, params, nil)
		assert.NoError(tt, err)
		badReq, ok := res.(*gcpserver.V1betaGetMultipleSnapshotsBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
	})

	t.Run("UnauthorizedError", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)
		mockErr := &snapshots.V1betaGetMultipleSnapshotsUnauthorized{
			Payload: &models.Error{Code: 401, Message: "unauthorized"},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleSnapshotsFromCVP(ctx, req, params, nil)
		assert.NoError(tt, err)
		unauth, ok := res.(*gcpserver.V1betaGetMultipleSnapshotsUnauthorized)
		assert.True(tt, ok)
		assert.Equal(tt, float64(401), unauth.Code)
	})

	t.Run("ForbiddenError", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)
		mockErr := &snapshots.V1betaGetMultipleSnapshotsForbidden{
			Payload: &models.Error{Code: 403, Message: "forbidden"},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleSnapshotsFromCVP(ctx, req, params, nil)
		assert.NoError(tt, err)
		forbidden, ok := res.(*gcpserver.V1betaGetMultipleSnapshotsForbidden)
		assert.True(tt, ok)
		assert.Equal(tt, float64(403), forbidden.Code)
	})

	t.Run("TooManyRequestsError", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)
		mockErr := &snapshots.V1betaGetMultipleSnapshotsTooManyRequests{
			Payload: &models.Error{Code: 429, Message: "too many"},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleSnapshotsFromCVP(ctx, req, params, nil)
		assert.NoError(tt, err)
		tooMany, ok := res.(*gcpserver.V1betaGetMultipleSnapshotsTooManyRequests)
		assert.True(tt, ok)
		assert.Equal(tt, float64(429), tooMany.Code)
	})

	t.Run("DefaultError", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)
		mockErr := &snapshots.V1betaGetMultipleSnapshotsDefault{}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleSnapshotsFromCVP(ctx, req, params, nil)
		assert.NoError(tt, err)
		internal, ok := res.(*gcpserver.V1betaGetMultipleSnapshotsInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internal.Code)
	})

	t.Run("AppendsVCPSnapshots", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)
		snap := &models.SnapshotV1beta{
			SnapshotID:  "cvp-snap-id",
			Description: nillable.ToPointer("desc"),
			VolumeID:    "cvp-vol-id",
		}
		resp := &snapshots.V1betaGetMultipleSnapshotsOK{
			Payload: &snapshots.V1betaGetMultipleSnapshotsOKBody{
				Snapshots: []*models.SnapshotV1beta{snap},
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(resp, nil)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		vcpSnap := gcpserver.SnapshotV1beta{
			ResourceId: "vcp-snap",
			SnapshotId: gcpserver.NewOptString("vcp-snap-id"),
		}
		res, err := _getMultipleSnapshotsFromCVP(ctx, req, params, []gcpserver.SnapshotV1beta{vcpSnap})
		assert.NoError(tt, err)
		ok, okType := res.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(tt, okType)
		assert.Len(tt, ok.Snapshots, 2)
		assert.Equal(tt, "cvp-snap-id", ok.Snapshots[0].SnapshotId.Value)
		assert.Equal(tt, "vcp-snap-id", ok.Snapshots[1].SnapshotId.Value)
	})
}
