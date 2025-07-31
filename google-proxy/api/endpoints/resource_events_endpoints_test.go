package api

// write UTs for resource_events_endpoints.go

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestResourceEventsEndpoints(t *testing.T) {
	t.Run("TestV1betaStartProjectEvent_StateDELETE", func(tt *testing.T) {
		ctx := context.Background()
		req := &gcpgenserver.StateUpdateV1beta{
			State: gcpgenserver.StateUpdateV1betaStateDELETE,
		}
		params := gcpgenserver.V1betaStartProjectEventParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", nil
		}
		handler := Handler{}
		res, err := handler.V1betaStartProjectEvent(ctx, req, params)

		errorCode := float64(405)
		errorMessage := "Start Project Event for " + models.StateDelete + " is not Implemented"

		assert.NoError(tt, err, "Expected no error when state is DELETE")
		assert.NotNil(tt, res)
		assert.Equal(tt, errorCode, res.(*gcpgenserver.V1betaStartProjectEventNotImplemented).Code)
		assert.Equal(tt, errorMessage, res.(*gcpgenserver.V1betaStartProjectEventNotImplemented).Message)
	})
	t.Run("TestV1betaStartProjectEvent_ErrorWhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.StateUpdateV1beta{}
		params := gcpgenserver.V1betaStartProjectEventParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		errorCode := float64(400)
		errorMessage := "Invalid location ID"
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}

		res, err := handler.V1betaStartProjectEvent(ctx, req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, errorCode, res.(*gcpgenserver.V1betaStartProjectEventBadRequest).Code)
		assert.Equal(tt, errorMessage, res.(*gcpgenserver.V1betaStartProjectEventBadRequest).Message)
	})
	t.Run("TestV1betaStartProjectEvent_ErrorWhenCreateOrGetStartProjectEventJob", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.StateUpdateV1beta{}
		params := gcpgenserver.V1betaStartProjectEventParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", nil
		}
		errorCode := float64(500)
		errorMessage := "panic"
		mockOrchestrator.EXPECT().CreateOrGetStartProjectEventJob(mock.Anything, mock.Anything).Return("", errors.New(errorMessage))

		res, err := handler.V1betaStartProjectEvent(ctx, req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, errorCode, res.(*gcpgenserver.V1betaStartProjectEventInternalServerError).Code)
		assert.Equal(tt, errorMessage, res.(*gcpgenserver.V1betaStartProjectEventInternalServerError).Message)
	})
	t.Run("TestV1betaStartProjectEvent_Success", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.StateUpdateV1beta{
			State: gcpgenserver.StateUpdateV1betaStateON,
		}
		params := gcpgenserver.V1betaStartProjectEventParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", nil
		}
		jobID := "jobID"
		mockOrchestrator.EXPECT().CreateOrGetStartProjectEventJob(mock.Anything, mock.Anything).Return(jobID, nil)
		operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobID

		res, err := handler.V1betaStartProjectEvent(ctx, req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, gcpgenserver.NewOptString(operationID), res.(*gcpgenserver.V1betaStartProjectEventAccepted).Name)
		assert.Equal(tt, gcpgenserver.NewOptBool(false), res.(*gcpgenserver.V1betaStartProjectEventAccepted).Done)
	})
}

func TestHandleResourceEventsEndpoints(t *testing.T) {
	t.Run("TestV1betaHandleResourceEvent_StateDELETE", func(tt *testing.T) {
		ctx := context.Background()
		req := &gcpgenserver.ResourceStateUpdateV1beta{
			State:        gcpgenserver.ResourceStateUpdateV1betaStateDELETE,
			ResourceType: gcpgenserver.ResourceStateUpdateV1betaResourceTypeKmsConfig,
		}
		params := gcpgenserver.V1betaResourceStateUpdateParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", nil
		}
		handler := Handler{}

		res, err := handler.V1betaResourceStateUpdate(ctx, req, params)

		errorCode := float64(405)
		errorMessage := "Handle Resource Event for " + models.StateDelete + " is not Implemented"

		assert.NoError(tt, err, "Expected no error when state is DELETE")
		assert.NotNil(tt, res)
		assert.Equal(tt, errorCode, res.(*gcpgenserver.V1betaResourceStateUpdateNotImplemented).Code)
		assert.Equal(tt, errorMessage, res.(*gcpgenserver.V1betaResourceStateUpdateNotImplemented).Message)
	})
	t.Run("TestV1betaHandleResourceEvent_ErrorWhenUpdateResourceState", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.ResourceStateUpdateV1beta{
			State: gcpgenserver.ResourceStateUpdateV1betaStateOFF,
		}
		params := gcpgenserver.V1betaResourceStateUpdateParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		errorCode := float64(500)
		errorMessage := "Some error"

		mockOrchestrator.EXPECT().UpdateResourceState(mock.Anything, mock.Anything).Return("", errors.New(errorMessage))

		res, err := handler.V1betaResourceStateUpdate(ctx, req, params)
		assert.NoError(tt, err, "Expected no error when account is found")
		assert.NotNil(tt, res)
		assert.Equal(tt, errorCode, res.(*gcpgenserver.V1betaResourceStateUpdateInternalServerError).Code)
		assert.Equal(tt, errorMessage, res.(*gcpgenserver.V1betaResourceStateUpdateInternalServerError).Message)
	})
	t.Run("TestV1betaStartProjectEvent_Success", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		ctx := context.Background()
		req := &gcpgenserver.ResourceStateUpdateV1beta{
			State: gcpgenserver.ResourceStateUpdateV1betaStateON,
		}
		params := gcpgenserver.V1betaResourceStateUpdateParams{
			ProjectNumber: "12345",
			LocationId:    "us-central1",
		}
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		jobID := "jobID"
		mockOrchestrator.EXPECT().UpdateResourceState(mock.Anything, mock.Anything).Return(jobID, nil)
		operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobID

		res, err := handler.V1betaResourceStateUpdate(ctx, req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, gcpgenserver.NewOptString(operationID), res.(*gcpgenserver.V1betaResourceStateUpdateAccepted).Name)
		assert.Equal(tt, gcpgenserver.NewOptBool(false), res.(*gcpgenserver.V1betaResourceStateUpdateAccepted).Done)
	})
}
