package api

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func (h Handler) V1betaStartProjectEvent(ctx context.Context, req *gcpgenserver.StateUpdateV1beta, params gcpgenserver.V1betaStartProjectEventParams) (gcpgenserver.V1betaStartProjectEventRes, error) {
	// Check state [ON, OFF, DELETE]
	// Do nothing if the state is DELETE
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaStartProjectEventBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	if req.State == gcpgenserver.StateUpdateV1betaStateDELETE {
		msg := "Start Project Event for " + models.StateDelete + " is not Implemented"
		return &gcpgenserver.V1betaStartProjectEventNotImplemented{
			Code:    models.NotImplementedErrorCode,
			Message: msg,
		}, nil
	}

	reqParams := &commonparams.StartProjectEventParams{
		LocationId:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: params.XCorrelationID.Value,
		State:          string(req.State),
	}

	job, err := h.Orchestrator.CreateOrGetStartProjectEventJob(ctx, reqParams)
	if err != nil {
		logger.Error("Failed to create startProjectEvent", "error", err.Error())
		return &gcpgenserver.V1betaStartProjectEventInternalServerError{Code: 500, Message: err.Error()}, nil
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + job
	return &gcpgenserver.V1betaStartProjectEventAccepted{
		Name: gcpgenserver.NewOptString(operationID),
		Done: gcpgenserver.NewOptBool(false),
	}, nil
}

func (h Handler) V1betaFinishProjectEvent(ctx context.Context, req *gcpgenserver.StateUpdateV1beta,
	params gcpgenserver.V1betaFinishProjectEventParams) (gcpgenserver.V1betaFinishProjectEventRes, error) {
	// Check state [ON, OFF, DELETE]
	// Only act on DELETE. ON and OFF are not implemented

	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)

	if parsingErr != nil {
		return &gcpgenserver.V1betaFinishProjectEventBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	// ON and OFF not implemented
	if req.State == gcpgenserver.StateUpdateV1betaStateON || req.State == gcpgenserver.StateUpdateV1betaStateOFF {
		msg := "Finish Project Event for " + string(req.State) + " is not Implemented"
		return &gcpgenserver.V1betaFinishProjectEventNotImplemented{
			Code:    models.NotImplementedErrorCode,
			Message: msg,
		}, nil
	}

	reqParams := &commonparams.FinishProjectEventParams{
		LocationId:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: params.XCorrelationID.Value,
		State:          string(req.State),
	}

	jobUUID, err := h.Orchestrator.CreateOrGetFinishProjectEventJob(ctx, reqParams)
	if err != nil {
		logger.Error("Failed to create finish project event", "error", err.Error())
		return &gcpgenserver.V1betaFinishProjectEventInternalServerError{Code: 500, Message: err.Error()}, nil
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + jobUUID

	return &gcpgenserver.V1betaFinishProjectEventAccepted{
		Name: gcpgenserver.NewOptString(operationID),
		Done: gcpgenserver.NewOptBool(false),
	}, nil
}

func (h Handler) V1betaResourceStateUpdate(ctx context.Context, req *gcpgenserver.ResourceStateUpdateV1beta, params gcpgenserver.V1betaResourceStateUpdateParams) (gcpgenserver.V1betaResourceStateUpdateRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaResourceStateUpdateBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	// check for state and return not implemented for [DELETE] state
	if req.State == gcpgenserver.ResourceStateUpdateV1betaStateDELETE &&
		req.ResourceType != gcpgenserver.ResourceStateUpdateV1betaResourceTypeVolume &&
		req.ResourceType != gcpgenserver.ResourceStateUpdateV1betaResourceTypeStoragePool {
		msg := "Handle Resource Event for " + models.StateDelete + " is not Implemented"
		return &gcpgenserver.V1betaResourceStateUpdateNotImplemented{
			Code:    models.NotImplementedErrorCode,
			Message: msg,
		}, nil
	}

	reqParams := &commonparams.UpdateResourceStateParams{
		LocationId:       params.LocationId,
		ProjectNumber:    params.ProjectNumber,
		XCorrelationID:   params.XCorrelationID.Value,
		State:            string(req.State),
		ResourceType:     string(req.ResourceType),
		ResourceId:       req.ResourceID,
		ParentResourceID: req.ParentResourceID.Value,
	}
	job, err := h.Orchestrator.UpdateResourceState(ctx, reqParams)
	if err != nil {
		if errors.IsUserInputValidationErr(err) {
			return &gcpgenserver.V1betaResourceStateUpdateInternalServerError{
				Code:    400,
				Message: err.Error(),
			}, nil
		}

		logger.Error("Failed to Handle resource event", "error", err.Error())
		return &gcpgenserver.V1betaResourceStateUpdateInternalServerError{Code: 500, Message: err.Error()}, nil
	}

	operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + job
	return &gcpgenserver.V1betaResourceStateUpdateAccepted{
		Name: gcpgenserver.NewOptString(operationID),
		Done: gcpgenserver.NewOptBool(false),
	}, nil
}
