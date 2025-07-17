package api

import (
	"context"
	
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func (h Handler) V1betaStartProjectEvent(ctx context.Context, req *gcpgenserver.StateUpdateV1beta, params gcpgenserver.V1betaStartProjectEventParams) (gcpgenserver.V1betaStartProjectEventRes, error) {
	// Check state [ON, OFF, DELETE}
	// Do nothing if the state is delete
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
		LocationID:     params.LocationId,
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
