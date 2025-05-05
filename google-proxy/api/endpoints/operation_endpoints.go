package api

import (
	"context"
	"fmt"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"golang.org/x/exp/slog"
)

var (
	jobFinished    = true
	jobNotFinished = false
)

const (
	jobNewStateDetails   = "Job is still new"
	jobInProgressDetails = "Job is in progress"
)

func (h Handler) V1betaDescribeOperation(ctx context.Context, params gcpgenserver.V1betaDescribeOperationParams) (gcpgenserver.V1betaDescribeOperationRes, error) {
	logger := ctx.Value(middleware.ContextSLoggerKey).(log.Logger)
	_, _, parsingErr := utils.ParseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDescribeOperationBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}
	jobUUID, err := uuid.Parse(params.OperationId)
	if err != nil {
		return &gcpgenserver.V1betaDescribeOperationBadRequest{
			Code:    400,
			Message: err.Error(),
		}, nil
	}
	job, err := h.Orchestrator.GetJob(ctx, jobUUID.String())
	if err != nil {
		logger.Error("Failed to describe operation", slog.String("error", err.Error()))
		return &gcpgenserver.V1betaDescribeOperationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	if job != nil {
		switch job.State {
		case models.JobsStateERROR:
			return &gcpgenserver.V1betaDescribeOperationBadRequest{
				Code:    400,
				Message: "Job failed",
			}, nil
		case models.JobsStateNEW:
			return &gcpgenserver.OperationV1beta{
				Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, params.OperationId)),
				Done:     gcpgenserver.NewOptBool(jobNotFinished),
				Response: jx.Raw(jobNewStateDetails),
			}, nil
		case models.JobsStatePROCESSING:
			return &gcpgenserver.OperationV1beta{
				Done:     gcpgenserver.NewOptBool(jobNotFinished),
				Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, params.OperationId)),
				Response: jx.Raw(jobInProgressDetails),
			}, nil
		case models.JobsStateDONE:
			return &gcpgenserver.OperationV1beta{
				Done: gcpgenserver.NewOptBool(jobFinished),
				Name: gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, params.OperationId)),
			}, nil
		default:
			return &gcpgenserver.V1betaDescribeOperationInternalServerError{
				Code:    500,
				Message: fmt.Sprintf("Invalid Job State: %s", job.State),
			}, nil
		}
	}
	return &gcpgenserver.V1betaDescribeOperationInternalServerError{
		Code:    500,
		Message: fmt.Sprintf("Invalid Job State: %s", job.State),
	}, nil
}
