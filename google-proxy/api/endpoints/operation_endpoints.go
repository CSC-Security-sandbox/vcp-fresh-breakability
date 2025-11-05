package api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
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
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
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
	if err != nil && !customerrors.IsNotFoundErr(err) {
		logger.Error("Failed to describe operation", "error", err.Error())
		return &gcpgenserver.V1betaDescribeOperationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}

	if job != nil {
		helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, job)
		switch job.State {
		case models.JobsStateERROR:
			errMsg := vsaerrors.GetErrorMessageByTrackingID(job.TrackingID)
			detailedErrorMessage := errMsg.Message
			if job.TrackingID == vsaerrors.ErrRestoreVolumeValidation {
				detailedErrorMessage = string(job.ErrorDetails)
			}
			return &gcpgenserver.OperationV1beta{
				Done: gcpgenserver.NewOptBool(jobFinished),
				Name: gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, params.OperationId)),
				Error: gcpgenserver.OptStatusV1Beta{
					Value: gcpgenserver.StatusV1Beta{
						Code:    gcpgenserver.NewOptFloat64(float64(*errMsg.HttpCode)),
						Message: gcpgenserver.NewOptString(detailedErrorMessage),
					},
					Set: true,
				},
			}, nil
		case models.JobsStateNEW:
			return &gcpgenserver.OperationV1beta{
				Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, params.OperationId)),
				Done:     gcpgenserver.NewOptBool(jobNotFinished),
				Response: encodeOperationV1Beta(jobNewStateDetails),
			}, nil
		case models.JobsStatePROCESSING:
			return &gcpgenserver.OperationV1beta{
				Done:     gcpgenserver.NewOptBool(jobNotFinished),
				Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, params.OperationId)),
				Response: encodeOperationV1Beta(jobInProgressDetails),
			}, nil
		case models.JobsStateDONE:
			return &gcpgenserver.OperationV1beta{
				Done: gcpgenserver.NewOptBool(jobFinished),
				Name: gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, params.OperationId)),
			}, nil

		case models.JobsStateWaitForTemporal:
			return &gcpgenserver.OperationV1beta{
				Done:     gcpgenserver.NewOptBool(jobNotFinished),
				Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, params.OperationId)),
				Response: encodeOperationV1Beta(jobNewStateDetails),
			}, nil
		default:
			return &gcpgenserver.V1betaDescribeOperationInternalServerError{
				Code:    500,
				Message: fmt.Sprintf("Invalid Job State: %s", job.State),
			}, nil
		}
	} else {
		// If the job is not found, we will check the CVP operation
		// Create a CVP client to check the operation
		jwtToken := utils.GetJWTTokenFromContext(ctx)
		logger := util.GetLogger(ctx)
		cvpClient := createClient(logger, jwtToken)
		operationUUID := utils.GetOperationUUID(params.OperationId)
		operationParams := async.NewV1betaDescribeOperationParams()
		operationParams.OperationID = operationUUID
		operationParams.ProjectNumber = params.ProjectNumber
		operationParams.LocationID = params.LocationId
		// Call the CVP operation
		operationResponse, err := cvpClient.Async.V1betaDescribeOperation(operationParams)
		if err != nil {
			switch e := err.(type) {
			case *async.V1betaDescribeOperationUnprocessableEntity:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeOperationUnprocessableEntity{
					Code:    code,
					Message: msg,
				}, nil
			case *async.V1betaDescribeOperationTooManyRequests:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeOperationTooManyRequests{
					Code:    code,
					Message: msg,
				}, nil
			case *async.V1betaDescribeOperationBadRequest:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeOperationBadRequest{
					Code:    code,
					Message: msg,
				}, nil
			case *async.V1betaDescribeOperationUnauthorized:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeOperationUnauthorized{
					Code:    code,
					Message: msg,
				}, nil

			case *async.V1betaDescribeOperationForbidden:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeOperationForbidden{
					Code:    code,
					Message: msg,
				}, nil

			case *async.V1betaDescribeOperationNotFound:
				msg := nillable.GetString(&e.Payload.Message, "")
				code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
				return &gcpgenserver.V1betaDescribeOperationNotFound{
					Code:    code,
					Message: msg,
				}, nil
			case *async.V1betaDescribeOperationInternalServerError:
				return &gcpgenserver.V1betaDescribeOperationInternalServerError{
					Code:    500,
					Message: err.Error(),
				}, nil
			default:
				return &gcpgenserver.V1betaDescribeOperationInternalServerError{
					Code:    500,
					Message: err.Error(),
				}, nil
			}
		}
		if operationResponse == nil || operationResponse.Payload == nil {
			return &gcpgenserver.V1betaDescribeOperationInternalServerError{
				Code:    500,
				Message: "unknown error during the get job",
			}, nil
		}
		// Convert the CVP operation to gcpgenserver operation
		convertedOperation := convertOperationToOperationV1Beta(operationResponse.Payload)
		// Return the converted operation
		return convertedOperation, nil
	}
}

func convertOperationToOperationV1Beta(op *cvpmodels.OperationV1beta) *gcpgenserver.OperationV1beta {
	result := &gcpgenserver.OperationV1beta{
		Name: gcpgenserver.NewOptString(op.Name),
	}
	if op.Done != nil {
		result.Done = gcpgenserver.NewOptBool(*op.Done)
	}
	if op.Error != nil {
		result.Error = gcpgenserver.OptStatusV1Beta{
			Value: gcpgenserver.StatusV1Beta{
				Code:    gcpgenserver.NewOptFloat64(op.Error.Code),
				Message: gcpgenserver.NewOptString(op.Error.Message),
			},
			Set: true,
		}
	}
	if op.Response != nil {
		responseData, err := json.Marshal(op.Response)
		if err == nil {
			result.Response = responseData
		}
	}
	return result
}

func encodeOperationV1Beta(res interface{}) jx.Raw {
	data, _ := json.Marshal(res)
	return data
}

func (h Handler) V1betaInternalDescribeOperation(ctx context.Context, params gcpgenserver.V1betaInternalDescribeOperationParams) (gcpgenserver.V1betaInternalDescribeOperationRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := utils.ParseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaInternalDescribeOperationBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}
	jobUUID, err := uuid.Parse(params.OperationId)
	if err != nil {
		return &gcpgenserver.V1betaInternalDescribeOperationBadRequest{
			Code:    400,
			Message: err.Error(),
		}, nil
	}
	job, err := h.Orchestrator.GetJob(ctx, jobUUID.String())
	if err != nil {
		logger.Error("Failed to describe internal operation", "error", err.Error())
		return &gcpgenserver.V1betaInternalDescribeOperationInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, nil
	}
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, job)

	// Build the base internal operation response
	baseOperation := &gcpgenserver.InternalOperationV1beta{
		Name:       gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/internal/operations/%s", params.ProjectNumber, params.LocationId, params.OperationId)),
		TrackingId: gcpgenserver.NewOptInt(job.TrackingID),
	}

	switch job.State {
	case models.JobsStateERROR:
		errMsg := vsaerrors.GetErrorMessageByTrackingID(job.TrackingID)
		detailedErrorMessage := errMsg.Message
		if job.TrackingID == vsaerrors.ErrRestoreVolumeValidation {
			detailedErrorMessage = string(job.ErrorDetails)
		}
		baseOperation.Done = gcpgenserver.NewOptBool(jobFinished)
		baseOperation.Error = gcpgenserver.OptStatusV1Beta{
			Value: gcpgenserver.StatusV1Beta{
				Code:    gcpgenserver.NewOptFloat64(float64(*errMsg.HttpCode)),
				Message: gcpgenserver.NewOptString(detailedErrorMessage),
			},
			Set: true,
		}
		return baseOperation, nil

	case models.JobsStateNEW:
		baseOperation.Done = gcpgenserver.NewOptBool(jobNotFinished)
		baseOperation.Response = encodeOperationV1Beta(jobNewStateDetails)
		return baseOperation, nil

	case models.JobsStatePROCESSING:
		baseOperation.Done = gcpgenserver.NewOptBool(jobNotFinished)
		baseOperation.Response = encodeOperationV1Beta(jobInProgressDetails)
		return baseOperation, nil

	case models.JobsStateDONE:
		baseOperation.Done = gcpgenserver.NewOptBool(jobFinished)
		return baseOperation, nil

	case models.JobsStateWaitForTemporal:
		baseOperation.Done = gcpgenserver.NewOptBool(jobNotFinished)
		baseOperation.Response = encodeOperationV1Beta(jobNewStateDetails)
		return baseOperation, nil

	default:
		baseOperation.Done = gcpgenserver.NewOptBool(jobNotFinished)
		baseOperation.Response = encodeOperationV1Beta(jobInProgressDetails)
		return baseOperation, nil
	}
}
