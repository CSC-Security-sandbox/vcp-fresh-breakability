package coreapi

import (
	"context"
	"encoding/json"
	"fmt"

	coreapi "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/core-api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var (
	coreAPIHost         = env.GetString("CORE_API_HOST", "")
	createCoreAPIClient = coreapi.GetCoreAPIClient
)

func FetchCredentials(ctx context.Context, poolDetails *models.PoolDetails, jwtToken string, logger log.Logger) (*coreapi.OntapCredentialsV1, error) {
	logger.InfoContext(ctx, "Fetching credentials from Core API",
		"poolID", poolDetails.PoolID,
		"accountName", poolDetails.AccountName,
		"userName", poolDetails.UserName)

	client := createCoreAPIClient(coreAPIHost, jwtToken, logger)

	params := coreapi.V1GetOntapCredentialsParams{
		PoolId:      poolDetails.PoolID,
		AccountName: coreapi.NewOptString(poolDetails.AccountName),
		UserName:    coreapi.NewOptString(poolDetails.UserName),
	}

	response, err := client.Invoker.V1GetOntapCredentials(ctx, params)
	if err != nil {
		logger.ErrorContext(ctx, "Core API call failed", "error", err, "poolID", poolDetails.PoolID)
		return nil, fmt.Errorf("core API call failed: %w", err)
	}

	switch resp := response.(type) {
	case *coreapi.OntapCredentialsV1:
		// Marshal the full response to JSON for logging
		respJSON, err := json.Marshal(resp)
		respJSONStr := ""
		if err != nil {
			respJSONStr = fmt.Sprintf("<error marshaling response: %v>", err)
		} else {
			respJSONStr = string(respJSON)
		}

		logger.InfoContext(ctx, "Successfully validated pool and got credentials",
			"poolID", poolDetails.PoolID,
			"authType", resp.AuthType.Value,
			"caURI", getStringValue(resp.CaURI),
			"fullResponse", log.Sanitize(respJSONStr))
		return resp, nil

	case *coreapi.V1GetOntapCredentialsNotFound:
		logger.ErrorContext(ctx, "Pool not found", "poolID", poolDetails.PoolID, "message", resp.Message)
		return nil, fmt.Errorf("pool not found: %s", resp.Message)

	case *coreapi.V1GetOntapCredentialsBadRequest:
		logger.ErrorContext(ctx, "Invalid pool details", "poolID", poolDetails.PoolID, "message", resp.Message)
		return nil, fmt.Errorf("invalid pool details: %s", resp.Message)

	case *coreapi.V1GetOntapCredentialsUnauthorized:
		logger.ErrorContext(ctx, "Unauthorized access", "poolID", poolDetails.PoolID, "message", resp.Message)
		return nil, fmt.Errorf("unauthorized access: %s", resp.Message)

	case *coreapi.V1GetOntapCredentialsForbidden:
		logger.ErrorContext(ctx, "Forbidden access", "poolID", poolDetails.PoolID, "message", resp.Message)
		return nil, fmt.Errorf("forbidden access: %s", resp.Message)

	case *coreapi.V1GetOntapCredentialsInternalServerError:
		logger.ErrorContext(ctx, "Internal server error", "poolID", poolDetails.PoolID, "message", resp.Message)
		return nil, fmt.Errorf("internal server error: %s", resp.Message)

	default:
		logger.ErrorContext(ctx, "Unexpected response from Core API", "responseType", fmt.Sprintf("%T", resp), "poolID", poolDetails.PoolID)
		return nil, fmt.Errorf("unexpected response from Core API")
	}
}

// getStringValue safely extracts string value from OptString, returning empty string if not set
func getStringValue(opt coreapi.OptString) string {
	if opt.IsSet() {
		return opt.Value
	}
	return ""
}

// SubmitExpertModeVolumeOperation submits a volume operation (Create/Update/Delete) to the Core API.
func SubmitExpertModeVolumeOperation(ctx context.Context, request *coreapi.ExpertModeVolumeV1, jwtToken string, logger log.Logger) error {
	logger.InfoContext(ctx, "Submitting expert mode volume operation",
		"projectNumber", request.ProjectNumber,
		"poolUUID", request.PoolUUID,
		"volumeName", request.VolumeName,
		"action", request.Action,
		"style", request.Style)

	client := createCoreAPIClient(coreAPIHost, jwtToken, logger)
	correlationID, _ := ctx.Value(middleware.CorrelationContextKey).(string)
	params := coreapi.V1ExpertModeVolumeParams{
		XCorrelationID: coreapi.NewOptString(correlationID),
	}

	response, err := client.Invoker.V1ExpertModeVolume(ctx, request, params)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to submit expert mode volume operation",
			"error", err,
			"volumeName", request.VolumeName,
			"poolUUID", request.PoolUUID,
			"action", request.Action)
		return err
	}

	// Handle different response types
	switch resp := response.(type) {
	case *coreapi.V1ExpertModeVolumeOK:
		logger.InfoContext(ctx, "Successfully submitted expert mode volume operation",
			"volumeName", request.VolumeName,
			"poolUUID", request.PoolUUID,
			"action", request.Action)
		return nil

	case *coreapi.V1ExpertModeVolumeBadRequest:
		logger.ErrorContext(ctx, "Bad request when submitting expert mode volume operation",
			"errorMessage", resp.Message,
			"volumeName", request.VolumeName,
			"action", request.Action)
		return customerrors.NewBadRequestErr(fmt.Sprintf("bad request: %s", resp.Message))

	case *coreapi.V1ExpertModeVolumeConflict:
		logger.ErrorContext(ctx, "Conflict when submitting expert mode volume operation",
			"errorMessage", resp.Message,
			"volumeName", request.VolumeName,
			"action", request.Action)
		return customerrors.NewConflictErr(fmt.Sprintf("conflict: %s", resp.Message))

	default:
		logger.ErrorContext(ctx, "Unexpected response from Core API",
			"responseType", fmt.Sprintf("%T", resp),
			"volumeName", request.VolumeName)
		return err
	}
}
