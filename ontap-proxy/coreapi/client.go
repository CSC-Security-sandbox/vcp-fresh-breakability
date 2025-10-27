package coreapi

import (
	"context"
	"fmt"

	coreapi "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/core-api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
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
		logger.InfoContext(ctx, "Successfully validated pool and got credentials",
			"poolID", poolDetails.PoolID,
			"authType", resp.AuthType.Value)
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
