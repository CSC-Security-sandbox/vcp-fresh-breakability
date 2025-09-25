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
	logger.Info("Fetching credentials from Core API",
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
		logger.Error("Core API call failed", "error", err)
		return nil, fmt.Errorf("core API call failed: %w", err)
	}

	switch resp := response.(type) {
	case *coreapi.OntapCredentialsV1:
		logger.Info("Successfully validated pool and got credentials",
			"poolID", poolDetails.PoolID,
			"authType", resp.AuthType.Value)
		return resp, nil

	case *coreapi.V1GetOntapCredentialsNotFound:
		logger.Error("Pool not found", "poolID", poolDetails.PoolID, "message", resp.Message)
		return nil, fmt.Errorf("pool not found: %s", resp.Message)

	case *coreapi.V1GetOntapCredentialsBadRequest:
		logger.Error("Invalid pool details", "poolID", poolDetails.PoolID, "message", resp.Message)
		return nil, fmt.Errorf("invalid pool details: %s", resp.Message)

	case *coreapi.V1GetOntapCredentialsUnauthorized:
		logger.Error("Unauthorized access", "poolID", poolDetails.PoolID, "message", resp.Message)
		return nil, fmt.Errorf("unauthorized access: %s", resp.Message)

	case *coreapi.V1GetOntapCredentialsForbidden:
		logger.Error("Forbidden access", "poolID", poolDetails.PoolID, "message", resp.Message)
		return nil, fmt.Errorf("forbidden access: %s", resp.Message)

	case *coreapi.V1GetOntapCredentialsInternalServerError:
		logger.Error("Internal server error", "poolID", poolDetails.PoolID, "message", resp.Message)
		return nil, fmt.Errorf("internal server error: %s", resp.Message)

	default:
		logger.Error("Unexpected response from Core API", "responseType", fmt.Sprintf("%T", resp))
		return nil, fmt.Errorf("unexpected response from Core API")
	}
}
