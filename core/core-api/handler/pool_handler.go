package api

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
)

func (h Handler) V1betaDescribePool(ctx context.Context, params oasgenserver.V1GetPoolParams) (oasgenserver.V1GetPoolRes, error) {
	return &oasgenserver.V1GetPoolNotFound{
		Message: "Something went wrong",
		Code:    404,
	}, nil
}

func (h Handler) V1GetOntapCredentials(ctx context.Context, params oasgenserver.V1GetOntapCredentialsParams) (oasgenserver.V1GetOntapCredentialsRes, error) {
	// Get account name from query parameters
	if !params.AccountName.IsSet() {
		return &oasgenserver.V1GetOntapCredentialsBadRequest{
			Message: "Account name is required",
			Code:    400,
		}, nil
	}
	accountName := params.AccountName.Value

	poolCredentials, err := h.Orchestrator.GetExpertModePoolCreds(ctx, params.PoolId, accountName, params.UserName.Value)
	if err != nil {
		// Check if the error is ErrPoolNotFound and return 404
		var customErr *errors.CustomError
		if errors.As(err, &customErr) && customErr.IsError(errors.ErrPoolNotFound) {
			return &oasgenserver.V1GetOntapCredentialsNotFound{
				Message: "Pool not found",
				Code:    404,
			}, nil
		}
		return &oasgenserver.V1GetOntapCredentialsInternalServerError{
			Message: fmt.Sprintf("Failed to get pool credentials: %v", err),
			Code:    500,
		}, nil
	}

	if poolCredentials == nil {
		return &oasgenserver.V1GetOntapCredentialsNotFound{
			Message: "Pool credentials not found",
			Code:    404,
		}, nil
	}

	ontapCreds := convertUserCredentialsToOntapCredentialsV1(poolCredentials)
	return ontapCreds, nil
}

func convertUserCredentialsToOntapCredentialsV1(poolCredentials *models.UserCredentials) *oasgenserver.OntapCredentialsV1 {
	if poolCredentials == nil {
		return nil
	}

	secretID := oasgenserver.NewOptString(poolCredentials.SecretID)
	certificateID := oasgenserver.NewOptString(poolCredentials.CertificateID)
	password := oasgenserver.NewOptString(poolCredentials.Password)
	authType := oasgenserver.NewOptInt(poolCredentials.AuthType)

	var endpointMappings []oasgenserver.OntapEndpoint
	if poolCredentials.OntapEndpoints != nil {
		for _, mapping := range poolCredentials.OntapEndpoints {
			endpointMappings = append(endpointMappings, oasgenserver.OntapEndpoint{
				IP:  mapping.IP,
				DNS: mapping.DNS,
			})
		}
	}

	return &oasgenserver.OntapCredentialsV1{
		SecretID:       secretID,
		CertificateID:  certificateID,
		Password:       password,
		AuthType:       authType,
		OntapEndpoints: endpointMappings,
	}
}
