package api

import (
	"context"
	"fmt"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
)

func (h Handler) V1GetOntapCredentials(ctx context.Context, params oasgenserver.V1GetOntapCredentialsParams) (oasgenserver.V1GetOntapCredentialsRes, error) {
	// Get account name from query parameters
	if !params.AccountName.IsSet() {
		return &oasgenserver.V1GetOntapCredentialsBadRequest{
			Message: "Account name is required",
			Code:    400,
		}, nil
	}
	accountName := params.AccountName.Value
	expertModeCredential, err := h.Orchestrator.GetExpertModePoolCreds(ctx, params.PoolId, accountName, params.UserName.Value)
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

	if expertModeCredential == nil {
		return &oasgenserver.V1GetOntapCredentialsNotFound{
			Message: "Pool credentials not found",
			Code:    404,
		}, nil
	}

	ontapCreds := convertUserCredentialsToOntapCredentialsV1(expertModeCredential)
	return ontapCreds, nil
}

func convertUserCredentialsToOntapCredentialsV1(expertModeCredentials *models.UserCredentials) *oasgenserver.OntapCredentialsV1 {
	if expertModeCredentials == nil {
		return nil
	}

	secretID := oasgenserver.NewOptString(expertModeCredentials.SecretID)
	certificateID := oasgenserver.NewOptString(expertModeCredentials.CertificateID)
	password := oasgenserver.NewOptString(expertModeCredentials.Password)
	authType := oasgenserver.NewOptInt(expertModeCredentials.AuthType)
	username := oasgenserver.NewOptString(expertModeCredentials.Username)

	var endpointMappings []oasgenserver.OntapEndpoint
	if expertModeCredentials.OntapEndpoints != nil {
		for _, mapping := range expertModeCredentials.OntapEndpoints {
			endpointMappings = append(endpointMappings, oasgenserver.OntapEndpoint{
				IP:  mapping.IP,
				DNS: mapping.DNS,
			})
		}
	}

	caURI := oasgenserver.NewOptString(expertModeCredentials.GetCaURIWithFallback())

	return &oasgenserver.OntapCredentialsV1{
		SecretID:       secretID,
		CertificateID:  certificateID,
		Password:       password,
		AuthType:       authType,
		OntapEndpoints: endpointMappings,
		CaURI:          caURI,
		Username:       username,
	}
}
