package api

import (
	"context"
	"fmt"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
)

func (h Handler) V1betaDescribePool(ctx context.Context, params oasgenserver.V1GetPoolParams) (oasgenserver.V1GetPoolRes, error) {
	return &oasgenserver.V1GetPoolNotFound{
		Message: "Something went wrong",
		Code:    404,
	}, nil
}

func (h Handler) V1betaGetOntapCredentials(ctx context.Context, params oasgenserver.V1GetOntapCredentialsParams) (oasgenserver.V1GetOntapCredentialsRes, error) {
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

	return &oasgenserver.OntapCredentialsV1{
		SecretID:      oasgenserver.NewOptString(poolCredentials.SecretID),
		CertificateID: oasgenserver.NewOptString(poolCredentials.CertificateID),
		Password:      oasgenserver.NewOptString(poolCredentials.Password),
		AuthType:      oasgenserver.NewOptInt(poolCredentials.AuthType),
	}, nil
}
