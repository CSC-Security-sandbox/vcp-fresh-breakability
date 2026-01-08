package middleware

import (
	"context"
	"fmt"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// SetupCredentialsForHandler sets up credentials for an ogen handler.
// Unlike CredentialMiddleware, this takes explicit parameters instead of
// extracting from request URL. This allows ogen handlers to call it directly
// with typed parameters from the OpenAPI spec.
//
// The JWT token is automatically extracted from context (set by auth.AuthMiddleware).
//
// Parameters:
//   - ctx: Request context (must have headers set by auth.AuthMiddleware)
//   - projectNumber: From path parameter (ogen params.ProjectNumber)
//   - poolID: From path parameter (ogen params.PoolId)
//   - credentialType: CredentialTypeAdmin or CredentialTypeGcnvAdmin
//
// Returns context with AuthDataKey set, ready for NewOntapClientFromContext().
func SetupCredentialsForHandler(
	ctx context.Context,
	projectNumber string,
	poolID string,
	credentialType string,
) (context.Context, error) {
	logger := util.GetLogger(ctx)

	// Get JWT token from context (set by auth.AuthMiddleware)
	jwtToken := getJWTFromContext(ctx)

	// Determine username based on credential type
	var userName string
	if credentialType == CredentialTypeAdmin {
		userName = AdminUserName
	} else {
		userName = env.ExpertModeUser
	}

	poolDetails := &models.PoolDetails{
		ProjectNumber: projectNumber,
		PoolID:        poolID,
		AccountName:   projectNumber,
		UserName:      userName,
	}

	cacheKey := generateCacheKey(projectNumber, poolID, userName)

	// Fetch and cache credentials (reuses existing logic)
	if err := fetchAndCacheCredentials(ctx, poolDetails, cacheKey, jwtToken, logger); err != nil {
		return ctx, fmt.Errorf("failed to setup credentials: %w", err)
	}

	logger.DebugContext(ctx, "Credentials setup for handler",
		"poolID", poolID,
		"projectNumber", projectNumber,
		"credentialType", credentialType,
		"cacheKey", cacheKey)

	return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
}

// getJWTFromContext extracts the JWT token from context.
// The token is stored by auth.AuthMiddleware in the request headers.
func getJWTFromContext(ctx context.Context) string {
	headers, ok := ctx.Value(utilsmiddleware.HeaderContextKey).(http.Header)
	if !ok || headers == nil {
		return ""
	}
	return headers.Get("Authorization")
}
