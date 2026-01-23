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
//   - credentialType: CredentialTypeAdmin or CredentialTypeExpertModeUser
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
	jwtToken := ExtractJWTFromContext(ctx)

	// Determine username based on IAM role mapping or credential type
	// Note: For SetupCredentialsForHandler, we need to extract headers from context
	// Since we don't have direct access to the request, we fall back to credential type
	var userName string
	if credentialType == CredentialTypeAdmin {
		userName = AdminUserName
	} else {
		req := &http.Request{}
		var err error
		// Try to get IAM role from context headers if available
		headers, ok := ctx.Value(utilsmiddleware.HeaderContextKey).(http.Header)
		if ok && headers != nil {
			// Create a temporary request to use determineUserNameFromRBAC
			// This is a workaround since SetupCredentialsForHandler doesn't have direct request access
			req.Header = headers
			req = req.WithContext(ctx)
			userName, err = determineUserNameFromRBAC(ctx, req)
			if err != nil {
				// Check if this is an IAM role validation error
				if isIAMRoleValidationError(err) {
					logger.ErrorContext(ctx, "IAM role validation failed", "error", err, "poolID", poolID, "projectNumber", projectNumber)
				} else {
					logger.ErrorContext(ctx, "Failed to determine user name from RBAC", "error", err, "poolID", poolID, "projectNumber", projectNumber)
				}
				return ctx, err
			}
		} else {
			// Headers not available in context
			if iamRoleToUserValidationEnabled {
				// Headers not available in context, but validation is enabled
				return ctx, fmt.Errorf("unable to determine IAM role from context headers")
			}
			// Headers not available and validation disabled - use default
			userName = env.ExpertModeUserSuffix
		}
	}
	poolDetails := &models.PoolDetails{
		ProjectNumber: projectNumber,
		PoolID:        poolID,
		AccountName:   projectNumber,
		UserName:      userName,
	}

	return SetupCredentialsCache(ctx, poolDetails, projectNumber, poolID, userName, jwtToken)
}

func SetupCredentialsCache(ctx context.Context, poolDetails *models.PoolDetails, projectNumber, poolID, userName, jwtToken string) (context.Context, error) {
	logger := util.GetLogger(ctx)
	cacheKey := generateCacheKey(projectNumber, poolID, userName)

	// Fetch and cache credentials (reuses existing logic)
	if err := fetchAndCacheCredentials(ctx, poolDetails, cacheKey, jwtToken, logger); err != nil {
		return ctx, fmt.Errorf("failed to fetch and cache credentials pooluuid: %s, error: %v", poolDetails.PoolID, err)
	}

	logger.DebugContext(ctx, "Credentials setup for handler", "poolID", poolID, "projectNumber", projectNumber, "cacheKey", cacheKey)
	return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
}
