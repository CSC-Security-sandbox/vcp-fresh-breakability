package middleware

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	coreapiclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/core-api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/coreapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// Credential type constants
const (
	// CredentialTypeAdmin is used for admin operations (snaplock, EBR, litigation)
	// handled by ogen handlers which call SetupCredentialsForHandler() directly.
	CredentialTypeAdmin = "admin"

	// CredentialTypeExpertModeUser is used for passthrough operations via CredentialMiddleware.
	CredentialTypeExpertModeUser = "expertModeUser"

	// AdminUserName is the username used for admin credential type
	AdminUserName = "admin"
)

var (
	fetchCredentialsFunc = coreapi.FetchCredentials
	poolUriRegex         = "^/v1beta/projects/([^/]+)/locations/([^/]+)/pools/([^/]+)"
)

func CredentialMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := util.GetLogger(r.Context())

			// Passthrough routes always use gcnvadmin credentials.
			// Admin operations (snaplock, EBR, litigation) are handled by ogen handlers
			// which call SetupCredentialsForHandler() directly with CredentialTypeAdmin.
			poolDetails, err := extractPoolDetailsFromRequest(r, CredentialTypeExpertModeUser)
			if err != nil {
				logger.ErrorContext(r.Context(), "Failed to extract pool details", "error", err, "path", r.URL.Path)
				// Check if this is an IAM role validation error vs URI validation error
				if isIAMRoleValidationError(err) {
					handleCredentialError(w, err)
				} else {
					// URI validation error
					http.Error(w, "Invalid URI", http.StatusBadRequest)
				}
				return
			}
			jwtToken := extractJWTTokenFromRequest(r)

			ctx, err := SetupCredentialsCache(r.Context(), poolDetails, poolDetails.ProjectNumber, poolDetails.PoolID, poolDetails.UserName, jwtToken)
			if err != nil {
				logger.ErrorContext(r.Context(), "Failed to fetch and cache credentials", "error", err, "poolID", poolDetails.PoolID)
				handleCredentialError(w, err)
				return
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func fetchAndCacheCredentials(ctx context.Context, poolDetails *models.PoolDetails, cacheKey string, jwtToken string, logger log.Logger) error {
	if _, exists := cache.GetFromAuthDataCache(cacheKey); exists {
		logger.InfoContext(ctx, "Using cached auth data",
			"projectNumber", poolDetails.ProjectNumber,
			"poolID", poolDetails.PoolID,
			"accountName", poolDetails.AccountName,
			"userName", poolDetails.UserName,
			"cacheKey", cacheKey)
		return nil
	}

	logger.InfoContext(ctx, "Cache miss - fetching credentials from Core API",
		"projectNumber", poolDetails.ProjectNumber,
		"poolID", poolDetails.PoolID,
		"userName", poolDetails.UserName)

	credentials, err := fetchCredentialsFunc(ctx, poolDetails, jwtToken, logger)
	if err != nil {
		return fmt.Errorf("failed to fetch credentials: %w", err)
	}

	authData := &models.AuthData{
		AuthType:       credentials.AuthType.Value,
		SecretID:       getStringValue(credentials.SecretID),
		CertificateID:  getStringValue(credentials.CertificateID),
		Password:       getStringValue(credentials.Password),
		PoolID:         poolDetails.PoolID,
		AccountName:    poolDetails.AccountName,
		Username:       getStringValue(credentials.Username),
		OntapEndpoints: convertOntapEndpoints(credentials.OntapEndpoints),
		CaURI:          getStringValue(credentials.CaURI),
	}

	cache.AddToAuthDataCache(cacheKey, authData)

	logger.InfoContext(ctx, "Credentials fetched from Core API and stored as AuthData",
		"poolID", authData.PoolID,
		"accountName", authData.AccountName,
		"userName", authData.Username,
		"authType", authData.AuthType,
		"cacheKey", cacheKey)

	return nil
}

func handleCredentialError(w http.ResponseWriter, err error) {
	errorMsg := err.Error()

	switch {
	case contains(errorMsg, "pool not found"):
		http.Error(w, "Pool not found", http.StatusNotFound)
	case contains(errorMsg, "invalid pool details"):
		http.Error(w, "Invalid pool details", http.StatusBadRequest)
	case contains(errorMsg, "unable to determine IAM role from context headers"):
		// IAM role validation error - missing or invalid IAM role header
		http.Error(w, "Unauthorized: Unable to determine IAM role from request headers", http.StatusUnauthorized)
	case contains(errorMsg, "unauthorized access"):
		http.Error(w, "Unauthorized access", http.StatusUnauthorized)
	case contains(errorMsg, "forbidden access"):
		http.Error(w, "Forbidden access", http.StatusForbidden)
	case contains(errorMsg, "internal server error"):
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	case contains(errorMsg, "core API call failed"):
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
	default:
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// isIAMRoleValidationError checks if the error is related to IAM role validation
// (missing IAM role header) rather than URI validation
func isIAMRoleValidationError(err error) bool {
	if err == nil {
		return false
	}
	errorMsg := err.Error()
	return contains(errorMsg, "unable to determine IAM role from context headers")
}

func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

func generateCacheKey(projectNumber, poolID, userName string) string {
	return fmt.Sprintf("%s:%s:%s", projectNumber, poolID, userName)
}

func getStringValue(opt coreapiclient.OptString) string {
	if opt.IsSet() {
		return opt.Value
	}
	return ""
}

func convertOntapEndpoints(apiEndpoints []coreapiclient.OntapEndpoint) []models.OntapEndpoint {
	var endpoints []models.OntapEndpoint
	for _, apiEndpoint := range apiEndpoints {
		endpoints = append(endpoints, models.OntapEndpoint{
			IP:  apiEndpoint.IP,
			DNS: apiEndpoint.DNS,
		})
	}
	return endpoints
}

// determineUserNameFromRBAC determines the username based on RBAC user role from the request
func determineUserNameFromRBAC(ctx context.Context, req *http.Request) (string, error) {
	roleUserName := GetRBACUserFromRequest(ctx, req)
	switch roleUserName {
	case "":
		if iamRoleToUserValidationEnabled {
			// Headers not available in context, but validation is enabled
			return "", fmt.Errorf("unable to determine IAM role from context headers")
		}
		return env.ExpertModeUserSuffix, nil
	case env.PrivExpertModeUserSuffix:
		// Privileged Mode User map to Admin for now
		return AdminUserName, nil
	default:
		return roleUserName, nil
	}
}

func extractPoolDetailsFromRequest(req *http.Request, credentialType string) (*models.PoolDetails, error) {
	uri := req.URL.Path

	err := validatePoolUri(uri)
	if err != nil {
		return nil, err
	}

	uriSlice := strings.Split(uri, "/")
	projectNumber := uriSlice[3]
	_ = uriSlice[5]
	poolID := uriSlice[7]

	accountName := projectNumber

	var userName string
	if credentialType == CredentialTypeAdmin {
		userName = AdminUserName
	} else {
		userName, err = determineUserNameFromRBAC(req.Context(), req)
		if err != nil {
			return nil, err
		}
	}

	return &models.PoolDetails{
		ProjectNumber: projectNumber,
		PoolID:        poolID,
		AccountName:   accountName,
		UserName:      userName,
	}, nil
}

func validatePoolUri(uri string) error {
	compiledRegex := regexp.MustCompile(poolUriRegex)
	valid := compiledRegex.MatchString(uri)
	if !valid {
		return fmt.Errorf("pool URI should match format: /v1beta/projects/project_number/locations/location_id/pools/pool_id, got: %s", uri)
	}

	uriList := strings.Split(uri, "/")
	if len(uriList) < 8 {
		return fmt.Errorf("pool URI should have at least 8 path segments, got: %s", uri)
	}

	return nil
}

func extractJWTTokenFromRequest(req *http.Request) string {
	authorizationHeader := req.Header.Get("authorization")
	if authorizationHeader == "" {
		return ""
	}

	tokenString := strings.TrimPrefix(authorizationHeader, "Bearer ")
	if tokenString == authorizationHeader {
		return authorizationHeader
	}
	return authorizationHeader
}
