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

var (
	fetchCredentialsFunc = coreapi.FetchCredentials
	poolUriRegex         = "^/v1beta/projects/([^/]+)/locations/([^/]+)/pools/([^/]+)"
)

func CredentialMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := util.GetLogger(r.Context())

			poolDetails, err := extractPoolDetailsFromRequest(r)
			if err != nil {
				logger.ErrorContext(r.Context(), "Failed to extract pool details", "error", err, "path", r.URL.Path)
				http.Error(w, "Invalid URI", http.StatusBadRequest)
				return
			}

			cacheKey := generateCacheKey(poolDetails.ProjectNumber, poolDetails.PoolID, poolDetails.UserName)

			jwtToken := extractJWTTokenFromRequest(r)
			err = fetchAndCacheCredentials(r.Context(), poolDetails, cacheKey, jwtToken, logger)
			if err != nil {
				logger.ErrorContext(r.Context(), "Failed to fetch and cache credentials", "error", err, "poolID", poolDetails.PoolID)
				handleCredentialError(w, err)
				return
			}

			logger.DebugContext(r.Context(), "Credentials processed successfully", "poolID", poolDetails.PoolID, "cacheKey", cacheKey)
			ctx := context.WithValue(r.Context(), models.AuthDataKey, cacheKey)
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
		Username:       poolDetails.UserName,
		PoolID:         poolDetails.PoolID,
		AccountName:    poolDetails.AccountName,
		UserName:       poolDetails.UserName,
		OntapEndpoints: convertOntapEndpoints(credentials.OntapEndpoints),
	}

	cache.AddToAuthDataCache(cacheKey, authData)

	logger.InfoContext(ctx, "Credentials fetched from Core API and stored as AuthData",
		"poolID", poolDetails.PoolID,
		"accountName", poolDetails.AccountName,
		"userName", poolDetails.UserName,
		"authType", credentials.AuthType.Value,
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

func extractPoolDetailsFromRequest(req *http.Request) (*models.PoolDetails, error) {
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
	userName := env.ExpertModeUser

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
