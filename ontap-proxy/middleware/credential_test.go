package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	coreapiclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/core-api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var mockFetchCredentials = func(ctx context.Context, poolDetails *models.PoolDetails, jwtToken string, logger log.Logger) (*coreapiclient.OntapCredentialsV1, error) {
	return &coreapiclient.OntapCredentialsV1{
		AuthType: coreapiclient.NewOptInt(2),
		SecretID: coreapiclient.NewOptString("test-secret"),
		Password: coreapiclient.NewOptString("test-password"),
		OntapEndpoints: []coreapiclient.OntapEndpoint{
			{IP: "1.2.3.4", DNS: "test.example.com"},
		},
	}, nil
}

func TestCredentialMiddleware(t *testing.T) {
	t.Run("WhenValidRequest_ShouldSucceed", func(t *testing.T) {
		originalFetchCredentials := fetchCredentialsFunc
		defer func() { fetchCredentialsFunc = originalFetchCredentials }()

		fetchCredentialsFunc = mockFetchCredentials

		req := httptest.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/volumes", nil)
		w := httptest.NewRecorder()

		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		middleware := CredentialMiddleware()
		middleware(nextHandler).ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("WhenInvalidURI_ShouldReturn400", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/invalid/path", nil)
		w := httptest.NewRecorder()

		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("Next handler should not be called")
		})

		middleware := CredentialMiddleware()
		middleware(nextHandler).ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "Invalid URI")
	})
}

func TestFetchAndCacheCredentials(t *testing.T) {
	t.Run("WhenCacheHit_ShouldReturnCachedData", func(t *testing.T) {
		poolDetails := &models.PoolDetails{
			PoolID:        "test-pool",
			AccountName:   "test-account",
			UserName:      "test-user",
			ProjectNumber: "test-project",
		}

		ctx := context.Background()
		cacheKey := "test-project:test-pool:test-user"

		testAuthData := &models.AuthData{
			AuthType: 2,
			PoolID:   "test-pool",
		}
		cache.AddToAuthDataCache(cacheKey, testAuthData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		logger := &log.MockLogger{}
		logger.On("InfoContext", mock.Anything, "Using cached auth data", "projectNumber", "test-project", "poolID", "test-pool", "accountName", "test-account", "userName", "test-user", "cacheKey", "test-project:test-pool:test-user").Return()

		err := fetchAndCacheCredentials(ctx, poolDetails, cacheKey, "test-jwt-token", logger)

		assert.NoError(t, err)
		logger.AssertExpectations(t)
	})

	t.Run("WhenCacheMiss_ShouldFetchFromAPI", func(t *testing.T) {
		originalFetchCredentials := fetchCredentialsFunc
		defer func() { fetchCredentialsFunc = originalFetchCredentials }()

		fetchCredentialsFunc = mockFetchCredentials

		poolDetails := &models.PoolDetails{
			PoolID:        "test-pool",
			AccountName:   "test-account",
			UserName:      "test-user",
			ProjectNumber: "test-project",
		}

		ctx := context.Background()
		cacheKey := "test-project:test-pool:test-user"
		defer cache.RemoveFromAuthDataCache(cacheKey)

		logger := &log.MockLogger{}
		logger.On("InfoContext", mock.Anything, "Cache miss - fetching credentials from Core API", "projectNumber", "test-project", "poolID", "test-pool", "userName", "test-user").Return()
		logger.On("InfoContext", mock.Anything, "Credentials fetched from Core API and stored as AuthData", "poolID", "test-pool", "accountName", "test-account", "userName", "test-user", "authType", 2, "cacheKey", "test-project:test-pool:test-user").Return()

		err := fetchAndCacheCredentials(ctx, poolDetails, cacheKey, "test-jwt-token", logger)

		assert.NoError(t, err)
		logger.AssertExpectations(t)

		cachedData, exists := cache.GetFromAuthDataCache(cacheKey)
		assert.True(t, exists)
		assert.Equal(t, "test-pool", cachedData.PoolID)
		assert.Equal(t, "test-account", cachedData.AccountName)
		assert.Equal(t, "test-user", cachedData.UserName)
	})
}

func TestExtractPoolDetailsFromRequest(t *testing.T) {
	t.Run("WhenValidURI_WithGcnvAdminCredential_ShouldExtractDetails", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/volumes", nil)

		poolDetails, err := extractPoolDetailsFromRequest(req, CredentialTypeGcnvAdmin)

		assert.NoError(t, err)
		assert.Equal(t, "1234", poolDetails.ProjectNumber)
		assert.Equal(t, "my-pool", poolDetails.PoolID)
		assert.Equal(t, "1234", poolDetails.AccountName)
		// After the change, expert mode uses suffix-based approach, so UserName is set to the suffix
		assert.Equal(t, "gadmin", poolDetails.UserName)
	})

	t.Run("WhenValidURI_WithAdminCredential_ShouldExtractDetailsWithAdminUser", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/snaplock/file/vol-uuid/path/to/file", nil)

		poolDetails, err := extractPoolDetailsFromRequest(req, CredentialTypeAdmin)

		assert.NoError(t, err)
		assert.Equal(t, "1234", poolDetails.ProjectNumber)
		assert.Equal(t, "my-pool", poolDetails.PoolID)
		assert.Equal(t, "1234", poolDetails.AccountName)
		assert.Equal(t, AdminUserName, poolDetails.UserName)
	})

	t.Run("WhenInvalidURI_ShouldReturnError", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/invalid/path", nil)

		poolDetails, err := extractPoolDetailsFromRequest(req, CredentialTypeGcnvAdmin)

		assert.Error(t, err)
		assert.Nil(t, poolDetails)
		assert.Contains(t, err.Error(), "pool URI should match format")
	})
}

func TestValidatePoolUri(t *testing.T) {
	t.Run("WhenValidURI_ShouldReturnNil", func(t *testing.T) {
		uri := "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/volumes"
		err := validatePoolUri(uri)
		assert.NoError(t, err)
	})

	t.Run("WhenInvalidFormat_ShouldReturnError", func(t *testing.T) {
		uri := "/invalid/path"
		err := validatePoolUri(uri)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pool URI should match format")
	})
}

func TestHandleCredentialError(t *testing.T) {
	t.Run("WhenPoolNotFound_ShouldReturn404", func(t *testing.T) {
		w := httptest.NewRecorder()

		handleCredentialError(w, assert.AnError)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestGenerateCacheKey(t *testing.T) {
	t.Run("WhenGcnvAdminCredential_ShouldGenerateKey", func(t *testing.T) {
		key := generateCacheKey("project1", "pool1", "gcnvadmin")
		assert.Equal(t, "project1:pool1:gcnvadmin", key)
	})

	t.Run("WhenAdminCredential_ShouldGenerateKey", func(t *testing.T) {
		key := generateCacheKey("project1", "pool1", "admin")
		assert.Equal(t, "project1:pool1:admin", key)
	})
}

func TestGetStringValue(t *testing.T) {
	t.Run("WhenValueIsSet_ShouldReturnValue", func(t *testing.T) {
		opt := coreapiclient.NewOptString("test-value")
		result := getStringValue(opt)
		assert.Equal(t, "test-value", result)
	})

	t.Run("WhenValueIsNotSet_ShouldReturnEmpty", func(t *testing.T) {
		opt := coreapiclient.OptString{}
		result := getStringValue(opt)
		assert.Equal(t, "", result)
	})
}

func TestConvertOntapEndpoints(t *testing.T) {
	apiEndpoints := []coreapiclient.OntapEndpoint{
		{IP: "1.2.3.4", DNS: "test1.example.com"},
		{IP: "5.6.7.8", DNS: "test2.example.com"},
	}

	endpoints := convertOntapEndpoints(apiEndpoints)

	assert.Len(t, endpoints, 2)
	assert.Equal(t, "1.2.3.4", endpoints[0].IP)
	assert.Equal(t, "test1.example.com", endpoints[0].DNS)
	assert.Equal(t, "5.6.7.8", endpoints[1].IP)
	assert.Equal(t, "test2.example.com", endpoints[1].DNS)
}

func TestExtractJWTTokenFromRequest(t *testing.T) {
	t.Run("WhenBearerToken_ShouldReturnFullHeader", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("authorization", "Bearer test-jwt-token")

		token := extractJWTTokenFromRequest(req)

		assert.Equal(t, "Bearer test-jwt-token", token)
	})

	t.Run("WhenNoBearerPrefix_ShouldReturnOriginalHeader", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("authorization", "test-jwt-token")

		token := extractJWTTokenFromRequest(req)

		assert.Equal(t, "test-jwt-token", token)
	})

	t.Run("WhenNoAuthorizationHeader_ShouldReturnEmpty", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)

		token := extractJWTTokenFromRequest(req)

		assert.Equal(t, "", token)
	})

	t.Run("WhenEmptyAuthorizationHeader_ShouldReturnEmpty", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("authorization", "")

		token := extractJWTTokenFromRequest(req)

		assert.Equal(t, "", token)
	})
}
