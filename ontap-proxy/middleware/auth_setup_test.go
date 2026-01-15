package middleware

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	coreapiclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/core-api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestSetupCredentialsForHandler(t *testing.T) {
	t.Run("WhenValidParams_WithAdminCredential_ShouldSucceed", func(t *testing.T) {
		originalFetchCredentials := fetchCredentialsFunc
		defer func() { fetchCredentialsFunc = originalFetchCredentials }()

		fetchCredentialsFunc = mockFetchCredentials

		ctx := context.Background()

		resultCtx, err := SetupCredentialsForHandler(ctx, "1234", "test-pool", CredentialTypeAdmin)

		assert.NoError(t, err)
		assert.NotNil(t, resultCtx)

		// Verify cache key is set in context
		cacheKey := cache.GetAuthDataKeyFromContext(resultCtx)
		assert.Equal(t, "1234:test-pool:admin", cacheKey)

		// Verify auth data is in cache
		authData, exists := cache.GetFromAuthDataCache(cacheKey)
		assert.True(t, exists)
		assert.Equal(t, "test-pool", authData.PoolID)

		// Cleanup
		cache.RemoveFromAuthDataCache(cacheKey)
	})

	t.Run("WhenValidParams_WithGcnvAdminCredential_ShouldSucceed", func(t *testing.T) {
		originalFetchCredentials := fetchCredentialsFunc
		defer func() { fetchCredentialsFunc = originalFetchCredentials }()

		fetchCredentialsFunc = mockFetchCredentials

		ctx := context.Background()

		resultCtx, err := SetupCredentialsForHandler(ctx, "5678", "another-pool", CredentialTypeGcnvAdmin)

		assert.NoError(t, err)
		assert.NotNil(t, resultCtx)

		// Verify cache key is set in context
		cacheKey := cache.GetAuthDataKeyFromContext(resultCtx)
		assert.Equal(t, "5678:another-pool:gadmin", cacheKey)

		// Cleanup
		cache.RemoveFromAuthDataCache(cacheKey)
	})

	t.Run("WhenCacheHit_ShouldUseCachedData", func(t *testing.T) {
		cacheKey := "cached-project:cached-pool:admin"
		testAuthData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			PoolID:   "cached-pool",
		}
		cache.AddToAuthDataCache(cacheKey, testAuthData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.Background()

		resultCtx, err := SetupCredentialsForHandler(ctx, "cached-project", "cached-pool", CredentialTypeAdmin)

		assert.NoError(t, err)
		assert.NotNil(t, resultCtx)

		// Verify cache key is set correctly
		resultCacheKey := cache.GetAuthDataKeyFromContext(resultCtx)
		assert.Equal(t, cacheKey, resultCacheKey)
	})

	t.Run("WhenFetchCredentialsFails_ShouldReturnError", func(t *testing.T) {
		originalFetchCredentials := fetchCredentialsFunc
		defer func() { fetchCredentialsFunc = originalFetchCredentials }()

		fetchCredentialsFunc = func(ctx context.Context, poolDetails *models.PoolDetails, jwtToken string, logger log.Logger) (*coreapiclient.OntapCredentialsV1, error) {
			return nil, assert.AnError
		}

		ctx := context.Background()

		resultCtx, err := SetupCredentialsForHandler(ctx, "fail-project", "fail-pool", CredentialTypeAdmin)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to setup credentials")
		// Context should still be returned (even if unchanged)
		assert.NotNil(t, resultCtx)
	})
}

func TestGetJWTFromContext(t *testing.T) {
	t.Run("WhenHeadersInContext_ShouldReturnAuthorizationHeader", func(t *testing.T) {
		headers := http.Header{}
		headers.Set("Authorization", "Bearer test-jwt-token")

		ctx := context.WithValue(context.Background(), utilsmiddleware.HeaderContextKey, headers)

		token := getJWTFromContext(ctx)

		assert.Equal(t, "Bearer test-jwt-token", token)
	})

	t.Run("WhenNoHeadersInContext_ShouldReturnEmpty", func(t *testing.T) {
		ctx := context.Background()

		token := getJWTFromContext(ctx)

		assert.Equal(t, "", token)
	})

	t.Run("WhenHeadersAreNil_ShouldReturnEmpty", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), utilsmiddleware.HeaderContextKey, (http.Header)(nil))

		token := getJWTFromContext(ctx)

		assert.Equal(t, "", token)
	})

	t.Run("WhenAuthorizationHeaderIsEmpty_ShouldReturnEmpty", func(t *testing.T) {
		headers := http.Header{}
		ctx := context.WithValue(context.Background(), utilsmiddleware.HeaderContextKey, headers)

		token := getJWTFromContext(ctx)

		assert.Equal(t, "", token)
	})

	t.Run("WhenWrongTypeInContext_ShouldReturnEmpty", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), utilsmiddleware.HeaderContextKey, "not-headers")

		token := getJWTFromContext(ctx)

		assert.Equal(t, "", token)
	})
}
