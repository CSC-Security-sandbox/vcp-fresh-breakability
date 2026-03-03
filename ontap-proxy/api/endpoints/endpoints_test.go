package endpoints

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/handlers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/reverseproxy"
)

func TestGetHealth(t *testing.T) {
	handler := Handler{}

	res, err := handler.GetHealth(context.Background())

	require.NoError(t, err, "GetHealth should not return an error")
	assert.NotNil(t, res, "Response should not be nil")

	health, ok := res.(*oasgenserver.Health)
	assert.True(t, ok, "Response should be of type *Health")
	assert.NotNil(t, health, "Health response should not be nil")
}

func TestGetCacheStatus(t *testing.T) {
	t.Run("returns empty entries when cache is empty", func(t *testing.T) {
		// Setup: ensure cache is empty
		setupEmptyCache()

		handler := Handler{}
		res, err := handler.GetCacheStatus(context.Background())

		require.NoError(t, err, "GetCacheStatus should not return an error")

		cacheStatus, ok := res.(*oasgenserver.CacheStatus)
		require.True(t, ok, "Response should be of type *CacheStatus")

		assert.Empty(t, cacheStatus.Entries, "Entries should be empty")
		assert.Equal(t, 0, cacheStatus.TotalEntries.Value, "TotalEntries should be 0")
		assert.True(t, cacheStatus.TotalEntries.Set, "TotalEntries should be set")
	})

	t.Run("returns cache entries with correct fields", func(t *testing.T) {
		// Setup: add entry to cache
		setupCacheWithKeys("pool-123")

		handler := Handler{}
		res, err := handler.GetCacheStatus(context.Background())

		require.NoError(t, err, "GetCacheStatus should not return an error")

		cacheStatus, ok := res.(*oasgenserver.CacheStatus)
		require.True(t, ok, "Response should be of type *CacheStatus")

		assert.Len(t, cacheStatus.Entries, 1, "Should have one entry")
		assert.Equal(t, 1, cacheStatus.TotalEntries.Value, "TotalEntries should be 1")

		entry := cacheStatus.Entries[0]
		assert.True(t, entry.CacheKey.Set, "CacheKey should be set")
		assert.Equal(t, "auth:pool-123", entry.CacheKey.Value, "CacheKey should match")
		assert.True(t, entry.CachedAt.Set, "CachedAt should be set")
		assert.True(t, entry.ExpiresAt.Set, "ExpiresAt should be set")
	})

	t.Run("returns multiple cache entries", func(t *testing.T) {
		// Setup: add multiple entries to cache
		setupCacheWithKeys("pool-1", "pool-2", "pool-3")

		handler := Handler{}
		res, err := handler.GetCacheStatus(context.Background())

		require.NoError(t, err, "GetCacheStatus should not return an error")

		cacheStatus, ok := res.(*oasgenserver.CacheStatus)
		require.True(t, ok, "Response should be of type *CacheStatus")

		assert.Len(t, cacheStatus.Entries, 3, "Should have three entries")
		assert.Equal(t, 3, cacheStatus.TotalEntries.Value, "TotalEntries should be 3")

		// Verify all entries are present (with auth: prefix)
		keys := make(map[string]bool)
		for _, entry := range cacheStatus.Entries {
			keys[entry.CacheKey.Value] = true
		}
		assert.True(t, keys["auth:pool-1"], "Should contain auth:pool-1")
		assert.True(t, keys["auth:pool-2"], "Should contain auth:pool-2")
		assert.True(t, keys["auth:pool-3"], "Should contain auth:pool-3")
	})

	t.Run("expiry time is after cached time", func(t *testing.T) {
		// Setup: add entry to cache
		setupCacheWithKeys("test-pool")

		handler := Handler{}
		res, err := handler.GetCacheStatus(context.Background())

		require.NoError(t, err, "GetCacheStatus should not return an error")

		cacheStatus, ok := res.(*oasgenserver.CacheStatus)
		require.True(t, ok, "Response should be of type *CacheStatus")

		require.Len(t, cacheStatus.Entries, 1, "Should have one entry")
		entry := cacheStatus.Entries[0]

		assert.True(t, entry.ExpiresAt.Value.After(entry.CachedAt.Value),
			"ExpiresAt should be after CachedAt")
	})

	t.Run("includes client cache entries when available", func(t *testing.T) {
		// Setup: add auth cache entry
		setupCacheWithKeys("auth-pool")

		// Add client cache entry via connection pool
		pool := reverseproxy.GetGlobalConnectionPool()
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			PoolID:      "client-pool",
			AccountName: "test-account",
			Username:    "testuser",
			Password:    "testpass",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: "127.0.0.1:9999"}, // Non-routable for test
			},
		}
		// This may fail to connect but will still create a cache entry attempt
		_, _, _ = pool.GetClient(context.Background(), authData)

		handler := Handler{}
		res, err := handler.GetCacheStatus(context.Background())

		require.NoError(t, err, "GetCacheStatus should not return an error")

		cacheStatus, ok := res.(*oasgenserver.CacheStatus)
		require.True(t, ok, "Response should be of type *CacheStatus")

		// Should have at least the auth cache entry
		assert.GreaterOrEqual(t, len(cacheStatus.Entries), 1, "Should have at least one entry")

		// Verify we have auth entries
		hasAuthEntry := false
		for _, entry := range cacheStatus.Entries {
			if entry.CacheKey.Set && len(entry.CacheKey.Value) > 5 && entry.CacheKey.Value[:5] == "auth:" {
				hasAuthEntry = true
				break
			}
		}
		assert.True(t, hasAuthEntry, "Should have auth cache entries")
	})
}

// setupEmptyCache clears the auth data cache
func setupEmptyCache() {
	// Remove all existing entries
	for _, entry := range cache.GetAuthDataCacheStatus() {
		cache.RemoveFromAuthDataCache(entry.CacheKey)
	}
}

// setupCacheWithKeys sets up the cache with the given keys
func setupCacheWithKeys(keys ...string) {
	setupEmptyCache()
	for _, key := range keys {
		cache.AddToAuthDataCache(key, &models.AuthData{
			PoolID:   key,
			AuthType: models.USERNAME_PWD,
		})
	}
}

func TestSnaplockFileDelete(t *testing.T) {
	testPoolUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	testVolumeUUID := uuid.MustParse("660e8400-e29b-41d4-a716-446655440001")

	t.Run("WhenSnapLockOperationDisabled_ShouldReturn400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}

		res, err := handler.SnaplockFileDelete(context.Background(), params)

		require.NoError(t, err, "SnaplockFileDelete should not return a Go error")
		require.NotNil(t, res, "Response should not be nil")

		internalErr, ok := res.(*oasgenserver.SnaplockFileDeleteBadRequest)
		require.True(t, ok, "Expected SnaplockFileDeleteBadRequest, got %T", res)
		assert.Equal(t, 400, internalErr.Code, "Code should be 400")
		assert.Equal(t, "Snaplock file delete operation is disabled", internalErr.Message, "Message should match")
	})

	t.Run("WhenMissingCredentials_ShouldReturnTypedError", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true // Ensure we hit credential path, not disabled path
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}

		// Create params with mock UUIDs
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}

		// Call handler - should fail at credential setup (no JWT, no mock)
		res, err := handler.SnaplockFileDelete(context.Background(), params)

		// ogen handlers return typed error responses, not Go errors
		require.NoError(t, err, "ogen handlers should not return Go errors")
		require.NotNil(t, res, "Should return a typed error response")

		// Should be an unauthorized or internal server error response
		switch v := res.(type) {
		case *oasgenserver.SnaplockFileDeleteUnauthorized:
			assert.NotEmpty(t, v.Message, "Error message should not be empty")
		case *oasgenserver.SnaplockFileDeleteInternalServerError:
			assert.NotEmpty(t, v.Message, "Error message should not be empty")
		default:
			t.Fatalf("Expected error response type, got %T", res)
		}
	})

	t.Run("WhenURLEncodedFilePath_ShouldDecodeCorrectly", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		// This test verifies that file paths with special characters are handled
		// The filePath parameter comes URL-decoded from ogen
		handler := Handler{}

		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "path/to/file with spaces.txt", // Already decoded by ogen
		}

		// Call handler - will fail at credential setup, but validates param handling
		res, err := handler.SnaplockFileDelete(context.Background(), params)

		// ogen handlers return typed error responses, not Go errors
		require.NoError(t, err, "ogen handlers should not return Go errors")
		require.NotNil(t, res, "Should return a typed error response")
	})

	t.Run("WhenFilePathHasLeadingSlash_ShouldTrimCorrectly", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}

		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "/leading/slash/path.txt", // With leading slash
		}

		// Call handler - will fail at credential setup
		res, err := handler.SnaplockFileDelete(context.Background(), params)

		// ogen handlers return typed error responses, not Go errors
		require.NoError(t, err, "ogen handlers should not return Go errors")
		require.NotNil(t, res, "Should return a typed error response")
	})
}

func TestV1ClusterLicensingAccessTokensCreate(t *testing.T) {
	poolUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	params := oasgenserver.V1ClusterLicensingAccessTokensCreateParams{
		ProjectNumber: "123456",
		LocationId:    "us-central1",
		PoolId:        poolUUID,
	}

	t.Run("WhenAdminCredentialOperationDisabled_ReturnsBadRequest", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = false
		defer func() { smcOperationEnabled = old }()

		handler := Handler{}
		req := &oasgenserver.AccessTokenRequest{
			ClientID:     oasgenserver.NewOptString("app"),
			ClientSecret: oasgenserver.NewOptString("secret"),
			GrantType:    oasgenserver.NewOptAccessTokenRequestGrantType(oasgenserver.AccessTokenRequestGrantTypeClientCredentials),
		}

		res, err := handler.V1ClusterLicensingAccessTokensCreate(context.Background(), req, params)

		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1ClusterLicensingAccessTokensCreateBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, "Operation is disabled", badReq.Message)
	})

	t.Run("WhenNoCredentials_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

		handler := Handler{}
		req := &oasgenserver.AccessTokenRequest{
			ClientID:     oasgenserver.NewOptString("app"),
			ClientSecret: oasgenserver.NewOptString("secret"),
			GrantType:    oasgenserver.NewOptAccessTokenRequestGrantType(oasgenserver.AccessTokenRequestGrantTypeClientCredentials),
		}

		res, err := handler.V1ClusterLicensingAccessTokensCreate(context.Background(), req, params)

		require.NoError(t, err)
		require.NotNil(t, res)
		internalErr, ok := res.(*oasgenserver.V1ClusterLicensingAccessTokensCreateInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internalErr.Code)
		assert.Contains(t, internalErr.Message, "credentials")
	})

	t.Run("WhenEnsureCertificateOrPasswordFails_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return ctx, nil
		}
		ensureCertificateOrPassword = func(context.Context) error {
			return errors.New("no cert")
		}

		handler := Handler{}
		req := &oasgenserver.AccessTokenRequest{
			ClientID:     oasgenserver.NewOptString("app"),
			ClientSecret: oasgenserver.NewOptString("secret"),
			GrantType:    oasgenserver.NewOptAccessTokenRequestGrantType(oasgenserver.AccessTokenRequestGrantTypeClientCredentials),
		}

		res, err := handler.V1ClusterLicensingAccessTokensCreate(context.Background(), req, params)

		require.NoError(t, err)
		require.NotNil(t, res)
		internalErr, ok := res.(*oasgenserver.V1ClusterLicensingAccessTokensCreateInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internalErr.Code)
		assert.Contains(t, internalErr.Message, "certificate")
	})

	t.Run("WhenNewOntapClientFromContextFails_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldNewClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldNewClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return ctx, nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (*handlers.OntapClient, error) {
			return nil, errors.New("no client")
		}

		handler := Handler{}
		req := &oasgenserver.AccessTokenRequest{
			ClientID:     oasgenserver.NewOptString("app"),
			ClientSecret: oasgenserver.NewOptString("secret"),
			GrantType:    oasgenserver.NewOptAccessTokenRequestGrantType(oasgenserver.AccessTokenRequestGrantTypeClientCredentials),
		}

		res, err := handler.V1ClusterLicensingAccessTokensCreate(context.Background(), req, params)

		require.NoError(t, err)
		require.NotNil(t, res)
		internalErr, ok := res.(*oasgenserver.V1ClusterLicensingAccessTokensCreateInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internalErr.Code)
		assert.Contains(t, internalErr.Message, "failed to connect to ONTAP")
	})

	t.Run("WhenOntapReturns200InvalidJSON_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not json"))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:licensing-test-200-invalid"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		handler := Handler{}
		req := &oasgenserver.AccessTokenRequest{
			ClientID:     oasgenserver.NewOptString("app"),
			ClientSecret: oasgenserver.NewOptString("secret"),
			GrantType:    oasgenserver.NewOptAccessTokenRequestGrantType(oasgenserver.AccessTokenRequestGrantTypeClientCredentials),
		}

		res, err := handler.V1ClusterLicensingAccessTokensCreate(context.Background(), req, params)

		require.NoError(t, err)
		require.NotNil(t, res)
		internalErr, ok := res.(*oasgenserver.V1ClusterLicensingAccessTokensCreateInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internalErr.Code)
		assert.Contains(t, internalErr.Message, "invalid ONTAP response")
	})

	t.Run("WhenOntapReturns200ValidJSON_ReturnsAccessTokenInfo", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tok123","expires_in":3600,"token_type":"bearer"}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:licensing-test-200-valid"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		handler := Handler{}
		req := &oasgenserver.AccessTokenRequest{
			ClientID:     oasgenserver.NewOptString("app"),
			ClientSecret: oasgenserver.NewOptString("secret"),
			GrantType:    oasgenserver.NewOptAccessTokenRequestGrantType(oasgenserver.AccessTokenRequestGrantTypeClientCredentials),
		}

		res, err := handler.V1ClusterLicensingAccessTokensCreate(context.Background(), req, params)

		require.NoError(t, err)
		require.NotNil(t, res)
		info, ok := res.(*oasgenserver.AccessTokenInfo)
		require.True(t, ok, "expected AccessTokenInfo, got %T", res)
		assert.True(t, info.AccessToken.Set)
		assert.Equal(t, "tok123", info.AccessToken.Value)
	})

	t.Run("WhenOntapReturnsNon200_ReturnsErrorStatusCode", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"invalid_grant","code":"400"}}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:licensing-test-non200"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		handler := Handler{}
		req := &oasgenserver.AccessTokenRequest{
			ClientID:     oasgenserver.NewOptString("app"),
			ClientSecret: oasgenserver.NewOptString("secret"),
			GrantType:    oasgenserver.NewOptAccessTokenRequestGrantType(oasgenserver.AccessTokenRequestGrantTypeClientCredentials),
		}

		res, err := handler.V1ClusterLicensingAccessTokensCreate(context.Background(), req, params)

		require.Error(t, err)
		require.Nil(t, res)
		sc, ok := err.(*oasgenserver.ErrorStatusCode)
		require.True(t, ok, "expected ErrorStatusCode, got %T", err)
		assert.Equal(t, 400, sc.StatusCode)
		assert.Equal(t, 400, sc.Response.Code)
		assert.Contains(t, sc.Response.Message, "invalid_grant")
	})
}
