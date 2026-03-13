package endpoints

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/handlers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
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
		setupCacheWithKeys("test-pool")
		handler := Handler{}
		res, err := handler.GetCacheStatus(context.Background())
		require.NoError(t, err)
		cacheStatus, ok := res.(*oasgenserver.CacheStatus)
		require.True(t, ok)
		require.Len(t, cacheStatus.Entries, 1)
		assert.True(t, cacheStatus.Entries[0].ExpiresAt.Value.After(cacheStatus.Entries[0].CachedAt.Value))
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

// contextWithSnaplockIAMRequest returns a context with headers (as auth middleware sets) and ManageSnaplockRole.
func contextWithSnaplockIAMRequest(t *testing.T) context.Context {
	t.Helper()
	return contextWithSnaplockIAMRequestForRole(t, middleware.ManageSnaplockRole)
}

// contextWithSnaplockIAMRequestForRole returns a context with the IAM role header set, mirroring auth middleware.
func contextWithSnaplockIAMRequestForRole(t *testing.T, role string) context.Context {
	t.Helper()
	headers := make(http.Header)
	headers.Set(middleware.IAMRoleHeader, role)
	return context.WithValue(context.Background(), utilsmiddleware.HeaderContextKey, headers)
}

func TestSnaplockFileDelete(t *testing.T) {
	testPoolUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	testVolumeUUID := uuid.MustParse("660e8400-e29b-41d4-a716-446655440001")

	t.Run("WhenMissingIAMRole_Returns403", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
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
		require.NoError(t, err)
		require.NotNil(t, res)
		forbidden, ok := res.(*oasgenserver.SnaplockFileDeleteForbidden)
		require.True(t, ok, "expected SnaplockFileDeleteForbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenWrongIAMRole_Returns403", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		// File delete requires PrivilegedDeleteRole; use ManageSnaplockRole so IAM check fails
		ctx := contextWithSnaplockIAMRequestForRole(t, middleware.ManageSnaplockRole)

		handler := Handler{}
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}
		res, err := handler.SnaplockFileDelete(ctx, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		forbidden, ok := res.(*oasgenserver.SnaplockFileDeleteForbidden)
		require.True(t, ok, "expected SnaplockFileDeleteForbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

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

		res, err := handler.SnaplockFileDelete(contextWithSnaplockIAMRequestForRole(t, middleware.PrivilegedDeleteRole), params)

		require.NoError(t, err, "SnaplockFileDelete should not return a Go error")
		require.NotNil(t, res, "Response should not be nil")

		internalErr, ok := res.(*oasgenserver.SnaplockFileDeleteBadRequest)
		require.True(t, ok, "Expected SnaplockFileDeleteBadRequest, got %T", res)
		assert.Equal(t, 400, internalErr.Code, "Code should be 400")
		assert.Equal(t, "Snaplock file delete operation is disabled", internalErr.Message, "Message should match")
	})
}

func TestSnaplockFileDelete_WithMockClient(t *testing.T) {
	testPoolUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	testVolumeUUID := uuid.MustParse("660e8400-e29b-41d4-a716-446655440001")
	vol := &handlers.VolumeInfo{UUID: testVolumeUUID.String(), Name: "vol1"}
	vol.SVM.Name = "svm1"
	vol.SVM.UUID = "svm-uuid"

	t.Run("WhenSetupCredsFails_Returns401", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithSetupCredsError(t, errors.New("auth failed"))()

		handler := Handler{}
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}
		res, err := handler.SnaplockFileDelete(contextWithSnaplockIAMRequestForRole(t, middleware.PrivilegedDeleteRole), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.SnaplockFileDeleteUnauthorized)
		require.True(t, ok, "expected SnaplockFileDeleteUnauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
	})

	t.Run("WhenEnsureCertFails_Returns401", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithEnsureCertError(t, errors.New("cert required"))()

		handler := Handler{}
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}
		res, err := handler.SnaplockFileDelete(contextWithSnaplockIAMRequestForRole(t, middleware.PrivilegedDeleteRole), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.SnaplockFileDeleteUnauthorized)
		require.True(t, ok, "expected SnaplockFileDeleteUnauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
	})

	t.Run("WhenNewClientFails_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithNewClientError(t, errors.New("connection refused"))()

		handler := Handler{}
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}
		res, err := handler.SnaplockFileDelete(contextWithSnaplockIAMRequestForRole(t, middleware.PrivilegedDeleteRole), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.SnaplockFileDeleteInternalServerError)
		require.True(t, ok, "expected SnaplockFileDeleteInternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
	})

	t.Run("WhenGetVolumeFails_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, testVolumeUUID.String()).Return(nil, errors.New("not found")).Once()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}
		res, err := handler.SnaplockFileDelete(contextWithSnaplockIAMRequestForRole(t, middleware.PrivilegedDeleteRole), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.SnaplockFileDeleteNotFound)
		require.True(t, ok, "expected SnaplockFileDeleteNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
	})

	t.Run("WhenVolumeIncomplete_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		incompleteVol := &handlers.VolumeInfo{UUID: testVolumeUUID.String(), Name: "vol1"}
		incompleteVol.SVM.Name = ""
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, testVolumeUUID.String()).Return(incompleteVol, nil).Once()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}
		res, err := handler.SnaplockFileDelete(contextWithSnaplockIAMRequestForRole(t, middleware.PrivilegedDeleteRole), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.SnaplockFileDeleteBadRequest)
		require.True(t, ok, "expected SnaplockFileDeleteBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "incomplete")
	})

	t.Run("WhenExecuteCLIReturnsOntapCLIError_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, testVolumeUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, &handlers.OntapCLIError{StatusCode: 400, Code: "400", Message: "file in use"}).Once()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}
		res, err := handler.SnaplockFileDelete(contextWithSnaplockIAMRequestForRole(t, middleware.PrivilegedDeleteRole), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.SnaplockFileDeleteBadRequest)
		require.True(t, ok, "expected SnaplockFileDeleteBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "file in use")
	})

	t.Run("WhenExecuteCLIFails_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, testVolumeUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, errors.New("connection reset")).Once()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}
		res, err := handler.SnaplockFileDelete(contextWithSnaplockIAMRequestForRole(t, middleware.PrivilegedDeleteRole), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.SnaplockFileDeleteInternalServerError)
		require.True(t, ok, "expected SnaplockFileDeleteInternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
	})

	t.Run("WhenCLINotSuccess_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, testVolumeUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: "Error: permission denied"}, nil).Once()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}
		res, err := handler.SnaplockFileDelete(contextWithSnaplockIAMRequestForRole(t, middleware.PrivilegedDeleteRole), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.SnaplockFileDeleteBadRequest)
		require.True(t, ok, "expected SnaplockFileDeleteBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})

	t.Run("WhenSuccess_ReturnsOK", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, testVolumeUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: "Deleted successfully"}, nil).Once()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.SnaplockFileDeleteParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        testPoolUUID,
			VolumeUuid:    testVolumeUUID,
			FilePath:      "test/file.txt",
		}
		res, err := handler.SnaplockFileDelete(contextWithSnaplockIAMRequestForRole(t, middleware.PrivilegedDeleteRole), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		okRes, ok := res.(*oasgenserver.SnaplockFileRetentionJobLinkResponse)
		require.True(t, ok, "expected SnaplockFileRetentionJobLinkResponse, got %T", res)
		assert.True(t, okRes.NumRecords.Set)
		assert.Equal(t, 1, okRes.NumRecords.Value)
	})
}

func TestV1SnaplockLitigationBegin(t *testing.T) {
	poolUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	volUUID := uuid.MustParse("660e8400-e29b-41d4-a716-446655440001")

	t.Run("WhenMissingIAMRole_Returns403", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "lit1",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{UUID: oasgenserver.NewOptUUID(volUUID)},
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
		}
		res, err := handler.V1SnaplockLitigationBegin(context.Background(), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		forbidden, ok := res.(*oasgenserver.V1SnaplockLitigationBeginForbidden)
		require.True(t, ok, "expected V1SnaplockLitigationBeginForbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenDisabled_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "lit1",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{UUID: oasgenserver.NewOptUUID(volUUID)},
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationBeginBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationBeginBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, "Snaplock litigation operation is disabled", badReq.Message)
	})

	t.Run("WhenRequestNil_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), nil, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationBeginBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationBeginBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "required")
	})

	t.Run("WhenLitigationNameEmpty_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{UUID: oasgenserver.NewOptUUID(volUUID)},
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationBeginBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationBeginBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})

	t.Run("WhenVolumeEmpty_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "lit1",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{}, // neither name nor uuid
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationBeginBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationBeginBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "volume (name or uuid) is required")
	})
}

func TestV1SnaplockLitigationCollectionGet(t *testing.T) {
	poolUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	t.Run("WhenMissingIAMRole_Returns403", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationCollectionGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
		}
		res, err := handler.V1SnaplockLitigationCollectionGet(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		forbidden, ok := res.(*oasgenserver.V1SnaplockLitigationCollectionGetForbidden)
		require.True(t, ok, "expected V1SnaplockLitigationCollectionGetForbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenDisabled_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationCollectionGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
		}

		res, err := handler.V1SnaplockLitigationCollectionGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationCollectionGetBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationCollectionGetBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})
}

func TestV1SnaplockLitigationEnd(t *testing.T) {
	poolUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	t.Run("WhenMissingIAMRole_Returns403", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationEndParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
			LitigationId:  "660e8400-e29b-41d4-a716-446655440001:lit1",
		}
		res, err := handler.V1SnaplockLitigationEnd(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		forbidden, ok := res.(*oasgenserver.V1SnaplockLitigationEndForbidden)
		require.True(t, ok, "expected V1SnaplockLitigationEndForbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenDisabled_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationEndParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
			LitigationId:  "660e8400-e29b-41d4-a716-446655440001:lit1",
		}

		res, err := handler.V1SnaplockLitigationEnd(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationEndBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationEndBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})
}

func TestV1SnaplockLitigationGet(t *testing.T) {
	poolUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	t.Run("WhenMissingIAMRole_Returns403", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
			LitigationId:  "660e8400-e29b-41d4-a716-446655440001:lit1",
		}
		res, err := handler.V1SnaplockLitigationGet(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		forbidden, ok := res.(*oasgenserver.V1SnaplockLitigationGetForbidden)
		require.True(t, ok, "expected V1SnaplockLitigationGetForbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenDisabled_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
			LitigationId:  "660e8400-e29b-41d4-a716-446655440001:lit1",
		}

		res, err := handler.V1SnaplockLitigationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationGetBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationGetBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})
}

func TestV1SnaplockLitigationOperationCreate(t *testing.T) {
	poolUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	t.Run("WhenMissingIAMRole_Returns403", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		handler := Handler{}
		req := &oasgenserver.SnaplockLegalHoldOperationRequest{
			Type: oasgenserver.SnaplockLegalHoldOperationRequestTypeBegin,
			Path: "/dir1",
		}
		params := oasgenserver.V1SnaplockLitigationOperationCreateParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
			LitigationId:  "660e8400-e29b-41d4-a716-446655440001:lit1",
		}
		res, err := handler.V1SnaplockLitigationOperationCreate(context.Background(), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		forbidden, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateForbidden)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateForbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenDisabled_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		req := &oasgenserver.SnaplockLegalHoldOperationRequest{
			Type: oasgenserver.SnaplockLegalHoldOperationRequestTypeBegin,
			Path: "/dir1",
		}
		params := oasgenserver.V1SnaplockLitigationOperationCreateParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
			LitigationId:  "660e8400-e29b-41d4-a716-446655440001:lit1",
		}

		res, err := handler.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})

	t.Run("WhenRequestNil_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationCreateParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
			LitigationId:  "660e8400-e29b-41d4-a716-446655440001:lit1",
		}

		res, err := handler.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), nil, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})
}

func TestV1SnaplockLitigationOperationGet(t *testing.T) {
	poolUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	t.Run("WhenMissingIAMRole_Returns403", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
			LitigationId:  "660e8400-e29b-41d4-a716-446655440001:lit1",
			OperationId:   "16908292",
		}
		res, err := handler.V1SnaplockLitigationOperationGet(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		forbidden, ok := res.(*oasgenserver.V1SnaplockLitigationOperationGetForbidden)
		require.True(t, ok, "expected V1SnaplockLitigationOperationGetForbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenDisabled_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
			LitigationId:  "660e8400-e29b-41d4-a716-446655440001:lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationGetBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationGetBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})
}

func TestV1SnaplockLitigationOperationAbort(t *testing.T) {
	poolUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	t.Run("WhenMissingIAMRole_Returns403", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationAbortParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
			LitigationId:  "660e8400-e29b-41d4-a716-446655440001:lit1",
			OperationId:   "16908292",
		}
		res, err := handler.V1SnaplockLitigationOperationAbort(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		forbidden, ok := res.(*oasgenserver.V1SnaplockLitigationOperationAbortForbidden)
		require.True(t, ok, "expected V1SnaplockLitigationOperationAbortForbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenDisabled_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationAbortParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        poolUUID,
			LitigationId:  "660e8400-e29b-41d4-a716-446655440001:lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationAbort(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationAbortBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationAbortBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})
}

// litigationTestPoolUUID and litigationTestVolUUID are used by litigation mock tests.
var (
	litigationTestPoolUUID = uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	litigationTestVolUUID  = uuid.MustParse("660e8400-e29b-41d4-a716-446655440001")
)

// stubLitigationCredsAndClient replaces package-level func vars so tests can inject a mock ONTAP client.
// TODO follow-up: Prefer injecting a "feature flag" (snapLockOperationEnabled) and credential/client
// factory into the handler (e.g. struct fields or constructor args) so tests avoid global mutation and
// defer restore; the current approach is correct but reduces test determinism and maintainability.
func stubLitigationCredsAndClient(t *testing.T, mockClient *handlers.MockOntapClient) (restore func()) {
	oldSetup := setupCredentialsForHandler
	oldEnsure := ensureCertificateOrPassword
	oldClient := newOntapClientFromContext
	restore = func() {
		setupCredentialsForHandler = oldSetup
		ensureCertificateOrPassword = oldEnsure
		newOntapClientFromContext = oldClient
	}
	setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
	ensureCertificateOrPassword = func(context.Context) error { return nil }
	newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }
	return restore
}

// stubWithSetupCredsError stubs setupCredentialsForHandler to return an error (for coverage of 401 setup path).
func stubWithSetupCredsError(t *testing.T, err error) (restore func()) {
	old := setupCredentialsForHandler
	restore = func() { setupCredentialsForHandler = old }
	setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, err }
	return restore
}

// stubWithEnsureCertError stubs setup to succeed and ensureCertificateOrPassword to return an error (for coverage of 401 cert path).
func stubWithEnsureCertError(t *testing.T, err error) (restore func()) {
	oldSetup := setupCredentialsForHandler
	oldEnsure := ensureCertificateOrPassword
	restore = func() {
		setupCredentialsForHandler = oldSetup
		ensureCertificateOrPassword = oldEnsure
	}
	setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
	ensureCertificateOrPassword = func(context.Context) error { return err }
	return restore
}

// stubWithNewClientError stubs setup and ensure to succeed and newOntapClientFromContext to return an error (for coverage of 500 client path).
func stubWithNewClientError(t *testing.T, err error) (restore func()) {
	oldSetup := setupCredentialsForHandler
	oldEnsure := ensureCertificateOrPassword
	oldClient := newOntapClientFromContext
	restore = func() {
		setupCredentialsForHandler = oldSetup
		ensureCertificateOrPassword = oldEnsure
		newOntapClientFromContext = oldClient
	}
	setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
	ensureCertificateOrPassword = func(context.Context) error { return nil }
	newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return nil, err }
	return restore
}

func TestV1SnaplockLitigationBegin_WithMockClient(t *testing.T) {
	vol := &handlers.VolumeInfo{UUID: litigationTestVolUUID.String(), Name: "vol1"}
	vol.SVM.Name = "svm1"
	vol.SVM.UUID = "svm-uuid"

	t.Run("WhenSetupCredsFails_Returns401", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithSetupCredsError(t, errors.New("auth failed"))()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{LitigationName: "lit1", Path: "/dir1", Volume: oasgenserver.SnaplockLitigationBeginRequestVolume{UUID: oasgenserver.NewOptUUID(litigationTestVolUUID)}}
		params := oasgenserver.V1SnaplockLitigationBeginParams{ProjectNumber: "123456", LocationId: "us-central1", PoolId: litigationTestPoolUUID}
		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1SnaplockLitigationBeginUnauthorized)
		require.True(t, ok, "expected V1SnaplockLitigationBeginUnauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
		assert.Contains(t, unauth.Message, "authentication error")
	})

	t.Run("WhenEnsureCertFails_Returns401", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithEnsureCertError(t, errors.New("cert required"))()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{LitigationName: "lit1", Path: "/dir1", Volume: oasgenserver.SnaplockLitigationBeginRequestVolume{UUID: oasgenserver.NewOptUUID(litigationTestVolUUID)}}
		params := oasgenserver.V1SnaplockLitigationBeginParams{ProjectNumber: "123456", LocationId: "us-central1", PoolId: litigationTestPoolUUID}
		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1SnaplockLitigationBeginUnauthorized)
		require.True(t, ok, "expected V1SnaplockLitigationBeginUnauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
		assert.Contains(t, unauth.Message, "authentication error")
	})

	t.Run("WhenNewClientFails_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithNewClientError(t, errors.New("connection refused"))()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{LitigationName: "lit1", Path: "/dir1", Volume: oasgenserver.SnaplockLitigationBeginRequestVolume{UUID: oasgenserver.NewOptUUID(litigationTestVolUUID)}}
		params := oasgenserver.V1SnaplockLitigationBeginParams{ProjectNumber: "123456", LocationId: "us-central1", PoolId: litigationTestPoolUUID}
		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1SnaplockLitigationBeginInternalServerError)
		require.True(t, ok, "expected V1SnaplockLitigationBeginInternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		assert.Contains(t, internal.Message, "failed to connect to ONTAP")
	})

	t.Run("WhenCLISuccess_ReturnsLitigationResponse", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: "Operation completed"}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "lit1",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{UUID: oasgenserver.NewOptUUID(litigationTestVolUUID)},
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		okRes, ok := res.(*oasgenserver.SnaplockLitigationResponse)
		require.True(t, ok, "expected SnaplockLitigationResponse, got %T", res)
		assert.True(t, okRes.ID.Set)
		assert.Equal(t, litigationTestVolUUID.String()+":lit1", okRes.ID.Value)
		assert.True(t, okRes.Name.Set)
		assert.Equal(t, "lit1", okRes.Name.Value)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenGetVolumeFails_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(nil, errors.New("not found")).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "lit1",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{UUID: oasgenserver.NewOptUUID(litigationTestVolUUID)},
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1SnaplockLitigationBeginNotFound)
		require.True(t, ok, "expected V1SnaplockLitigationBeginNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenVolumeIncomplete_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		incompleteVol := &handlers.VolumeInfo{UUID: litigationTestVolUUID.String(), Name: "vol1"}
		incompleteVol.SVM.Name = "" // incomplete
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(incompleteVol, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "lit1",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{UUID: oasgenserver.NewOptUUID(litigationTestVolUUID)},
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationBeginBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationBeginBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "incomplete")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLIReturnsOntapCLIError_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, &handlers.OntapCLIError{StatusCode: 400, Code: "ERR", Message: "path invalid"}).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "lit1",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{UUID: oasgenserver.NewOptUUID(litigationTestVolUUID)},
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationBeginBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationBeginBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "path invalid")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLINotSuccess_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: "Error: litigation already exists"}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "lit1",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{UUID: oasgenserver.NewOptUUID(litigationTestVolUUID)},
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationBeginBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationBeginBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenVolumeNameOnly_ListVolumesSuccess_ReturnsLitigationResponse", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		volByName := handlers.VolumeInfo{UUID: litigationTestVolUUID.String(), Name: "vol1"}
		volByName.SVM.Name = "svm1"
		volByName.SVM.UUID = "svm-uuid"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{volByName}, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: "Operation completed"}, nil).Once()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "lit1",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{Name: oasgenserver.NewOptString("vol1")},
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		okRes, ok := res.(*oasgenserver.SnaplockLitigationResponse)
		require.True(t, ok, "expected SnaplockLitigationResponse, got %T", res)
		assert.True(t, okRes.ID.Set)
		assert.Equal(t, litigationTestVolUUID.String()+":lit1", okRes.ID.Value)
		assert.Equal(t, "lit1", okRes.Name.Value)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenVolumeNameOnly_VolumeNotFoundInList_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		otherVol := handlers.VolumeInfo{UUID: "other-uuid", Name: "other_vol"}
		otherVol.SVM.Name = "svm1"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{otherVol}, nil).Once()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "lit1",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{Name: oasgenserver.NewOptString("nonexistent")},
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1SnaplockLitigationBeginNotFound)
		require.True(t, ok, "expected V1SnaplockLitigationBeginNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		assert.Contains(t, notFound.Message, "volume not found")
		assert.Contains(t, notFound.Message, "nonexistent")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenVolumeNameOnly_ListVolumesFails_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return(nil, errors.New("list failed")).Once()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		req := &oasgenserver.SnaplockLitigationBeginRequest{
			LitigationName: "lit1",
			Path:           "/dir1",
			Volume:         oasgenserver.SnaplockLitigationBeginRequestVolume{Name: oasgenserver.NewOptString("vol1")},
		}
		params := oasgenserver.V1SnaplockLitigationBeginParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
		}

		res, err := handler.V1SnaplockLitigationBegin(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1SnaplockLitigationBeginInternalServerError)
		require.True(t, ok, "expected V1SnaplockLitigationBeginInternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		assert.Contains(t, internal.Message, "failed to list volumes")
		mockClient.AssertExpectations(t)
	})
}

func TestV1SnaplockLitigationCollectionGet_WithMockClient(t *testing.T) {
	vol := handlers.VolumeInfo{UUID: "vol-uuid-1", Name: "vol1"}
	vol.SVM.Name = "svm1"
	vol.SVM.UUID = "svm-uuid"
	// CLI output that ParseSnaplockLegalHoldShowInstanceOutputToOperations can parse (has Vserver block, Operation ID, Litigation Name)
	legalHoldShowOutput := "Vserver: svm1\n  Volume: vol1\nLitigation Name: lit1\nPath: /p1\nOperation ID: 1\nStatus: Completed\nOperation Type: begin\n"

	collectionGetParams := func() oasgenserver.V1SnaplockLitigationCollectionGetParams {
		return oasgenserver.V1SnaplockLitigationCollectionGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
		}
	}

	t.Run("WhenSetupCredsFails_Returns401", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithSetupCredsError(t, errors.New("auth failed"))()

		res, err := Handler{}.V1SnaplockLitigationCollectionGet(contextWithSnaplockIAMRequest(t), collectionGetParams())
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1SnaplockLitigationCollectionGetUnauthorized)
		require.True(t, ok, "expected V1SnaplockLitigationCollectionGetUnauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
		assert.Contains(t, unauth.Message, "authentication error")
	})

	t.Run("WhenEnsureCertFails_Returns401", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithEnsureCertError(t, errors.New("cert required"))()

		res, err := Handler{}.V1SnaplockLitigationCollectionGet(contextWithSnaplockIAMRequest(t), collectionGetParams())
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1SnaplockLitigationCollectionGetUnauthorized)
		require.True(t, ok, "expected V1SnaplockLitigationCollectionGetUnauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
		assert.Contains(t, unauth.Message, "authentication error")
	})

	t.Run("WhenNewClientFails_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithNewClientError(t, errors.New("connection refused"))()

		res, err := Handler{}.V1SnaplockLitigationCollectionGet(contextWithSnaplockIAMRequest(t), collectionGetParams())
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1SnaplockLitigationCollectionGetInternalServerError)
		require.True(t, ok, "expected V1SnaplockLitigationCollectionGetInternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		assert.Contains(t, internal.Message, "failed to connect to ONTAP")
	})

	t.Run("WhenListVolumesAndCLISuccess_ReturnsRecords", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{vol}, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: legalHoldShowOutput}, nil).Once()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationCollectionGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
		}

		res, err := handler.V1SnaplockLitigationCollectionGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		listRes, ok := res.(*oasgenserver.SnaplockLitigationListResponse)
		require.True(t, ok, "expected SnaplockLitigationListResponse, got %T", res)
		require.True(t, listRes.NumRecords.Set)
		assert.GreaterOrEqual(t, listRes.NumRecords.Value, 1)
		assert.GreaterOrEqual(t, len(listRes.Records), 1)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenListVolumesFails_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return(nil, errors.New("list failed")).Once()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationCollectionGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
		}

		res, err := handler.V1SnaplockLitigationCollectionGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internalErr, ok := res.(*oasgenserver.V1SnaplockLitigationCollectionGetInternalServerError)
		require.True(t, ok, "expected V1SnaplockLitigationCollectionGetInternalServerError, got %T", res)
		assert.Equal(t, 500, internalErr.Code)
		assert.Contains(t, internalErr.Message, "list volumes")
		mockClient.AssertExpectations(t)
	})
}

func TestV1SnaplockLitigationEnd_WithMockClient(t *testing.T) {
	vol := &handlers.VolumeInfo{UUID: litigationTestVolUUID.String(), Name: "vol1"}
	vol.SVM.Name = "svm1"
	vol.SVM.UUID = "svm-uuid"
	endParams := func(litID string) oasgenserver.V1SnaplockLitigationEndParams {
		return oasgenserver.V1SnaplockLitigationEndParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litID,
		}
	}

	t.Run("WhenSetupCredsFails_Returns401", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithSetupCredsError(t, errors.New("auth failed"))()

		res, err := Handler{}.V1SnaplockLitigationEnd(contextWithSnaplockIAMRequest(t), endParams(litigationTestVolUUID.String()+":lit1"))
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1SnaplockLitigationEndUnauthorized)
		require.True(t, ok, "expected V1SnaplockLitigationEndUnauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
		assert.Contains(t, unauth.Message, "authentication error")
	})

	t.Run("WhenEnsureCertFails_Returns401", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithEnsureCertError(t, errors.New("cert required"))()

		res, err := Handler{}.V1SnaplockLitigationEnd(contextWithSnaplockIAMRequest(t), endParams(litigationTestVolUUID.String()+":lit1"))
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1SnaplockLitigationEndUnauthorized)
		require.True(t, ok, "expected V1SnaplockLitigationEndUnauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
		assert.Contains(t, unauth.Message, "authentication error")
	})

	t.Run("WhenNewClientFails_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithNewClientError(t, errors.New("connection refused"))()

		res, err := Handler{}.V1SnaplockLitigationEnd(contextWithSnaplockIAMRequest(t), endParams(litigationTestVolUUID.String()+":lit1"))
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1SnaplockLitigationEndInternalServerError)
		require.True(t, ok, "expected V1SnaplockLitigationEndInternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		assert.Contains(t, internal.Message, "failed to connect to ONTAP")
	})

	t.Run("WhenLitigationIdInvalid_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationEndParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  "bad-id-no-colon",
		}

		res, err := handler.V1SnaplockLitigationEnd(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationEndBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationEndBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "volumeUuid:litigationName")
	})

	t.Run("WhenCLISuccess_ReturnsOK", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: "Deleted successfully"}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationEndParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationEnd(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		_, ok := res.(*oasgenserver.V1SnaplockLitigationEndOK)
		require.True(t, ok, "expected V1SnaplockLitigationEndOK, got %T", res)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenGetVolumeFails_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(nil, errors.New("not found")).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationEndParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationEnd(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1SnaplockLitigationEndNotFound)
		require.True(t, ok, "expected V1SnaplockLitigationEndNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsOntapCLIError_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, &handlers.OntapCLIError{Code: "400", Message: "litigation not found"}).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationEndParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationEnd(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationEndBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationEndBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, "litigation not found", badReq.Message)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsOntapCLIErrorWithZeroCode_ReturnsBadRequest400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, &handlers.OntapCLIError{Code: "0", Message: "invalid"}).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationEndParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationEnd(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationEndBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationEndBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsNonOntapError_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, errors.New("connection refused")).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationEndParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationEnd(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1SnaplockLitigationEndInternalServerError)
		require.True(t, ok, "expected V1SnaplockLitigationEndInternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		assert.Contains(t, internal.Message, "ONTAP operation failed")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLIOutputNotSuccess_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		cliOutput := "Error: litigation end failed"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: cliOutput}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationEndParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationEnd(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationEndBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationEndBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, handlers.ParseCLIError(cliOutput), badReq.Message)
		mockClient.AssertExpectations(t)
	})
}

func TestV1SnaplockLitigationGet_WithMockClient(t *testing.T) {
	vol := &handlers.VolumeInfo{UUID: litigationTestVolUUID.String(), Name: "vol1"}
	vol.SVM.Name = "svm1"
	vol.SVM.UUID = "svm-uuid"
	legalHoldShowOutput := "Vserver: svm1\nLitigation Name: lit1\nPath: /dir1\n"
	t.Run("WhenLitigationIdInvalid_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  "nocolon",
		}

		res, err := handler.V1SnaplockLitigationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationGetBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationGetBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})

	t.Run("WhenCLISuccess_ReturnsLitigation", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: legalHoldShowOutput}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		litRes, ok := res.(*oasgenserver.SnaplockLitigationResponse)
		require.True(t, ok, "expected SnaplockLitigationResponse, got %T", res)
		assert.True(t, litRes.Name.Set)
		assert.Equal(t, "lit1", litRes.Name.Value)
		assert.True(t, litRes.Path.Set)
		assert.Equal(t, "/dir1", litRes.Path.Value)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLISuccess_ReturnsLitigationWithOperationsMapped", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		// CLI output: one litigation block with Path; two operation blocks to cover oasOps loop:
		// - Op1: Path, Type end, all optional fields (NumFilesProcessed, NumFilesFailed, NumFilesSkipped, NumInodesIgnored)
		// - Op2: no Path / no Operation Type → defaults path="/", type=begin; no optional fields
		cliOutput := "Vserver: svm1\nLitigation Name: lit1\nPath: /dir1\nOperation ID: 1\nStatus: Completed\nOperation Type: end\nNumber of Files Processed: 5\nNumber of Files Failed: 1\nNumber of Files Skipped: 0\nNumber of Inodes Ignored: 2\n\nVserver: svm1\nOperation ID: 2\nStatus: In-Progress\n"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: cliOutput}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		litRes, ok := res.(*oasgenserver.SnaplockLitigationResponse)
		require.True(t, ok, "expected SnaplockLitigationResponse, got %T", res)
		oasOps := litRes.Operations
		require.Len(t, oasOps, 2, "expected two operation records")

		// First op: path set, type end, all optional fields set
		op1 := oasOps[0]
		require.True(t, op1.ID.Set)
		assert.Equal(t, 1, op1.ID.Value)
		require.True(t, op1.Path.Set)
		assert.Equal(t, "/dir1", op1.Path.Value)
		require.True(t, op1.Type.Set)
		assert.Equal(t, oasgenserver.SnaplockLegalHoldOperationResponseTypeEnd, op1.Type.Value)
		require.True(t, op1.State.Set)
		assert.Equal(t, oasgenserver.SnaplockLegalHoldOperationResponseState("completed"), op1.State.Value)
		require.True(t, op1.NumFilesProcessed.Set)
		assert.Equal(t, "5", op1.NumFilesProcessed.Value)
		require.True(t, op1.NumFilesFailed.Set)
		assert.Equal(t, "1", op1.NumFilesFailed.Value)
		require.True(t, op1.NumFilesSkipped.Set)
		assert.Equal(t, "0", op1.NumFilesSkipped.Value)
		require.True(t, op1.NumInodesIgnored.Set)
		assert.Equal(t, "2", op1.NumInodesIgnored.Value)

		// Second op: empty path/type in CLI → default path "/", type begin; optional fields not set
		op2 := oasOps[1]
		require.True(t, op2.ID.Set)
		assert.Equal(t, 2, op2.ID.Value)
		require.True(t, op2.Path.Set)
		assert.Equal(t, "/", op2.Path.Value, "empty path should default to /")
		require.True(t, op2.Type.Set)
		assert.Equal(t, oasgenserver.SnaplockLegalHoldOperationResponseTypeBegin, op2.Type.Value, "empty operation type should default to begin")
		require.True(t, op2.State.Set)
		assert.Equal(t, oasgenserver.SnaplockLegalHoldOperationResponseState("in_progress"), op2.State.Value)
		assert.False(t, op2.NumFilesProcessed.Set, "optional field should be unset when not in CLI output")
		assert.False(t, op2.NumFilesFailed.Set)
		assert.False(t, op2.NumFilesSkipped.Set)
		assert.False(t, op2.NumInodesIgnored.Set)

		mockClient.AssertExpectations(t)
	})

	t.Run("WhenGetVolumeFails_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(nil, errors.New("not found")).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1SnaplockLitigationGetNotFound)
		require.True(t, ok, "expected V1SnaplockLitigationGetNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenVolumeIncomplete_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		incompleteVol := &handlers.VolumeInfo{UUID: litigationTestVolUUID.String(), Name: ""}
		incompleteVol.SVM.Name = "svm1"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(incompleteVol, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationGetBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationGetBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "incomplete")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIFails_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, errors.New("connection refused")).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1SnaplockLitigationGetInternalServerError)
		require.True(t, ok, "expected V1SnaplockLitigationGetInternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		assert.Contains(t, internal.Message, "litigation get CLI failed")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLIOutputNotSuccess_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		cliOutput := "Error: litigation not found"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: cliOutput}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1SnaplockLitigationGetNotFound)
		require.True(t, ok, "expected V1SnaplockLitigationGetNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		assert.Equal(t, handlers.ParseCLIError(cliOutput), notFound.Message)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenParseReturnsNoRecords_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		// Output has no "Litigation Name:" so parser returns 0 records; no "Error:" so IsCLISuccess is true
		cliOutput := "Vserver: svm1\nPath: /dir1\n"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: cliOutput}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1SnaplockLitigationGetNotFound)
		require.True(t, ok, "expected V1SnaplockLitigationGetNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		assert.Contains(t, notFound.Message, "litigation not found")
		assert.Contains(t, notFound.Message, "lit1")
		mockClient.AssertExpectations(t)
	})
}

func TestV1SnaplockLitigationOperationCreate_WithMockClient(t *testing.T) {
	vol := &handlers.VolumeInfo{UUID: litigationTestVolUUID.String(), Name: "vol1"}
	vol.SVM.Name = "svm1"
	vol.SVM.UUID = "svm-uuid"
	beginOutputWithOpID := "some output -operation-id 16908292 done"

	t.Run("WhenSetupCredsFails_Returns401", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithSetupCredsError(t, errors.New("auth failed"))()

		req := &oasgenserver.SnaplockLegalHoldOperationRequest{Type: oasgenserver.SnaplockLegalHoldOperationRequestTypeBegin, Path: "/dir1"}
		params := oasgenserver.V1SnaplockLitigationOperationCreateParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}
		res, err := Handler{}.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateUnauthorized)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateUnauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
		assert.Contains(t, unauth.Message, "authentication error")
	})

	t.Run("WhenEnsureCertFails_Returns401", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithEnsureCertError(t, errors.New("cert required"))()

		req := &oasgenserver.SnaplockLegalHoldOperationRequest{Type: oasgenserver.SnaplockLegalHoldOperationRequestTypeBegin, Path: "/dir1"}
		params := oasgenserver.V1SnaplockLitigationOperationCreateParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}
		res, err := Handler{}.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateUnauthorized)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateUnauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
		assert.Contains(t, unauth.Message, "authentication error")
	})

	t.Run("WhenNewClientFails_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()
		defer stubWithNewClientError(t, errors.New("connection refused"))()

		req := &oasgenserver.SnaplockLegalHoldOperationRequest{Type: oasgenserver.SnaplockLegalHoldOperationRequestTypeBegin, Path: "/dir1"}
		params := oasgenserver.V1SnaplockLitigationOperationCreateParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}
		res, err := Handler{}.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateInternalServerError)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateInternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		assert.Contains(t, internal.Message, "failed to connect to ONTAP")
	})

	t.Run("WhenCLISuccessBegin_ReturnsResponse", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: beginOutputWithOpID}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		req := &oasgenserver.SnaplockLegalHoldOperationRequest{
			Type: oasgenserver.SnaplockLegalHoldOperationRequestTypeBegin,
			Path: "/dir1",
		}
		params := oasgenserver.V1SnaplockLitigationOperationCreateParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		headers, ok := res.(*oasgenserver.SnaplockLegalHoldOperationResponseHeaders)
		require.True(t, ok, "expected SnaplockLegalHoldOperationResponseHeaders, got %T", res)
		assert.True(t, headers.Response.ID.Set)
		assert.Equal(t, 16908292, headers.Response.ID.Value)
		assert.Equal(t, "begin", string(headers.Response.Type.Value))
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenInvalidType_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		volForInvalidType := &handlers.VolumeInfo{UUID: litigationTestVolUUID.String(), Name: "vol1"}
		volForInvalidType.SVM.Name = "svm1"
		volForInvalidType.SVM.UUID = "u"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(volForInvalidType, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		req := &oasgenserver.SnaplockLegalHoldOperationRequest{
			Type: oasgenserver.SnaplockLegalHoldOperationRequestType("invalid_type"),
			Path: "/dir1",
		}
		params := oasgenserver.V1SnaplockLitigationOperationCreateParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "invalid type")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenLitigationIdInvalid_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		req := &oasgenserver.SnaplockLegalHoldOperationRequest{
			Type: oasgenserver.SnaplockLegalHoldOperationRequestTypeBegin,
			Path: "/dir1",
		}
		params := oasgenserver.V1SnaplockLitigationOperationCreateParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  "bad",
		}

		res, err := handler.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "volumeUuid:litigationName")
	})

	t.Run("WhenGetVolumeFails_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(nil, errors.New("not found")).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		req := &oasgenserver.SnaplockLegalHoldOperationRequest{
			Type: oasgenserver.SnaplockLegalHoldOperationRequestTypeBegin,
			Path: "/dir1",
		}
		params := oasgenserver.V1SnaplockLitigationOperationCreateParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
		}

		res, err := handler.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), req, params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateNotFound)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	opCreateReq := &oasgenserver.SnaplockLegalHoldOperationRequest{Type: oasgenserver.SnaplockLegalHoldOperationRequestTypeBegin, Path: "/dir1"}
	opCreateParams := oasgenserver.V1SnaplockLitigationOperationCreateParams{
		ProjectNumber: "123456",
		LocationId:    "us-central1",
		PoolId:        litigationTestPoolUUID,
		LitigationId:  litigationTestVolUUID.String() + ":lit1",
	}

	t.Run("WhenExecuteCLIReturnsOntapCLIError_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, &handlers.OntapCLIError{Code: "400", Message: "path already under legal hold"}).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		res, err := Handler{}.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), opCreateReq, opCreateParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, "path already under legal hold", badReq.Message)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsOntapCLIErrorWithZeroCode_ReturnsBadRequest400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, &handlers.OntapCLIError{Code: "0", Message: "invalid"}).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		res, err := Handler{}.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), opCreateReq, opCreateParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsNonOntapError_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, errors.New("connection refused")).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		res, err := Handler{}.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), opCreateReq, opCreateParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateInternalServerError)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateInternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		assert.Contains(t, internal.Message, "ONTAP operation failed")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLIOutputNotSuccess_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		cliOutput := "Error: command failed"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: cliOutput}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		res, err := Handler{}.V1SnaplockLitigationOperationCreate(contextWithSnaplockIAMRequest(t), opCreateReq, opCreateParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationCreateBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationCreateBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, handlers.ParseCLIError(cliOutput), badReq.Message)
		mockClient.AssertExpectations(t)
	})
}

func TestV1SnaplockLitigationOperationGet_WithMockClient(t *testing.T) {
	showOpOutput := "Vserver: svm1\nOperation ID: 16908292\nPath: /dir1\nStatus: In-Progress\nOperation Type: begin\nNumber of Files Processed: 10\nNumber of Files Failed: 0\n"
	t.Run("WhenCLISuccess_ReturnsOperation", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: showOpOutput}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		opRes, ok := res.(*oasgenserver.SnaplockLegalHoldOperationResponse)
		require.True(t, ok, "expected SnaplockLegalHoldOperationResponse, got %T", res)
		assert.True(t, opRes.ID.Set)
		assert.Equal(t, 16908292, opRes.ID.Value)
		assert.Equal(t, "in_progress", string(opRes.State.Value))
		assert.True(t, opRes.Path.Set)
		assert.Equal(t, "/dir1", opRes.Path.Value)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLIFails_Returns500", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, errors.New("connection refused")).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internalErr, ok := res.(*oasgenserver.V1SnaplockLitigationOperationGetInternalServerError)
		require.True(t, ok, "expected V1SnaplockLitigationOperationGetInternalServerError, got %T", res)
		assert.Equal(t, 500, internalErr.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLINotSuccess_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: "Error: operation not found"}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationGetParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationGet(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1SnaplockLitigationOperationGetNotFound)
		require.True(t, ok, "expected V1SnaplockLitigationOperationGetNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})
}

func TestV1SnaplockLitigationOperationAbort_WithMockClient(t *testing.T) {
	vol := &handlers.VolumeInfo{UUID: litigationTestVolUUID.String(), Name: "vol1"}
	vol.SVM.Name = "svm1"
	vol.SVM.UUID = "svm-uuid"
	t.Run("WhenLitigationIdInvalid_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationAbortParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  "bad",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationAbort(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationAbortBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationAbortBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "volumeUuid:litigationName")
	})

	t.Run("WhenCLISuccess_ReturnsOK", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: "Abort completed"}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationAbortParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationAbort(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		_, ok := res.(*oasgenserver.V1SnaplockLitigationOperationAbortOK)
		require.True(t, ok, "expected V1SnaplockLitigationOperationAbortOK, got %T", res)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenGetVolumeFails_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(nil, errors.New("not found")).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationAbortParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationAbort(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1SnaplockLitigationOperationAbortNotFound)
		require.True(t, ok, "expected V1SnaplockLitigationOperationAbortNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLIErrorOperationComplete_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, &handlers.OntapCLIError{Message: "SnapLock legal-hold operation is complete"}).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationAbortParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationAbort(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationAbortBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationAbortBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLIErrorNotFound_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(nil, &handlers.OntapCLIError{Message: "operation not found"}).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationAbortParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationAbort(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1SnaplockLitigationOperationAbortNotFound)
		require.True(t, ok, "expected V1SnaplockLitigationOperationAbortNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	// Tests for IsCLISuccess(cliResponse.Output) == false path (CLI returns response with failure output, not OntapCLIError)
	t.Run("WhenCLIOutputOperationComplete_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		cliOutput := "Error: SnapLock legal-hold operation is complete. Run \"snaplock legal-hold show\" to view status."
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: cliOutput}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationAbortParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationAbort(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationAbortBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationAbortBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, handlers.ParseSnaplockAbortError(cliOutput), badReq.Message)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLIOutputNotFound_Returns404", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		cliOutput := "Error: operation 16908292 not found"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: cliOutput}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationAbortParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationAbort(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1SnaplockLitigationOperationAbortNotFound)
		require.True(t, ok, "expected V1SnaplockLitigationOperationAbortNotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		assert.Equal(t, handlers.ParseSnaplockAbortError(cliOutput), notFound.Message)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLIOutputNotSuccess_Returns400", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = true
		defer func() { snapLockOperationEnabled = original }()

		cliOutput := "Error: command failed"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, litigationTestVolUUID.String()).Return(vol, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, handlers.SnaplockPrivilegeLevel).
			Return(&handlers.CLIResponse{Output: cliOutput}, nil).Once()
		mockClient.On("ListVolumesWithSvm", mock.Anything, 1000).Return([]handlers.VolumeInfo{}, nil).Maybe()
		defer stubLitigationCredsAndClient(t, mockClient)()

		handler := Handler{}
		params := oasgenserver.V1SnaplockLitigationOperationAbortParams{
			ProjectNumber: "123456",
			LocationId:    "us-central1",
			PoolId:        litigationTestPoolUUID,
			LitigationId:  litigationTestVolUUID.String() + ":lit1",
			OperationId:   "16908292",
		}

		res, err := handler.V1SnaplockLitigationOperationAbort(contextWithSnaplockIAMRequest(t), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1SnaplockLitigationOperationAbortBadRequest)
		require.True(t, ok, "expected V1SnaplockLitigationOperationAbortBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, handlers.ParseSnaplockAbortError(cliOutput), badReq.Message)
		mockClient.AssertExpectations(t)
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
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) {
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
