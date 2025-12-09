package rules_v2

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	coreapi "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/core-api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestValidateVolumeCreation(t *testing.T) {
	// Save and restore original function
	origSubmit := submitExpertModeVolumeOperation
	defer func() { submitExpertModeVolumeOperation = origSubmit }()

	const cacheKey = "unit-test-cache-key"

	// success
	t.Run("WhenSuccess", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		r := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", bytes.NewBufferString(`{"name":"vol1","size":1024,"svm":{}}`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeCreation(r)
		if !ok || reason != "" {
			t.Fatalf("expected success, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("WhenParseError", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		r := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", bytes.NewBufferString(`{invalid`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeCreation(r)
		if ok || !strings.Contains(reason, "invalid JSON") {
			t.Fatalf("expected parse error, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("WhenMissingCacheKey", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		r := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", bytes.NewBufferString(`{"name":"vol1","size":1024}`))
		ok, reason := _validateVolumeCreation(r)
		if ok || !strings.Contains(reason, "cache key not found") {
			t.Fatalf("expected cache key error, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("WhenMissingAuthdata", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.WithValue(context.Background(), models.AuthDataKey, "wrong-key")
		r := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", bytes.NewBufferString(`{"name":"vol1","size":1024}`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeCreation(r)
		if ok || !strings.Contains(reason, "auth data not found") {
			t.Fatalf("expected missing auth data error, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("WhenPersistFails", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return errors.New("persist failed")
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		r := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", bytes.NewBufferString(`{"name":"vol1","size":1024}`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeCreation(r)
		if ok || !strings.Contains(reason, "persist failed") {
			t.Fatalf("expected persist failure, got ok=%v reason=%q", ok, reason)
		}
	})

	// invalid size cases
	t.Run("WhenInvalidSizeUnit", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		r := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", bytes.NewBufferString(`{"name":"vol1","size":"100GiB"}`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeCreation(r)
		if ok || !strings.Contains(reason, `"100GiB" is an invalid value for field "size"`) {
			t.Fatalf("expected invalid size error, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("WhenEmptySize", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		r := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", bytes.NewBufferString(`{"name":"vol1","size":""}`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeCreation(r)
		if ok || !strings.Contains(reason, `"" is an invalid value for field "size"`) {
			t.Fatalf("expected invalid size error for empty, got ok=%v reason=%q", ok, reason)
		}
	})
}

func TestValidateVolumeModification(t *testing.T) {
	origSubmit := submitExpertModeVolumeOperation
	defer func() { submitExpertModeVolumeOperation = origSubmit }()
	const cacheKey = "unit-test-cache-key-mod"

	// success
	t.Run("WhenSuccess", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		// PATCH on specific volume UUID
		r := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/abcd-1234", bytes.NewBufferString(`{"name":"vol1","size":2048}`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeModification(r)
		if !ok || reason != "" {
			t.Fatalf("expected success, got ok=%v reason=%q", ok, reason)
		}
	})

	// parse error invalid json
	t.Run("WhenParseError", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		r := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/abcd-1234", bytes.NewBufferString(`{invalid`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeModification(r)
		if ok || !strings.Contains(reason, "invalid JSON") {
			t.Fatalf("expected parse error, got ok=%v reason=%q", ok, reason)
		}
	})

	// missing cache key
	t.Run("WhenMissingCacheKey", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		r := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/abcd-1234", bytes.NewBufferString(`{"name":"vol1","size":2048}`))
		ok, reason := _validateVolumeModification(r)
		if ok || !strings.Contains(reason, "cache key not found") {
			t.Fatalf("expected cache key error, got ok=%v reason=%q", ok, reason)
		}
	})

	// missing auth data
	t.Run("WhenMissingAuthdata", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.WithValue(context.Background(), models.AuthDataKey, "wrong-key")
		r := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/abcd-1234", bytes.NewBufferString(`{"name":"vol1","size":2048}`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeModification(r)
		if ok || !strings.Contains(reason, "auth data not found") {
			t.Fatalf("expected missing auth data error, got ok=%v reason=%q", ok, reason)
		}
	})

	// persist fails
	t.Run("WhenPersistFails", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return errors.New("persist failed")
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		r := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/abcd-1234", bytes.NewBufferString(`{"name":"vol1","size":2048}`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeModification(r)
		if ok || !strings.Contains(reason, "persist failed") {
			t.Fatalf("expected persist failure, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("WhenInvalidSizeUnit", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		r := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/abcd-1234", bytes.NewBufferString(`{"name":"vol1","size":"100GiB"}`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeModification(r)
		if ok || !strings.Contains(reason, `"100GiB" is an invalid value for field "size"`) {
			t.Fatalf("expected invalid size error, got ok=%v reason=%q", ok, reason)
		}
	})

	t.Run("WhenNonNumericSize", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		r := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/abcd-1234", bytes.NewBufferString(`{"name":"vol1","size":"abc"}`))
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeModification(r)
		if ok || !strings.Contains(reason, `"abc" is an invalid value for field "size"`) {
			t.Fatalf("expected invalid size error for abc, got ok=%v reason=%q", ok, reason)
		}
	})
}

func TestValidateVolumeDeletion(t *testing.T) {
	origSubmit := submitExpertModeVolumeOperation
	defer func() { submitExpertModeVolumeOperation = origSubmit }()
	const cacheKey = "unit-test-cache-key-del"

	// success
	t.Run("WhenSuccess", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		r := httptest.NewRequest(http.MethodDelete, "/api/storage/volumes/abcd-1234", nil)
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeDeletion(r)
		if !ok || reason != "" {
			t.Fatalf("expected success, got ok=%v reason=%q", ok, reason)
		}
	})

	// missing cache key
	t.Run("WhenMissingCacheKey", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		r := httptest.NewRequest(http.MethodDelete, "/api/storage/volumes/abcd-1234", nil)
		ok, reason := _validateVolumeDeletion(r)
		if ok || !strings.Contains(reason, "cache key not found") {
			t.Fatalf("expected cache key error, got ok=%v reason=%q", ok, reason)
		}
	})

	// missing auth data
	t.Run("WhenMissingAuthdata", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		ctx := context.WithValue(context.Background(), models.AuthDataKey, "wrong-key")
		r := httptest.NewRequest(http.MethodDelete, "/api/storage/volumes/abcd-1234", nil)
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeDeletion(r)
		if ok || !strings.Contains(reason, "auth data not found") {
			t.Fatalf("expected missing auth data error, got ok=%v reason=%q", ok, reason)
		}
	})

	// persist fails
	t.Run("WhenPersistFails", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return errors.New("persist failed")
		}
		ctx := context.Background()
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{AccountName: "acc", PoolID: "pool"})
		ctx = context.WithValue(ctx, models.AuthDataKey, cacheKey)
		r := httptest.NewRequest(http.MethodDelete, "/api/storage/volumes/abcd-1234", nil)
		r = r.WithContext(ctx)
		ok, reason := _validateVolumeDeletion(r)
		if ok || !strings.Contains(reason, "persist failed") {
			t.Fatalf("expected persist failure, got ok=%v reason=%q", ok, reason)
		}
	})
}
