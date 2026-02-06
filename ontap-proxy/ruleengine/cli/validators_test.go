package cli

import (
	"context"
	"errors"
	"testing"

	coreapi "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/core-api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func Test_validateVolumeCreate(t *testing.T) {
	origSubmit := submitExpertModeVolumeOperation
	defer func() { submitExpertModeVolumeOperation = origSubmit }()

	t.Run("WhenCacheKeyMissing_ShouldReturnNotAllowed", func(t *testing.T) {
		ctx := context.Background()
		cmd := &CLICommand{
			FullCommand: "volume create",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
				"-size":    "10g",
			},
		}

		allowed, reason := _validateVolumeCreate(ctx, cmd)

		if allowed {
			t.Error("Expected not allowed when cache key missing")
		}
		if reason == "" {
			t.Error("Expected non-empty reason")
		}
		if reason != "cache key not found in context" {
			t.Errorf("Reason = %q, want cache key not found", reason)
		}
	})

	t.Run("WhenAuthDataNotFoundInCache_ShouldReturnNotAllowed", func(t *testing.T) {
		wrongKey := "wrong-pool-key"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, wrongKey)
		cmd := &CLICommand{
			FullCommand: "volume create",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
				"-size":    "10g",
			},
		}

		allowed, reason := _validateVolumeCreate(ctx, cmd)

		if allowed {
			t.Error("Expected not allowed when auth data not in cache")
		}
		if reason == "" {
			t.Error("Expected non-empty reason")
		}
		wantPrefix := "auth data not found in cache for key:"
		if len(reason) < len(wantPrefix) || reason[:len(wantPrefix)] != wantPrefix {
			t.Errorf("Reason = %q, want prefix %q", reason, wantPrefix)
		}
	})

	t.Run("WhenInvalidSize_ShouldReturnNotAllowed", func(t *testing.T) {
		cacheKey := "test-pool-key"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "volume create",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
				"-size":    "invalid",
			},
		}
		allowed, reason := _validateVolumeCreate(ctx, cmd)
		if allowed {
			t.Error("Expected not allowed for invalid size")
		}
		if reason == "" {
			t.Error("Expected non-empty reason")
		}
		if reason != `"invalid" is an invalid value for argument "-size"` {
			t.Errorf("Reason = %q", reason)
		}
	})

	t.Run("WhenCoreSucceeds_ShouldReturnAllowed", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		cacheKey := "test-pool-key-create-success"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "volume create",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
				"-size":    "10g",
			},
		}
		allowed, reason := _validateVolumeCreate(ctx, cmd)
		if !allowed {
			t.Errorf("Expected allowed when core succeeds, got reason = %q", reason)
		}
		if reason != "" {
			t.Errorf("Expected empty reason on success, got %q", reason)
		}
	})

	t.Run("WhenCoreReturnsError_ShouldReturnNotAllowed", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return errors.New("core validation failed")
		}
		cacheKey := "test-pool-key-create-core-err"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "volume create",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
				"-size":    "10g",
			},
		}
		allowed, reason := _validateVolumeCreate(ctx, cmd)
		if allowed {
			t.Error("Expected not allowed when core returns error")
		}
		if reason != "core validation failed" {
			t.Errorf("Reason = %q, want core validation failed", reason)
		}
	})
}

func Test_validateVolumeDelete(t *testing.T) {
	origSubmit := submitExpertModeVolumeOperation
	defer func() { submitExpertModeVolumeOperation = origSubmit }()

	t.Run("WhenCacheKeyMissing_ShouldReturnNotAllowed", func(t *testing.T) {
		ctx := context.Background()
		cmd := &CLICommand{
			FullCommand: "volume delete",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
			},
		}

		allowed, reason := _validateVolumeDelete(ctx, cmd)

		if allowed {
			t.Error("Expected not allowed when cache key missing")
		}
		if reason == "" {
			t.Error("Expected non-empty reason")
		}
		if reason != "cache key not found in context" {
			t.Errorf("Reason = %q, want cache key not found", reason)
		}
	})

	t.Run("WhenAuthDataNotFoundInCache_ShouldReturnNotAllowed", func(t *testing.T) {
		wrongKey := "wrong-pool-key-delete"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, wrongKey)
		cmd := &CLICommand{
			FullCommand: "volume delete",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
			},
		}

		allowed, reason := _validateVolumeDelete(ctx, cmd)

		if allowed {
			t.Error("Expected not allowed when auth data not in cache")
		}
		if reason == "" {
			t.Error("Expected non-empty reason")
		}
		wantPrefix := "auth data not found in cache for key:"
		if len(reason) < len(wantPrefix) || reason[:len(wantPrefix)] != wantPrefix {
			t.Errorf("Reason = %q, want prefix %q", reason, wantPrefix)
		}
	})

	t.Run("WhenCoreSucceeds_ShouldReturnAllowed", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return nil
		}
		cacheKey := "test-pool-key-delete-success"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "volume delete",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
			},
		}
		allowed, reason := _validateVolumeDelete(ctx, cmd)
		if !allowed {
			t.Errorf("Expected allowed when core succeeds, got reason = %q", reason)
		}
		if reason != "" {
			t.Errorf("Expected empty reason on success, got %q", reason)
		}
	})

	t.Run("WhenCoreReturnsError_ShouldReturnNotAllowed", func(t *testing.T) {
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return errors.New("volume not found")
		}
		cacheKey := "test-pool-key-delete-core-err"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "volume delete",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
			},
		}
		allowed, reason := _validateVolumeDelete(ctx, cmd)
		if allowed {
			t.Error("Expected not allowed when core returns error")
		}
		if reason != "volume not found" {
			t.Errorf("Reason = %q, want volume not found", reason)
		}
	})
}
