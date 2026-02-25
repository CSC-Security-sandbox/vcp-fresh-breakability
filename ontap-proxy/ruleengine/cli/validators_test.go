package cli

import (
	"context"
	"errors"
	"strings"
	"testing"

	coreapi "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/core-api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func Test_validateVolumeCreate(t *testing.T) {
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
		if !strings.HasPrefix(reason, wantPrefix) {
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
		origSubmit := submitExpertModeVolumeOperation
		defer func() { submitExpertModeVolumeOperation = origSubmit }()
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
		origSubmit := submitExpertModeVolumeOperation
		defer func() { submitExpertModeVolumeOperation = origSubmit }()
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
		if !strings.HasPrefix(reason, wantPrefix) {
			t.Errorf("Reason = %q, want prefix %q", reason, wantPrefix)
		}
	})

	t.Run("WhenCoreSucceeds_ShouldReturnAllowed", func(t *testing.T) {
		origSubmit := submitExpertModeVolumeOperation
		defer func() { submitExpertModeVolumeOperation = origSubmit }()
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
		origSubmit := submitExpertModeVolumeOperation
		defer func() { submitExpertModeVolumeOperation = origSubmit }()
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

func Test_validateVolumeUpdate(t *testing.T) {
	t.Run("WhenSizeNotPresent_ShouldReturnAllowed", func(t *testing.T) {
		ctx := context.Background()
		cmd := &CLICommand{
			FullCommand: "volume modify",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
			},
		}

		allowed, reason := _validateVolumeUpdate(ctx, cmd)

		if !allowed {
			t.Errorf("Expected allowed when -size not present, got reason = %q", reason)
		}
		if reason != "" {
			t.Errorf("Expected empty reason when -size not present, got %q", reason)
		}
	})

	t.Run("WhenCacheKeyMissing_ShouldReturnNotAllowed", func(t *testing.T) {
		ctx := context.Background()
		cmd := &CLICommand{
			FullCommand: "volume modify",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
				"-size":    "10g",
			},
		}

		allowed, reason := _validateVolumeUpdate(ctx, cmd)

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
		wrongKey := "wrong-pool-key-update"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, wrongKey)
		cmd := &CLICommand{
			FullCommand: "volume modify",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
				"-size":    "10g",
			},
		}

		allowed, reason := _validateVolumeUpdate(ctx, cmd)

		if allowed {
			t.Error("Expected not allowed when auth data not in cache")
		}
		if reason == "" {
			t.Error("Expected non-empty reason")
		}
		wantPrefix := "auth data not found in cache for key:"
		if !strings.HasPrefix(reason, wantPrefix) {
			t.Errorf("Reason = %q, want prefix %q", reason, wantPrefix)
		}
	})

	t.Run("WhenInvalidSize_ShouldReturnNotAllowed", func(t *testing.T) {
		cacheKey := "test-pool-key-update-invalid-size"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "volume modify",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
				"-size":    "invalid",
			},
		}
		allowed, reason := _validateVolumeUpdate(ctx, cmd)
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

	t.Run("WhenNewSizePlus10g_ShouldReturnNotAllowed", func(t *testing.T) {
		cacheKey := "test-pool-key-update-plus-size"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "volume size",
			Arguments: map[string]string{
				"-vserver":  "vs1",
				"-volume":   "vol1",
				"-new-size": "+10g",
			},
		}
		allowed, reason := _validateVolumeUpdate(ctx, cmd)
		if allowed {
			t.Error("Expected not allowed for -new-size +10g")
		}
		if reason != `"+10g" is an invalid value for argument "-new-size"` {
			t.Errorf("Reason = %q", reason)
		}
	})

	t.Run("WhenNewSizeMinus10g_ShouldReturnNotAllowed", func(t *testing.T) {
		cacheKey := "test-pool-key-update-minus-size"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "volume size",
			Arguments: map[string]string{
				"-vserver":  "vs1",
				"-volume":   "vol1",
				"-new-size": "-10g",
			},
		}
		allowed, reason := _validateVolumeUpdate(ctx, cmd)
		if allowed {
			t.Error("Expected not allowed for -new-size -10g")
		}
		if reason != `"-10g" is an invalid value for argument "-new-size"` {
			t.Errorf("Reason = %q", reason)
		}
	})

	t.Run("WhenCoreSucceeds_ShouldReturnAllowed", func(t *testing.T) {
		origSubmit := submitExpertModeVolumeOperation
		defer func() { submitExpertModeVolumeOperation = origSubmit }()
		var capturedReq *coreapi.ExpertModeVolumeV1
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			capturedReq = req
			return nil
		}
		cacheKey := "test-pool-key-update-success"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "volume modify",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
				"-size":    "200g",
			},
		}
		allowed, reason := _validateVolumeUpdate(ctx, cmd)
		if !allowed {
			t.Errorf("Expected allowed when core succeeds, got reason = %q", reason)
		}
		if reason != "" {
			t.Errorf("Expected empty reason on success, got %q", reason)
		}
		if capturedReq == nil {
			t.Fatal("Expected request to be sent to core")
		}
		if capturedReq.Action != coreapi.ExpertModeVolumeV1ActionUpdate {
			t.Errorf("Action = %v, want Update", capturedReq.Action)
		}
		if capturedReq.VolumeName != "vol1" {
			t.Errorf("VolumeName = %q, want vol1", capturedReq.VolumeName)
		}
		if capturedReq.SizeInBytes == 0 {
			t.Error("Expected SizeInBytes to be set")
		}
	})

	t.Run("WhenCoreReturnsError_ShouldReturnNotAllowed", func(t *testing.T) {
		origSubmit := submitExpertModeVolumeOperation
		defer func() { submitExpertModeVolumeOperation = origSubmit }()
		submitExpertModeVolumeOperation = func(ctx context.Context, req *coreapi.ExpertModeVolumeV1, jwt string, logger log.Logger) error {
			return errors.New("update conflict")
		}
		cacheKey := "test-pool-key-update-core-err"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "volume modify",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
				"-size":    "10g",
			},
		}
		allowed, reason := _validateVolumeUpdate(ctx, cmd)
		if allowed {
			t.Error("Expected not allowed when core returns error")
		}
		if reason != "update conflict" {
			t.Errorf("Reason = %q, want update conflict", reason)
		}
	})
}

func Test_validateVolumeRename(t *testing.T) {
	origSubmit := submitExpertModeVolumeRename
	defer func() { submitExpertModeVolumeRename = origSubmit }()

	t.Run("WhenCacheKeyMissing_ShouldReturnNotAllowed", func(t *testing.T) {
		ctx := context.Background()
		cmd := &CLICommand{
			FullCommand: "vol rename",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "reconcile004",
				"-newname": "reconcile_update004",
			},
		}

		allowed, reason := _validateVolumeRename(ctx, cmd)

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
		wrongKey := "wrong-pool-key-rename"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, wrongKey)
		cmd := &CLICommand{
			FullCommand: "vol rename",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "reconcile004",
				"-newname": "reconcile_update004",
			},
		}

		allowed, reason := _validateVolumeRename(ctx, cmd)

		if allowed {
			t.Error("Expected not allowed when auth data not in cache")
		}
		if reason == "" {
			t.Error("Expected non-empty reason")
		}
		wantPrefix := "auth data not found in cache for key:"
		if !strings.HasPrefix(reason, wantPrefix) {
			t.Errorf("Reason = %q, want prefix %q", reason, wantPrefix)
		}
	})

	t.Run("WhenCoreSucceeds_ShouldReturnAllowed", func(t *testing.T) {
		var capturedReq *coreapi.ExpertModeVolumeRenameV1
		var capturedParams coreapi.V1ExpertModeVolumeRenameParams
		submitExpertModeVolumeRename = func(ctx context.Context, req *coreapi.ExpertModeVolumeRenameV1, params coreapi.V1ExpertModeVolumeRenameParams, jwt string, logger log.Logger) error {
			capturedReq = req
			capturedParams = params
			return nil
		}
		cacheKey := "test-pool-key-rename-success"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "vol rename",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "reconcile004",
				"-newname": "reconcile_update004",
			},
		}
		allowed, reason := _validateVolumeRename(ctx, cmd)
		if !allowed {
			t.Errorf("Expected allowed when core succeeds, got reason = %q", reason)
		}
		if reason != "" {
			t.Errorf("Expected empty reason on success, got %q", reason)
		}
		if capturedReq == nil {
			t.Fatal("Expected rename request to be sent to core")
		}
		if capturedReq.Name != "reconcile_update004" {
			t.Errorf("Request.Name = %q, want reconcile_update004", capturedReq.Name)
		}
		if capturedReq.ProjectNumber != "test-account" || capturedReq.PoolUUID != "pool-uuid" || capturedReq.SvmName != "vs1" {
			t.Errorf("Request context: project=%q pool=%q svm=%q", capturedReq.ProjectNumber, capturedReq.PoolUUID, capturedReq.SvmName)
		}
		if capturedParams.Name != "reconcile004" {
			t.Errorf("Params.Name (current volume) = %q, want reconcile004", capturedParams.Name)
		}
	})

	t.Run("WhenCoreReturnsError_ShouldReturnNotAllowed", func(t *testing.T) {
		submitExpertModeVolumeRename = func(ctx context.Context, req *coreapi.ExpertModeVolumeRenameV1, params coreapi.V1ExpertModeVolumeRenameParams, jwt string, logger log.Logger) error {
			return errors.New("volume not found")
		}
		cacheKey := "test-pool-key-rename-core-err"
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)
		cache.AddToAuthDataCache(cacheKey, &models.AuthData{
			AccountName: "test-account",
			PoolID:      "pool-uuid",
		})
		defer cache.RemoveFromAuthDataCache(cacheKey)

		cmd := &CLICommand{
			FullCommand: "vol rename",
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "reconcile004",
				"-newname": "reconcile_update004",
			},
		}
		allowed, reason := _validateVolumeRename(ctx, cmd)
		if allowed {
			t.Error("Expected not allowed when core returns error")
		}
		if reason != "volume not found" {
			t.Errorf("Reason = %q, want volume not found", reason)
		}
	})
}
