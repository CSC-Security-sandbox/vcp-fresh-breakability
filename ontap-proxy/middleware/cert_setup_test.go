package middleware

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscalergoogle "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
)

func TestEnsureCertificateOrPassword(t *testing.T) {
	t.Run("WhenNoCacheKeyInContext_ShouldReturnError", func(t *testing.T) {
		ctx := context.Background()

		err := EnsureCertificateOrPassword(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no cache key found in context")
	})

	t.Run("WhenNoAuthDataInCache_ShouldReturnError", func(t *testing.T) {
		cacheKey := "missing-key"
		cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "authentication data not available in cache")
	})

	t.Run("WhenAuthTypeIsUSERNAME_PWD_ShouldSucceed", func(t *testing.T) {
		cacheKey := "username-pwd-key"
		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			Username: "testuser",
			Password: "testpass",
			PoolID:   "test-pool",
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.NoError(t, err)
	})

	t.Run("WhenAuthTypeIsUSER_CERTIFICATE_WithExistingCertificate_ShouldSucceed", func(t *testing.T) {
		cacheKey := "cert-exists-key"
		authData := &models.AuthData{
			AuthType:      models.USER_CERTIFICATE,
			CertificateID: "test-cert-id",
			Certificate: &models.Certificate{
				SignedCertificate: "existing-cert",
				CommonName:        "existing.example.com",
			},
			PoolID: "test-pool",
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.NoError(t, err)
	})

	t.Run("WhenAuthTypeIsUSER_CERTIFICATE_WithMissingCertificate_ShouldFetch", func(t *testing.T) {
		setupMocks()
		defer restoreMocks()

		cacheKey := "cert-missing-key"
		authData := &models.AuthData{
			AuthType:      models.USER_CERTIFICATE,
			CertificateID: "test-cert-id",
			Certificate:   nil, // No certificate
			PoolID:        "test-pool",
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.NoError(t, err)

		// Verify certificate was fetched and cached
		updatedAuthData, exists := cache.GetFromAuthDataCache(cacheKey)
		assert.True(t, exists)
		assert.NotNil(t, updatedAuthData.Certificate)
		assert.Equal(t, "test-cert.example.com", updatedAuthData.Certificate.CommonName)
	})

	t.Run("WhenAuthTypeIsUSER_CERTIFICATE_WithEmptyCertificateID_ShouldSucceed", func(t *testing.T) {
		cacheKey := "cert-empty-id-key"
		authData := &models.AuthData{
			AuthType:      models.USER_CERTIFICATE,
			CertificateID: "", // Empty certificate ID
			Certificate:   nil,
			PoolID:        "test-pool",
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.NoError(t, err)
	})

	t.Run("WhenAuthTypeIsUSERNAME_PWD_SEC_MGR_WithExistingPassword_ShouldSucceed", func(t *testing.T) {
		cacheKey := "pwd-exists-key"
		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD_SEC_MGR,
			SecretID: "test-secret-id",
			Password: "existing-password",
			PoolID:   "test-pool",
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.NoError(t, err)
	})

	t.Run("WhenAuthTypeIsUSERNAME_PWD_SEC_MGR_WithMissingPassword_ShouldFetch", func(t *testing.T) {
		setupMocks()
		defer restoreMocks()

		cacheKey := "pwd-missing-key"
		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD_SEC_MGR,
			SecretID: "test-secret-id",
			Password: "", // No password
			PoolID:   "test-pool",
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.NoError(t, err)

		// Verify password was fetched and cached
		updatedAuthData, exists := cache.GetFromAuthDataCache(cacheKey)
		assert.True(t, exists)
		assert.Equal(t, "mock-password", updatedAuthData.Password)
	})

	t.Run("WhenAuthTypeIsUSERNAME_PWD_SEC_MGR_WithEmptySecretID_ShouldSucceed", func(t *testing.T) {
		cacheKey := "secret-empty-id-key"
		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD_SEC_MGR,
			SecretID: "", // Empty secret ID
			Password: "",
			PoolID:   "test-pool",
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.NoError(t, err)
	})

	t.Run("WhenUnknownAuthType_ShouldSucceed", func(t *testing.T) {
		cacheKey := "unknown-auth-key"
		authData := &models.AuthData{
			AuthType: 999, // Unknown auth type
			PoolID:   "test-pool",
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.NoError(t, err)
	})
}

func TestEnsureCertificateOrPassword_ErrorCases(t *testing.T) {
	t.Run("WhenCertificateFetchFails_ShouldReturnError", func(t *testing.T) {
		hyperscaler.GetGCPService = func(ctx context.Context) (*hyperscalergoogle.GcpServices, error) {
			return nil, assert.AnError
		}
		defer restoreMocks()

		cacheKey := "cert-fetch-fail-key"
		authData := &models.AuthData{
			AuthType:      models.USER_CERTIFICATE,
			CertificateID: "test-cert-id",
			Certificate:   nil,
			PoolID:        "test-pool",
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch certificate")
	})

	t.Run("WhenPasswordFetchFails_ShouldReturnError", func(t *testing.T) {
		hyperscaler.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", assert.AnError
		}
		defer restoreMocks()

		cacheKey := "pwd-fetch-fail-key"
		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD_SEC_MGR,
			SecretID: "test-secret-id",
			Password: "",
			PoolID:   "test-pool",
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch password")
	})

	t.Run("WhenAuthDataIsNil_ShouldReturnError", func(t *testing.T) {
		cacheKey := "nil-auth-key"
		cache.AddToAuthDataCache(cacheKey, nil)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		err := EnsureCertificateOrPassword(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "authentication data not available in cache")
	})
}
