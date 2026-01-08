package middleware

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	ontapproxymodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// EnsureCertificateOrPassword fetches certificate or password from secret manager
// if needed based on the auth type. This extracts core logic from CertificateMiddleware
// for use in ogen handlers.
//
// Prerequisite: Context must have AuthDataKey set (call SetupCredentialsForHandler first).
//
// This function:
//   - For USER_CERTIFICATE auth: Fetches certificate from Secret Manager if not cached
//   - For USERNAME_PWD_SEC_MGR auth: Fetches password from Secret Manager if not cached
//   - For USERNAME_PWD auth: No action needed (credentials already available)
func EnsureCertificateOrPassword(ctx context.Context) error {
	logger := util.GetLogger(ctx)

	cacheKey := cache.GetAuthDataKeyFromContext(ctx)
	if cacheKey == "" {
		return fmt.Errorf("no cache key found in context - call SetupCredentialsForHandler first")
	}

	authData, exists := cache.GetFromAuthDataCache(cacheKey)
	if !exists || authData == nil {
		return fmt.Errorf("authentication data not available in cache for key: %s", cacheKey)
	}

	switch authData.AuthType {
	case ontapproxymodels.USER_CERTIFICATE:
		if authData.CertificateID != "" && authData.Certificate == nil {
			logger.InfoContext(ctx, "Fetching certificate from secret manager",
				"certificateID", authData.CertificateID,
				"poolID", authData.PoolID)

			certificate, err := getCertificateFromSecretManager(ctx, authData, logger)
			if err != nil {
				return fmt.Errorf("failed to fetch certificate: %w", err)
			}

			authData.Certificate = certificate
			cache.UpdateAuthDataInCache(cacheKey, authData)

			logger.InfoContext(ctx, "Certificate fetched and cached for handler",
				"certificateID", authData.CertificateID,
				"commonName", certificate.CommonName,
				"poolID", authData.PoolID)
		} else if authData.Certificate != nil {
			logger.DebugContext(ctx, "Certificate already present in auth data",
				"certificateID", authData.CertificateID,
				"poolID", authData.PoolID)
		}

	case ontapproxymodels.USERNAME_PWD_SEC_MGR:
		if authData.SecretID != "" && authData.Password == "" {
			logger.InfoContext(ctx, "Fetching password from secret manager",
				"secretID", authData.SecretID,
				"poolID", authData.PoolID)

			password, err := getPasswordFromSecretManager(ctx, authData.SecretID, logger)
			if err != nil {
				return fmt.Errorf("failed to fetch password: %w", err)
			}

			authData.Password = password
			cache.UpdateAuthDataInCache(cacheKey, authData)

			logger.InfoContext(ctx, "Password fetched and cached for handler",
				"secretID", authData.SecretID,
				"poolID", authData.PoolID)
		} else if authData.Password != "" {
			logger.DebugContext(ctx, "Password already present in auth data",
				"secretID", authData.SecretID,
				"poolID", authData.PoolID)
		}

	case ontapproxymodels.USERNAME_PWD:
		logger.DebugContext(ctx, "Using basic username/password authentication",
			"poolID", authData.PoolID)

	default:
		logger.WarnContext(ctx, "Unknown authentication type",
			"authType", authData.AuthType,
			"poolID", authData.PoolID)
	}

	return nil
}
