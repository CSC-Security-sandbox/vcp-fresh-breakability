package middleware

import (
	"context"
	"fmt"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func CertificateMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := log.NewLogger()

			cacheKey := cache.GetAuthDataKeyFromContext(r.Context())
			if cacheKey == "" {
				logger.Error("No cache key found in context")
				http.Error(w, "Cache key not available", http.StatusInternalServerError)
				return
			}

			authData, exists := cache.GetFromAuthDataCache(cacheKey)
			if !exists || authData == nil {
				logger.Error("No authentication data found in cache", "cacheKey", cacheKey)
				http.Error(w, "Authentication data not available in cache", http.StatusInternalServerError)
				return
			}

			switch authData.AuthType {
			case models.USER_CERTIFICATE:
				if authData.CertificateID != "" && authData.Certificate == nil {
					certificate, err := getCertificateFromSecretManager(authData.CertificateID, logger)
					if err != nil {
						logger.Error("Failed to fetch certificate from secret manager", "error", err, "certificateID", authData.CertificateID)
						http.Error(w, "Failed to fetch certificate", http.StatusInternalServerError)
						return
					}

					authData.Certificate = certificate
					cache.UpdateAuthDataInCache(cacheKey, authData)

					logger.Info("Certificate fetched and added to auth data in cache",
						"certificateID", authData.CertificateID,
						"commonName", certificate.CommonName,
						"poolID", authData.PoolID,
						"cacheKey", cacheKey)
				} else if authData.Certificate != nil {
					logger.Info("Certificate already present in auth data",
						"certificateID", authData.CertificateID,
						"commonName", authData.Certificate.CommonName,
						"poolID", authData.PoolID,
						"cacheKey", cacheKey)
				}

			case models.USERNAME_PWD_SEC_MGR:
				if authData.SecretID != "" && authData.Password == "" {
					password, err := getPasswordFromSecretManager(authData.SecretID, logger)
					if err != nil {
						logger.Error("Failed to fetch password from secret manager", "error", err, "secretID", authData.SecretID)
						http.Error(w, "Failed to fetch password", http.StatusInternalServerError)
						return
					}

					authData.Password = password
					cache.UpdateAuthDataInCache(cacheKey, authData)

					logger.Info("Password fetched and added to auth data in cache",
						"secretID", authData.SecretID,
						"poolID", authData.PoolID,
						"cacheKey", cacheKey)
				} else if authData.Password != "" {
					logger.Info("Password already present in auth data",
						"secretID", authData.SecretID,
						"poolID", authData.PoolID,
						"cacheKey", cacheKey)
				}

			case models.USERNAME_PWD:
				logger.Info("Using basic username/password authentication",
					"poolID", authData.PoolID,
					"cacheKey", cacheKey)

			default:
				logger.Warn("Unknown authentication type", "authType", authData.AuthType, "poolID", authData.PoolID)
			}

			next.ServeHTTP(w, r)
		})
	}
}

func getCertificateFromSecretManager(certificateID string, logger log.Logger) (*models.Certificate, error) {
	logger.Info("Getting certificate from secret manager", "certificateID", certificateID)
	ctx := context.Background()

	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		logger.Error("Failed to get GCP service", "error", err, "certificateID", certificateID)
		return nil, fmt.Errorf("failed to get GCP service: %w", err)
	}

	certificateResponse, err := hyperscaler.GetCertificateAndPrivateKeyByID(
		gcpService,
		env.CaPoolDeployedProjectID,
		env.SecretManagerProjectID,
		env.Region,
		env.CaPoolName,
		certificateID,
	)
	if err != nil {
		logger.Error("Failed to get certificate and private key", "error", err, "certificateID", certificateID)
		return nil, fmt.Errorf("failed to get certificate and private key: %w", err)
	}

	cert := &models.Certificate{
		SignedCertificate:        certificateResponse.Certificate.PemCertificate,
		PrivateKey:               certificateResponse.Secret.SecretVersion.Value,
		CommonName:               certificateResponse.Certificate.SubjectCommonName,
		InterMediateCertificates: certificateResponse.Certificate.PemCertificateChain,
	}

	logger.Info("Successfully retrieved certificate from secret manager", "certificateID", certificateID)
	return cert, nil
}

func getPasswordFromSecretManager(secretID string, logger log.Logger) (string, error) {
	logger.Info("Getting password from cache or secret manager", "secretID", secretID)
	ctx := context.Background()

	password, err := hyperscaler.GetPasswordFromCacheOrSecretManager(ctx, secretID)
	if err != nil {
		logger.Error("Failed to get password from secret manager", "error", err, "secretID", secretID)
		return "", fmt.Errorf("failed to get password from secret manager: %w", err)
	}

	logger.Info("Password fetched and cached", "secretID", secretID)
	return password, nil
}
