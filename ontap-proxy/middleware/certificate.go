package middleware

import (
	"context"
	"fmt"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	ontapproxymodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	ontapproxyutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func CertificateMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := util.GetLogger(r.Context())

			cacheKey := cache.GetAuthDataKeyFromContext(r.Context())
			if cacheKey == "" {
				logger.ErrorContext(r.Context(), "No cache key found in context")
				ontapproxyutils.WriteErrorResponse(w, http.StatusInternalServerError, "Cache key not available")
				return
			}

			authData, exists := cache.GetFromAuthDataCache(cacheKey)
			if !exists || authData == nil {
				logger.ErrorContext(r.Context(), "No authentication data found in cache", "cacheKey", cacheKey)
				ontapproxyutils.WriteErrorResponse(w, http.StatusInternalServerError, "Authentication data not available in cache")
				return
			}

			switch authData.AuthType {
			case ontapproxymodels.USER_CERTIFICATE:
				if authData.CertificateID != "" && authData.Certificate == nil {
					certificate, err := getCertificateFromSecretManager(r.Context(), authData, logger)
					if err != nil {
						logger.ErrorContext(r.Context(), "Failed to fetch certificate from secret manager", "error", err, "certificateID", authData.CertificateID)
						ontapproxyutils.WriteErrorResponse(w, http.StatusInternalServerError, "Failed to fetch certificate")
						return
					}

					authData.Certificate = certificate
					cache.UpdateAuthDataInCache(cacheKey, authData)

					logger.InfoContext(r.Context(), "Certificate fetched and added to auth data in cache",
						"certificateID", authData.CertificateID,
						"poolID", authData.PoolID,
						"cacheKey", cacheKey)
				} else if authData.Certificate != nil {
					logger.InfoContext(r.Context(), "Certificate already present in auth data",
						"certificateID", authData.CertificateID,
						"poolID", authData.PoolID,
						"cacheKey", cacheKey)
				}

			case ontapproxymodels.USERNAME_PWD_SEC_MGR:
				if authData.SecretID != "" && authData.Password == "" {
					password, err := getPasswordFromSecretManager(r.Context(), authData.SecretID, logger)
					if err != nil {
						logger.ErrorContext(r.Context(), "Failed to fetch password from secret manager", "error", err, "secretID", authData.SecretID)
						ontapproxyutils.WriteErrorResponse(w, http.StatusInternalServerError, "Failed to fetch password")
						return
					}

					authData.Password = password
					cache.UpdateAuthDataInCache(cacheKey, authData)

					logger.InfoContext(r.Context(), "Password fetched and added to auth data in cache",
						"secretID", authData.SecretID,
						"poolID", authData.PoolID,
						"cacheKey", cacheKey)
				} else if authData.Password != "" {
					logger.InfoContext(r.Context(), "Password already present in auth data",
						"secretID", authData.SecretID,
						"poolID", authData.PoolID,
						"cacheKey", cacheKey)
				}

			case ontapproxymodels.USERNAME_PWD:
				logger.InfoContext(r.Context(), "Using basic username/password authentication",
					"poolID", authData.PoolID,
					"cacheKey", cacheKey)

			default:
				logger.WarnContext(r.Context(), "Unknown authentication type", "authType", authData.AuthType, "poolID", authData.PoolID)
			}

			logger.DebugContext(r.Context(), "Certificate middleware processing completed", "authType", authData.AuthType, "poolID", authData.PoolID)
			next.ServeHTTP(w, r)
		})
	}
}

// parseCaURIFromAuthData parses CaURI from AuthData and returns CA components with fallback to env vars
func parseCaURIFromAuthData(authData *ontapproxymodels.AuthData, logger log.Logger) (caName, caPoolName, caPoolDeployedProjectID string) {
	certificateID := ""
	if authData != nil {
		certificateID = authData.CertificateID
	}

	if authData == nil || authData.CaURI == "" {
		logger.DebugContext(context.Background(), "Using environment variables for CA config", "certificateID", certificateID)
		return env.CaName, env.CaPoolName, env.CaPoolDeployedProjectID
	}

	caPoolDeployedProjectID, caPoolName, caName = env.ParseCaURI(authData.CaURI)

	if caPoolDeployedProjectID == "" {
		caPoolDeployedProjectID = env.CaPoolDeployedProjectID
		logger.DebugContext(context.Background(), "Using environment variable for caPoolDeployedProjectID", "certificateID", certificateID)
	} else {
		logger.DebugContext(context.Background(), "Using pool credential for caPoolDeployedProjectID", "certificateID", certificateID, "caPoolDeployedProjectID", caPoolDeployedProjectID)
	}

	if caPoolName == "" {
		caPoolName = env.CaPoolName
		logger.DebugContext(context.Background(), "Using environment variable for caPoolName", "certificateID", certificateID)
	} else {
		logger.DebugContext(context.Background(), "Using pool credential for caPoolName", "certificateID", certificateID, "caPoolName", caPoolName)
	}

	if caName == "" {
		caName = env.CaName
	}

	return caName, caPoolName, caPoolDeployedProjectID
}

func getCertificateFromSecretManager(ctx context.Context, authData *ontapproxymodels.AuthData, logger log.Logger) (*ontapproxymodels.Certificate, error) {
	if authData == nil {
		return nil, fmt.Errorf("authData is nil")
	}

	certificateID := authData.CertificateID
	if certificateID == "" {
		return nil, fmt.Errorf("certificateID is empty in authData")
	}

	logger.InfoContext(ctx, "Getting certificate from secret manager", "certificateID", certificateID)

	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get GCP service", "error", err, "certificateID", certificateID)
		return nil, fmt.Errorf("failed to get GCP service: %w", err)
	}

	// Parse CA URI from authData, fallback to environment variables if not set
	_, caPoolName, caPoolDeployedProjectID := parseCaURIFromAuthData(authData, logger)

	region := env.Region

	certificateResponse, err := hyperscaler.GetCertificateAndPrivateKeyByID(
		gcpService,
		caPoolDeployedProjectID,
		env.SecretManagerProjectID,
		region,
		caPoolName,
		certificateID,
	)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get certificate and private key", "error", err, "certificateID", certificateID)
		return nil, fmt.Errorf("failed to get certificate and private key: %w", err)
	}

	cert := &ontapproxymodels.Certificate{
		SignedCertificate:        certificateResponse.Certificate.PemCertificate,
		PrivateKey:               certificateResponse.Secret.SecretVersion.Value,
		CommonName:               certificateResponse.Certificate.SubjectCommonName,
		InterMediateCertificates: certificateResponse.Certificate.PemCertificateChain,
	}

	logger.InfoContext(ctx, "Successfully retrieved certificate from secret manager", "certificateID", certificateID)
	return cert, nil
}

func getPasswordFromSecretManager(ctx context.Context, secretID string, logger log.Logger) (string, error) {
	logger.InfoContext(ctx, "Getting password from cache or secret manager", "secretID", secretID)

	password, err := hyperscaler.GetPasswordFromCacheOrSecretManager(ctx, secretID)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get password from secret manager", "error", err, "secretID", secretID)
		return "", fmt.Errorf("failed to get password from secret manager: %w", err)
	}

	logger.InfoContext(ctx, "Password fetched and cached", "secretID", secretID)
	return password, nil
}
