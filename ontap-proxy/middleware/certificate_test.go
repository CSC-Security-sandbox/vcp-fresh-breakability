package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscalergoogle "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var originalGetGCPService = hyperscaler.GetGCPService
var originalGetCertificateAndPrivateKeyByID = hyperscaler.GetCertificateAndPrivateKeyByID
var originalGetPasswordFromCacheOrSecretManager = hyperscaler.GetPasswordFromCacheOrSecretManager

func setupMocks() {
	hyperscaler.GetGCPService = func(ctx context.Context) (*hyperscalergoogle.GcpServices, error) {
		return &hyperscalergoogle.GcpServices{}, nil
	}
	hyperscaler.GetCertificateAndPrivateKeyByID = func(gcpService hyperscaler.GoogleServices, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID string) (*hyperscalermodels.CustomCertificateResponse, error) {
		return &hyperscalermodels.CustomCertificateResponse{
			Certificate: &hyperscalermodels.CustomCertificate{
				PemCertificate:      "-----BEGIN CERTIFICATE-----\nMOCK_CERTIFICATE\n-----END CERTIFICATE-----",
				SubjectCommonName:   "test-cert.example.com",
				PemCertificateChain: []string{"-----BEGIN CERTIFICATE-----\nMOCK_CHAIN\n-----END CERTIFICATE-----"},
			},
			Secret: &hyperscalermodels.CustomSecret{
				SecretVersion: &hyperscalermodels.CustomSecretVersion{
					Value: "-----BEGIN PRIVATE KEY-----\nMOCK_PRIVATE_KEY\n-----END PRIVATE KEY-----",
				},
			},
		}, nil
	}
	hyperscaler.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
		return "mock-password", nil
	}
}

func restoreMocks() {
	hyperscaler.GetGCPService = originalGetGCPService
	hyperscaler.GetCertificateAndPrivateKeyByID = originalGetCertificateAndPrivateKeyByID
	hyperscaler.GetPasswordFromCacheOrSecretManager = originalGetPasswordFromCacheOrSecretManager
}

func TestCertificateMiddleware(t *testing.T) {
	t.Run("WhenNoCacheKeyInContext_ShouldReturn500", func(t *testing.T) {
		middleware := CertificateMiddleware()

		req := httptest.NewRequest("GET", "/test", nil)
		w := httptest.NewRecorder()

		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("Next handler should not be called")
		})

		middleware(nextHandler).ServeHTTP(w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "Cache key not available")
	})

	t.Run("WhenNoAuthDataInCache_ShouldReturn500", func(t *testing.T) {
		middleware := CertificateMiddleware()

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, "test-key")
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		cache.RemoveFromAuthDataCache("test-key")

		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("Next handler should not be called")
		})

		middleware(nextHandler).ServeHTTP(w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "Authentication data not available in cache")
	})

	t.Run("WhenAuthTypeIsUSER_CERTIFICATE_WithExistingCertificate_ShouldCallNext", func(t *testing.T) {
		middleware := CertificateMiddleware()

		authData := &models.AuthData{
			AuthType:      models.USER_CERTIFICATE,
			CertificateID: "test-cert-id",
			Certificate: &models.Certificate{
				SignedCertificate: "existing-cert",
				CommonName:        "existing.example.com",
			},
			PoolID: "test-pool",
		}

		cacheKey := "test-key"
		cache.AddToAuthDataCache(cacheKey, authData)

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		nextHandlerCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextHandlerCalled = true
		})

		middleware(nextHandler).ServeHTTP(w, req)

		assert.True(t, nextHandlerCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("WhenAuthTypeIsUSER_CERTIFICATE_WithMissingCertificate_ShouldFetchAndUpdateCache", func(t *testing.T) {
		setupMocks()
		defer restoreMocks()

		middleware := CertificateMiddleware()

		authData := &models.AuthData{
			AuthType:      models.USER_CERTIFICATE,
			CertificateID: "test-cert-id",
			Certificate:   nil, // No certificate
			PoolID:        "test-pool",
		}

		cacheKey := "test-key"
		cache.AddToAuthDataCache(cacheKey, authData)

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		nextHandlerCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextHandlerCalled = true
		})

		middleware(nextHandler).ServeHTTP(w, req)

		assert.True(t, nextHandlerCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, w.Code)

		updatedAuthData, exists := cache.GetFromAuthDataCache(cacheKey)
		assert.True(t, exists, "Auth data should exist in cache")
		assert.NotNil(t, updatedAuthData.Certificate, "Certificate should be added to auth data")
		assert.Equal(t, "test-cert.example.com", updatedAuthData.Certificate.CommonName, "Certificate common name should match")
	})

	t.Run("WhenAuthTypeIsUSERNAME_PWD_SEC_MGR_WithExistingPassword_ShouldCallNext", func(t *testing.T) {
		middleware := CertificateMiddleware()

		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD_SEC_MGR,
			SecretID: "test-secret-id",
			Password: "existing-password",
			PoolID:   "test-pool",
		}

		cacheKey := "test-key"
		cache.AddToAuthDataCache(cacheKey, authData)

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		nextHandlerCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextHandlerCalled = true
		})

		middleware(nextHandler).ServeHTTP(w, req)

		assert.True(t, nextHandlerCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("WhenAuthTypeIsUSERNAME_PWD_SEC_MGR_WithMissingPassword_ShouldFetchAndUpdateCache", func(t *testing.T) {
		setupMocks()
		defer restoreMocks()

		middleware := CertificateMiddleware()

		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD_SEC_MGR,
			SecretID: "test-secret-id",
			Password: "", // No password
			PoolID:   "test-pool",
		}

		cacheKey := "test-key"
		cache.AddToAuthDataCache(cacheKey, authData)

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		nextHandlerCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextHandlerCalled = true
		})

		middleware(nextHandler).ServeHTTP(w, req)

		assert.True(t, nextHandlerCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, w.Code)

		updatedAuthData, exists := cache.GetFromAuthDataCache(cacheKey)
		assert.True(t, exists, "Auth data should exist in cache")
		assert.Equal(t, "mock-password", updatedAuthData.Password, "Password should be added to auth data")
	})

	t.Run("WhenAuthTypeDoesNotRequireFetching_ShouldCallNext", func(t *testing.T) {
		testCases := []struct {
			name     string
			authData *models.AuthData
		}{
			{
				name: "USERNAME_PWD",
				authData: &models.AuthData{
					AuthType: models.USERNAME_PWD,
					Username: "testuser",
					Password: "testpass",
					PoolID:   "test-pool",
				},
			},
			{
				name: "USER_CERTIFICATE_WithEmptyCertificateID",
				authData: &models.AuthData{
					AuthType:      models.USER_CERTIFICATE,
					CertificateID: "", // Empty certificate ID
					Certificate:   nil,
					PoolID:        "test-pool",
				},
			},
			{
				name: "USERNAME_PWD_SEC_MGR_WithEmptySecretID",
				authData: &models.AuthData{
					AuthType: models.USERNAME_PWD_SEC_MGR,
					SecretID: "", // Empty secret ID
					Password: "",
					PoolID:   "test-pool",
				},
			},
			{
				name: "UnknownAuthType",
				authData: &models.AuthData{
					AuthType: 999, // Unknown auth type
					PoolID:   "test-pool",
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				middleware := CertificateMiddleware()

				cacheKey := "test-key"
				cache.AddToAuthDataCache(cacheKey, tc.authData)

				req := httptest.NewRequest("GET", "/test", nil)
				ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
				req = req.WithContext(ctx)
				w := httptest.NewRecorder()

				nextHandlerCalled := false
				nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					nextHandlerCalled = true
				})

				middleware(nextHandler).ServeHTTP(w, req)

				assert.True(t, nextHandlerCalled, "Next handler should be called for %s", tc.name)
				assert.Equal(t, http.StatusOK, w.Code, "Should return 200 for %s", tc.name)
			})
		}
	})
}

func TestGetCertificateFromSecretManager(t *testing.T) {
	t.Run("WhenGCPServiceSucceeds_ShouldReturnCertificate", func(t *testing.T) {
		setupMocks()
		defer restoreMocks()

		logger := &log.MockLogger{}
		logger.On("Info", "Getting certificate from secret manager", "certificateID", "test-cert-id").Return()
		logger.On("Info", "Successfully retrieved certificate from secret manager", "certificateID", "test-cert-id").Return()

		certificateID := "test-cert-id"

		cert, err := getCertificateFromSecretManager(certificateID, logger)

		assert.NoError(t, err, "Should not return error when GCP service succeeds")
		assert.NotNil(t, cert, "Certificate should not be nil")
		assert.Equal(t, "test-cert.example.com", cert.CommonName, "Common name should match")
		assert.Equal(t, "-----BEGIN CERTIFICATE-----\nMOCK_CERTIFICATE\n-----END CERTIFICATE-----", cert.SignedCertificate, "Signed certificate should match")
		assert.Equal(t, "-----BEGIN PRIVATE KEY-----\nMOCK_PRIVATE_KEY\n-----END PRIVATE KEY-----", cert.PrivateKey, "Private key should match")
		assert.Len(t, cert.InterMediateCertificates, 1, "Should have one intermediate certificate")
		assert.Equal(t, "-----BEGIN CERTIFICATE-----\nMOCK_CHAIN\n-----END CERTIFICATE-----", cert.InterMediateCertificates[0], "Intermediate certificate should match")

		logger.AssertExpectations(t)
	})

	t.Run("WhenGCPServiceFails_ShouldReturnError", func(t *testing.T) {
		hyperscaler.GetGCPService = func(ctx context.Context) (*hyperscalergoogle.GcpServices, error) {
			return nil, assert.AnError
		}
		defer restoreMocks()

		logger := &log.MockLogger{}
		logger.On("Info", "Getting certificate from secret manager", "certificateID", "test-cert-id").Return()
		logger.On("Error", "Failed to get GCP service", "error", assert.AnError, "certificateID", "test-cert-id").Return()

		certificateID := "test-cert-id"

		cert, err := getCertificateFromSecretManager(certificateID, logger)

		assert.Error(t, err, "Should return error when GCP service fails")
		assert.Nil(t, cert, "Certificate should be nil")
		assert.Contains(t, err.Error(), "failed to get GCP service", "Error should contain expected message")

		logger.AssertExpectations(t)
	})

	t.Run("WhenGetCertificateAndPrivateKeyByIDFails_ShouldReturnError", func(t *testing.T) {
		hyperscaler.GetGCPService = func(ctx context.Context) (*hyperscalergoogle.GcpServices, error) {
			return &hyperscalergoogle.GcpServices{}, nil
		}
		hyperscaler.GetCertificateAndPrivateKeyByID = func(gcpService hyperscaler.GoogleServices, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID string) (*hyperscalermodels.CustomCertificateResponse, error) {
			return nil, assert.AnError
		}
		defer restoreMocks()

		logger := &log.MockLogger{}
		logger.On("Info", "Getting certificate from secret manager", "certificateID", "test-cert-id").Return()
		logger.On("Error", "Failed to get certificate and private key", "error", assert.AnError, "certificateID", "test-cert-id").Return()

		certificateID := "test-cert-id"

		cert, err := getCertificateFromSecretManager(certificateID, logger)

		assert.Error(t, err, "Should return error when GetCertificateAndPrivateKeyByID fails")
		assert.Nil(t, cert, "Certificate should be nil")
		assert.Contains(t, err.Error(), "failed to get certificate and private key", "Error should contain expected message")

		logger.AssertExpectations(t)
	})
}

func TestGetPasswordFromSecretManager(t *testing.T) {
	t.Run("WhenSecretManagerSucceeds_ShouldReturnPassword", func(t *testing.T) {
		setupMocks()
		defer restoreMocks()

		logger := &log.MockLogger{}
		logger.On("Info", "Getting password from cache or secret manager", "secretID", "test-secret-id").Return()
		logger.On("Info", "Password fetched and cached", "secretID", "test-secret-id").Return()

		secretID := "test-secret-id"

		password, err := getPasswordFromSecretManager(secretID, logger)

		assert.NoError(t, err, "Should not return error when secret manager succeeds")
		assert.Equal(t, "mock-password", password, "Password should match expected value")

		logger.AssertExpectations(t)
	})

	t.Run("WhenSecretManagerFails_ShouldReturnError", func(t *testing.T) {
		hyperscaler.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", assert.AnError
		}
		defer restoreMocks()

		logger := &log.MockLogger{}
		logger.On("Info", "Getting password from cache or secret manager", "secretID", "test-secret-id").Return()
		logger.On("Error", "Failed to get password from secret manager", "error", assert.AnError, "secretID", "test-secret-id").Return()

		secretID := "test-secret-id"

		password, err := getPasswordFromSecretManager(secretID, logger)

		assert.Error(t, err, "Should return error when secret manager fails")
		assert.Empty(t, password, "Password should be empty")
		assert.Contains(t, err.Error(), "failed to get password from secret manager", "Error should contain expected message")

		logger.AssertExpectations(t)
	})
}

func TestCertificateMiddleware_ErrorCases(t *testing.T) {
	testCases := []struct {
		name          string
		authData      *models.AuthData
		setupMock     func()
		expectedError string
	}{
		{
			name: "USER_CERTIFICATE_FetchFails",
			authData: &models.AuthData{
				AuthType:      models.USER_CERTIFICATE,
				CertificateID: "test-cert-id",
				Certificate:   nil, // No certificate
				PoolID:        "test-pool",
			},
			setupMock: func() {
				hyperscaler.GetGCPService = func(ctx context.Context) (*hyperscalergoogle.GcpServices, error) {
					return nil, assert.AnError
				}
			},
			expectedError: "Failed to fetch certificate",
		},
		{
			name: "USERNAME_PWD_SEC_MGR_FetchFails",
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD_SEC_MGR,
				SecretID: "test-secret-id",
				Password: "", // No password
				PoolID:   "test-pool",
			},
			setupMock: func() {
				hyperscaler.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
					return "", assert.AnError
				}
			},
			expectedError: "Failed to fetch password",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setupMock()
			defer restoreMocks()

			middleware := CertificateMiddleware()

			cacheKey := "test-key"
			cache.AddToAuthDataCache(cacheKey, tc.authData)

			req := httptest.NewRequest("GET", "/test", nil)
			ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
			req = req.WithContext(ctx)
			w := httptest.NewRecorder()

			nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("Next handler should not be called")
			})

			middleware(nextHandler).ServeHTTP(w, req)

			assert.Equal(t, http.StatusInternalServerError, w.Code)
			assert.Contains(t, w.Body.String(), tc.expectedError)
		})
	}
}

func TestCertificateMiddleware_EdgeCases(t *testing.T) {
	t.Run("WhenAuthDataIsNil_ShouldReturn500", func(t *testing.T) {
		middleware := CertificateMiddleware()

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, "test-key")
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		cache.AddToAuthDataCache("test-key", nil)

		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("Next handler should not be called")
		})

		middleware(nextHandler).ServeHTTP(w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "Authentication data not available in cache")
	})
}
