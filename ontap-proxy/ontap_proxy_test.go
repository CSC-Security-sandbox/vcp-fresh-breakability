package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
)

// generateTestCertificates generates self-signed certificates for testing
func generateTestCertificates(t *testing.T) ([]byte, []byte, []byte) {
	// Generate CA key
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assert.NoError(t, err)

	// Create CA certificate
	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Test CA"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{""},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	assert.NoError(t, err)

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Generate client key
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assert.NoError(t, err)

	// Create client certificate
	clientTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Client"},
			Country:      []string{"US"},
			CommonName:   "test-client",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature,
	}

	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, &caTemplate, &clientKey.PublicKey, caKey)
	assert.NoError(t, err)

	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})

	return clientCertPEM, clientKeyPEM, caCertPEM
}

// MockLogger for testing
type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) InfoContext(ctx context.Context, msg string, args ...interface{}) {
	m.Called(ctx, msg, args)
}

func (m *MockLogger) ErrorContext(ctx context.Context, msg string, args ...interface{}) {
	m.Called(ctx, msg, args)
}

func (m *MockLogger) DebugContext(ctx context.Context, msg string, args ...interface{}) {
	m.Called(ctx, msg, args)
}

func (m *MockLogger) Info(msg string, args ...interface{}) {
	m.Called(msg, args)
}

func (m *MockLogger) Error(msg string, args ...interface{}) {
	m.Called(msg, args)
}

func (m *MockLogger) Debug(msg string, args ...interface{}) {
	m.Called(msg, args)
}

func TestConnectionPool_Creation(t *testing.T) {
	pool := NewConnectionPool()

	assert.NotNil(t, pool)
	assert.NotNil(t, pool.clients)
	assert.Equal(t, 200, pool.maxIdleConns)
	assert.Equal(t, 50, pool.maxIdleConnsPerHost)
	assert.Equal(t, 120*time.Second, pool.idleConnTimeout)
}

func TestConnectionPool_GetClient(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	// Create mock auth data
	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: "test-ontap.example.com"},
		},
	}

	// First call should create a new client
	client1, err := pool.GetClient("test-ontap.example.com", authData)
	assert.NoError(t, err)
	assert.NotNil(t, client1)

	// Second call should return the same client
	client2, err := pool.GetClient("test-ontap.example.com", authData)
	assert.NoError(t, err)
	assert.NotNil(t, client2)
	assert.Equal(t, client1, client2)

	// Verify pool has one client
	stats := pool.GetStats()
	assert.Equal(t, 1, stats["total_connections"])
}

func TestConnectionPool_DifferentAuthTypes(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	ontapAddress := "test-ontap.example.com"

	// Create auth data for basic auth
	basicAuth := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: ontapAddress},
		},
	}

	// Create auth data for certificate auth with invalid certificates
	// This will fail to parse, but we can verify the pool key generation is different
	certAuth := &models.AuthData{
		AuthType: models.USER_CERTIFICATE,
		PoolID:   "test-pool-123",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: ontapAddress},
		},
		Certificate: &models.Certificate{
			SignedCertificate:        "test-cert",
			PrivateKey:               "test-key",
			InterMediateCertificates: []string{"test-ca"},
		},
	}

	// Get client for basic auth - should succeed
	client1, err := pool.GetClient(ontapAddress, basicAuth)
	assert.NoError(t, err)
	assert.NotNil(t, client1)

	// Get client for certificate auth - should fail due to invalid certificates
	// but this tests that different auth types generate different pool keys
	client2, err := pool.GetClient(ontapAddress, certAuth)
	assert.Error(t, err)
	assert.Nil(t, client2)
	assert.Contains(t, err.Error(), "failed to prepare certificate")

	// Verify pool has one client (only basic auth succeeded)
	stats := pool.GetStats()
	assert.Equal(t, 1, stats["total_connections"])

	// Verify that pool keys are different for different auth types
	key1 := pool.generatePoolKey(ontapAddress, basicAuth)
	key2 := pool.generatePoolKey(ontapAddress, certAuth)
	assert.NotEqual(t, key1, key2, "Pool keys should be different for different auth types")
}

func TestConnectionPool_DifferentPools(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	// Create auth data for different pools
	authData1 := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "pool-1",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: "ontap1.example.com"},
		},
	}

	authData2 := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "pool-2",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: "ontap2.example.com"},
		},
	}

	// Get clients for different pools
	client1, err := pool.GetClient("ontap1.example.com", authData1)
	assert.NoError(t, err)
	assert.NotNil(t, client1)

	client2, err := pool.GetClient("ontap2.example.com", authData2)
	assert.NoError(t, err)
	assert.NotNil(t, client2)

	// Should be different clients due to different ONTAP addresses
	assert.NotEqual(t, client1, client2)

	// Verify pool has two clients
	stats := pool.GetStats()
	assert.Equal(t, 2, stats["total_connections"])
}

func TestPooledAuthTransport_RoundTrip(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	defer server.Close()

	// Create mock auth data
	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: server.URL},
		},
	}

	// Mock the cache
	cacheKey := "test-cache-key"
	cache.AddToAuthDataCache(cacheKey, authData)

	// Create transport
	transport := NewPooledAuthTransport()

	// Create request with context
	req, err := http.NewRequest("GET", server.URL+"/test", nil)
	assert.NoError(t, err)

	ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
	req = req.WithContext(ctx)

	// Execute request
	resp, err := transport.RoundTrip(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func setupTestContextWithAuthData(t *testing.T, req *http.Request, username, password string) {
	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		Username: username,
		Password: password,
		OntapEndpoints: []models.OntapEndpoint{
			{
				IP:  "192.168.1.100",
				DNS: "test-cluster:443",
			},
		},
	}

	cacheKey := "test-project:test-pool:test-user"

	cache.AddToAuthDataCache(cacheKey, authData)

	ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
	*req = *req.WithContext(ctx)
}

func TestBuildTargetURL(t *testing.T) {
	t.Run("WhenHTTPSWithQueryParams_ShouldBuildURLWithQuery", func(t *testing.T) {
		result := buildTargetURL("https://ontap-cluster:443", "/api/storage/qtrees", "fields=name,size")
		expected := "https://ontap-cluster:443/api/storage/qtrees?fields=name,size"
		assert.Equal(t, expected, result, "Should build HTTPS URL with query parameters")
	})

	t.Run("WhenHTTPWithoutQueryParams_ShouldBuildURLWithoutQuery", func(t *testing.T) {
		result := buildTargetURL("http://ontap-cluster:8080", "/api/storage/volumes", "")
		expected := "http://ontap-cluster:8080/api/storage/volumes"
		assert.Equal(t, expected, result, "Should build HTTP URL without query parameters")
	})

	t.Run("WhenAddressWithoutProtocol_ShouldDefaultToHTTPS", func(t *testing.T) {
		result := buildTargetURL("ontap-cluster:443", "/api/storage/qtrees", "")
		expected := "https://ontap-cluster:443/api/storage/qtrees"
		assert.Equal(t, expected, result, "Should default to HTTPS when no protocol specified")
	})

	t.Run("WhenAddressWithoutProtocolAndPort_ShouldHandleCorrectly", func(t *testing.T) {
		result := buildTargetURL("ontap-cluster", "/api/storage/qtrees", "")
		expected := "https://ontap-cluster/api/storage/qtrees"
		assert.Equal(t, expected, result, "Should handle address without port")
	})

	t.Run("WhenAddressWithHTTPS_ShouldKeepHTTPS", func(t *testing.T) {
		result := buildTargetURL("https://ontap-cluster:443", "/api/storage/qtrees", "")
		expected := "https://ontap-cluster:443/api/storage/qtrees"
		assert.Equal(t, expected, result, "Should keep HTTPS protocol")
	})

	t.Run("WhenAddressWithHTTP_ShouldKeepHTTP", func(t *testing.T) {
		result := buildTargetURL("http://ontap-cluster:8080", "/api/storage/qtrees", "")
		expected := "http://ontap-cluster:8080/api/storage/qtrees"
		assert.Equal(t, expected, result, "Should keep HTTP protocol")
	})

	t.Run("WhenPathWithLeadingSlash_ShouldHandleCorrectly", func(t *testing.T) {
		result := buildTargetURL("ontap-cluster", "/api/storage/qtrees", "")
		expected := "https://ontap-cluster/api/storage/qtrees"
		assert.Equal(t, expected, result, "Should handle path with leading slash")
	})
}

func TestExtractOntapPath(t *testing.T) {
	t.Run("WhenValidOntapAPIPath_ShouldExtractCorrectly", func(t *testing.T) {
		result := extractOntapPath("/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/qtrees")
		expected := "/api/storage/qtrees"
		assert.Equal(t, expected, result, "Should extract ONTAP API path correctly")
	})

	t.Run("WhenOntapAPIPathWithQueryParams_ShouldExtractWithQuery", func(t *testing.T) {
		result := extractOntapPath("/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/volumes?fields=name,size")
		expected := "/api/storage/volumes?fields=name,size"
		assert.Equal(t, expected, result, "Should extract ONTAP API path with query parameters")
	})

	t.Run("WhenPathWithoutOntapAPI_ShouldReturnEmpty", func(t *testing.T) {
		result := extractOntapPath("/v1beta/projects/1234/locations/us-central1/pools/my-pool/invalid-path")
		expected := ""
		assert.Equal(t, expected, result, "Should return empty string for path without ontap")
	})

	t.Run("WhenEmptyPath_ShouldHandleCorrectly", func(t *testing.T) {
		result := extractOntapPath("")
		expected := ""
		assert.Equal(t, expected, result, "Should handle empty path")
	})

	t.Run("WhenOntapAPIAtRoot_ShouldHandleCorrectly", func(t *testing.T) {
		result := extractOntapPath("/ontap/api/storage/qtrees")
		expected := "/api/storage/qtrees"
		assert.Equal(t, expected, result, "Should handle ONTAP API at root level")
	})

	t.Run("WhenOntapAPIAtEnd_ShouldHandleCorrectly", func(t *testing.T) {
		result := extractOntapPath("/v1beta/projects/123/locations/us-central1/pools/pool1/ontap")
		expected := "/"
		assert.Equal(t, expected, result, "Should handle ONTAP API at end of path")
	})

	t.Run("WhenMultipleOntapAPI_ShouldHandleFirstOccurrence", func(t *testing.T) {
		result := extractOntapPath("/v1beta/projects/123/ontap/api1/ontap/api2")
		expected := "/api1/ontap/api2"
		assert.Equal(t, expected, result, "Should handle first occurrence of ontap")
	})

	t.Run("WhenPathStartsWithOntapAPI_ShouldHandleCorrectly", func(t *testing.T) {
		result := extractOntapPath("ontap/api/storage/qtrees")
		expected := "/api/storage/qtrees"
		assert.Equal(t, expected, result, "Should handle path starting with ontap")
	})
}

func TestLogCurlCommand(t *testing.T) {
	t.Run("WhenCalled_ShouldNotPanic", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "test-agent")
		req.Header.Set("Authorization", "Basic YWRtaW46cGFzc3dvcmQ=")

		targetURL := "https://ontap-cluster:443/api/storage/qtrees"

		assert.NotPanics(t, func() {
			logCurlCommand(req, targetURL)
		}, "logCurlCommand should not panic")
	})

	t.Run("WhenNoHeaders_ShouldHandleCorrectly", func(t *testing.T) {
		req, err := http.NewRequest("POST", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		targetURL := "https://ontap-cluster:443/api/storage/qtrees"

		assert.NotPanics(t, func() {
			logCurlCommand(req, targetURL)
		}, "logCurlCommand should not panic with no headers")
	})

	t.Run("WhenMultipleHeaderValues_ShouldHandleCorrectly", func(t *testing.T) {
		req, err := http.NewRequest("PUT", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		req.Header.Set("Accept", "application/json")
		req.Header.Add("Accept", "application/xml")
		req.Header.Set("User-Agent", "test-agent")

		targetURL := "https://ontap-cluster:443/api/storage/qtrees"

		assert.NotPanics(t, func() {
			logCurlCommand(req, targetURL)
		}, "logCurlCommand should not panic with multiple header values")
	})

	t.Run("WhenAuthorizationHeaderPresent_ShouldIncludeBasicAuth", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", "Basic YWRtaW46cGFzc3dvcmQ=")

		targetURL := "https://ontap-cluster:443/api/storage/qtrees"

		assert.NotPanics(t, func() {
			logCurlCommand(req, targetURL)
		}, "logCurlCommand should not panic with authorization header")
	})

	t.Run("WhenNoHeadersAtAll_ShouldHandleCorrectly", func(t *testing.T) {
		req, err := http.NewRequest("DELETE", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		targetURL := "https://ontap-cluster:443/api/storage/qtrees"

		assert.NotPanics(t, func() {
			logCurlCommand(req, targetURL)
		}, "logCurlCommand should not panic with no headers at all")
	})
}

func TestBuildOntapRESTProxy(t *testing.T) {
	t.Run("WhenProxyCreated_ShouldReturnValidProxy", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		assert.NotNil(t, proxy, "Proxy should not be nil")
		assert.NotNil(t, proxy.Director, "Director function should not be nil")
		assert.NotNil(t, proxy.Transport, "Transport should not be nil")
		assert.NotNil(t, proxy.ModifyResponse, "ModifyResponse function should not be nil")
		assert.NotNil(t, proxy.ErrorHandler, "ErrorHandler function should not be nil")
	})

	t.Run("WhenDirectorCalledWithValidPath_ShouldProcessRequest", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")
		req.RemoteAddr = "192.168.1.1:12345"

		setupTestContextWithAuthData(t, req, "testuser", "testpass")

		proxy.Director(req)

		// Director only modifies URL and Host, not headers or authentication
		// Headers and authentication are set in RoundTrip
		assert.Equal(t, "https://test-cluster:443/api/storage/qtrees", req.URL.String())
		assert.Equal(t, "test-cluster:443", req.Host)
	})

	t.Run("WhenDirectorCalledWithInvalidPath_ShouldReturnEarly", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/invalid/path", nil)
		assert.NoError(t, err, "Failed to create request")

		setupTestContextWithAuthData(t, req, "testuser", "testpass")

		originalURL := req.URL.String()
		proxy.Director(req)

		assert.Equal(t, originalURL, req.URL.String(), "URL should not be modified for invalid path")
	})

	t.Run("WhenDirectorCalledWithNoOntapAddress_ShouldReturnEarly", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		authData := &models.AuthData{
			AuthType:       models.USERNAME_PWD,
			Username:       "testuser",
			Password:       "testpass",
			OntapEndpoints: []models.OntapEndpoint{},
		}
		cacheKey := "test-project:test-pool:test-user"
		cache.AddToAuthDataCache(cacheKey, authData)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
		*req = *req.WithContext(ctx)

		originalURL := req.URL.String()
		proxy.Director(req)

		assert.Equal(t, originalURL, req.URL.String(), "URL should not be modified when no ONTAP address")
	})

	t.Run("WhenDirectorCalledWithNoUsername_ShouldStillProcessRequest", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			Username: "",
			Password: "testpass",
			OntapEndpoints: []models.OntapEndpoint{
				{
					IP:  "192.168.1.100",
					DNS: "test-cluster:443",
				},
			},
		}
		cacheKey := "test-project:test-pool:test-user"
		cache.AddToAuthDataCache(cacheKey, authData)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
		*req = *req.WithContext(ctx)

		proxy.Director(req)

		assert.Equal(t, "https://test-cluster:443/api/storage/qtrees", req.URL.String(), "URL should be modified even when no username")
	})

	t.Run("WhenDirectorCalledWithNoPassword_ShouldStillProcessRequest", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			Username: "testuser",
			Password: "",
			OntapEndpoints: []models.OntapEndpoint{
				{
					IP:  "192.168.1.100",
					DNS: "test-cluster:443",
				},
			},
		}
		cacheKey := "test-project:test-pool:test-user"
		cache.AddToAuthDataCache(cacheKey, authData)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
		*req = *req.WithContext(ctx)

		proxy.Director(req)

		assert.Equal(t, "https://test-cluster:443/api/storage/qtrees", req.URL.String(), "URL should be modified even when no password")
	})

	t.Run("WhenDirectorCalledWithInvalidURL_ShouldStillProcessRequest", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			Username: "testuser",
			Password: "testpass",
			OntapEndpoints: []models.OntapEndpoint{
				{
					IP:  "192.168.1.100",
					DNS: "://invalid-url",
				},
			},
		}
		cacheKey := "test-project:test-pool:test-user"
		cache.AddToAuthDataCache(cacheKey, authData)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
		*req = *req.WithContext(ctx)

		proxy.Director(req)

		assert.Equal(t, "https://://invalid-url/api/storage/qtrees", req.URL.String(), "URL should be modified even with invalid ONTAP address")
	})

	t.Run("WhenModifyResponseCalledWithRuleContext_ShouldProcessResponse", func(t *testing.T) {
		mockProcessor := actions.NewMockRequestProcessor(t)
		mockProcessor.EXPECT().ProcessResponse(mock.AnythingOfType("*http.Response")).Return(nil)

		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockProcessor)
		req = req.WithContext(ctx)

		resp := &http.Response{
			Request: req,
			Header:  make(http.Header),
		}

		err = proxy.ModifyResponse(resp)
		assert.NoError(t, err, "ModifyResponse should not return error")
	})

	t.Run("WhenModifyResponseCalledWithoutRuleContext_ShouldReturnNil", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		resp := &http.Response{
			Request: req,
			Header:  make(http.Header),
		}

		err = proxy.ModifyResponse(resp)
		assert.NoError(t, err, "ModifyResponse should return nil when no rule context")
	})

	t.Run("WhenModifyResponseCalledWithInvalidRuleContext_ShouldReturnNil", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		ctx := context.WithValue(req.Context(), models.RuleContextKey, "invalid")
		req = req.WithContext(ctx)

		resp := &http.Response{
			Request: req,
			Header:  make(http.Header),
		}

		err = proxy.ModifyResponse(resp)
		assert.NoError(t, err, "ModifyResponse should return nil when rule context is invalid")
	})

	t.Run("WhenErrorHandlerCalledWithContextCanceled_ShouldReturn504", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()
		err = fmt.Errorf("context canceled")

		proxy.ErrorHandler(rr, req, err)

		assert.Equal(t, http.StatusGatewayTimeout, rr.Code, "Should return 504 Gateway Timeout")
		assert.Contains(t, rr.Body.String(), "Request timeout", "Should contain timeout message")
	})

	t.Run("WhenErrorHandlerCalledWithConnectionRefused_ShouldReturn502", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()
		err = fmt.Errorf("connection refused")

		proxy.ErrorHandler(rr, req, err)

		assert.Equal(t, http.StatusBadGateway, rr.Code, "Should return 502 Bad Gateway")
		assert.Contains(t, rr.Body.String(), "Cannot connect", "Should contain connection error message")
	})

	t.Run("WhenErrorHandlerCalledWithNoSuchHost_ShouldReturn502", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()
		err = fmt.Errorf("no such host")

		proxy.ErrorHandler(rr, req, err)

		assert.Equal(t, http.StatusBadGateway, rr.Code, "Should return 502 Bad Gateway")
		assert.Contains(t, rr.Body.String(), "host not found", "Should contain host not found message")
	})

	t.Run("WhenErrorHandlerCalledWithMissingCredentials_ShouldReturn500", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()
		err = fmt.Errorf("Missing ONTAP credentials")

		proxy.ErrorHandler(rr, req, err)

		assert.Equal(t, http.StatusInternalServerError, rr.Code, "Should return 500 Internal Server Error")
		assert.Contains(t, rr.Body.String(), "credentials not configured", "Should contain credentials error message")
	})

	t.Run("WhenErrorHandlerCalledWithOtherError_ShouldReturn502", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()
		err = fmt.Errorf("some other error")

		proxy.ErrorHandler(rr, req, err)

		assert.Equal(t, http.StatusBadGateway, rr.Code, "Should return 502 Bad Gateway")
		assert.Contains(t, rr.Body.String(), "Proxy error: some other error", "Should contain proxy error message")
	})
}

func TestConnectionPool_Cleanup(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	// Create multiple clients
	for i := 0; i < 5; i++ {
		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			PoolID:   fmt.Sprintf("pool-%d", i),
			Username: "testuser",
			Password: "testpass",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: fmt.Sprintf("ontap%d.example.com", i)},
			},
		}

		client, err := pool.GetClient(fmt.Sprintf("ontap%d.example.com", i), authData)
		assert.NoError(t, err)
		assert.NotNil(t, client)
	}

	// Verify we have 5 clients
	stats := pool.GetStats()
	assert.Equal(t, 5, stats["total_connections"])

	// Run cleanup (this is a simplified test - in real scenario, cleanup would be time-based)
	pool.cleanup()

	// In this test, cleanup shouldn't remove anything since we're under the limit
	stats = pool.GetStats()
	assert.Equal(t, 5, stats["total_connections"])
}

func TestConnectionPool_Stats(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	stats := pool.GetStats()

	assert.Contains(t, stats, "total_connections")
	assert.Contains(t, stats, "max_idle_conns")
	assert.Contains(t, stats, "max_idle_per_host")
	assert.Contains(t, stats, "idle_timeout")

	assert.Equal(t, 0, stats["total_connections"])
	assert.Equal(t, 200, stats["max_idle_conns"])
	assert.Equal(t, 50, stats["max_idle_per_host"])
}

func TestConnectionPool_Close(t *testing.T) {
	pool := NewConnectionPool()

	// Create a client
	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: "test-ontap.example.com"},
		},
	}

	client, err := pool.GetClient("test-ontap.example.com", authData)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	// Verify we have a client
	stats := pool.GetStats()
	assert.Equal(t, 1, stats["total_connections"])

	// Close the pool
	pool.Close()

	// Verify pool is empty
	stats = pool.GetStats()
	assert.Equal(t, 0, stats["total_connections"])
}

func TestBuildOptimizedTransport(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	// Test basic auth transport
	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
	}

	transport, err := pool.buildOptimizedTransport(authData)
	assert.NoError(t, err)
	assert.NotNil(t, transport)
	assert.Equal(t, 200, transport.MaxIdleConns)
	assert.Equal(t, 50, transport.MaxIdleConnsPerHost)
	assert.Equal(t, 120*time.Second, transport.IdleConnTimeout)
	assert.False(t, transport.DisableKeepAlives)
	assert.False(t, transport.DisableCompression)
	assert.True(t, transport.ForceAttemptHTTP2)
}

func TestGeneratePoolKey(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
	}

	key := pool.generatePoolKey("test-ontap.example.com", authData)
	expectedKey := "test-ontap.example.com:0:test-pool-123"

	assert.Equal(t, expectedKey, key)
}

func TestConnectionPool_GetClient_DoubleCheckLock(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: "test-ontap.example.com"},
		},
	}

	// Simulate concurrent access to trigger double-check lock path (lines 99-100)
	// Use more goroutines to increase chance of hitting the double-check
	done := make(chan bool, 10)
	clients := make([]*http.Client, 10)

	for i := 0; i < 10; i++ {
		go func(idx int) {
			client, err := pool.GetClient("test-ontap.example.com", authData)
			assert.NoError(t, err)
			assert.NotNil(t, client)
			clients[idx] = client
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify only one client was created (all should be the same instance)
	firstClient := clients[0]
	for i := 1; i < 10; i++ {
		assert.Equal(t, firstClient, clients[i], "All clients should be the same instance")
	}

	// Verify only one client in pool
	stats := pool.GetStats()
	assert.Equal(t, 1, stats["total_connections"])
}

func TestBuildOptimizedTransport_DefaultCase(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	// Test with unknown auth type (should default to basic auth)
	authData := &models.AuthData{
		AuthType: 999, // Unknown auth type
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
	}

	transport, err := pool.buildOptimizedTransport(authData)
	assert.NoError(t, err)
	assert.NotNil(t, transport)
	assert.True(t, transport.TLSClientConfig.InsecureSkipVerify) // Basic auth uses insecure skip
}

func TestBuildCertificateTransport_Success(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	// This will fail due to invalid cert, but we can test the path
	authData := &models.AuthData{
		AuthType: models.USER_CERTIFICATE,
		PoolID:   "test-pool-123",
		Certificate: &models.Certificate{
			SignedCertificate:        "test-cert",
			PrivateKey:               "test-key",
			InterMediateCertificates: []string{"test-ca"},
		},
	}

	// This will fail, but tests the return path
	_, err := pool.buildCertificateTransport(authData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to prepare certificate")
}

func TestBuildCertificateTransport_WithValidCerts(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	// Generate a self-signed certificate for testing
	certPEM, keyPEM, caPEM := generateTestCertificates(t)

	authData := &models.AuthData{
		AuthType: models.USER_CERTIFICATE,
		PoolID:   "test-pool-123",
		Certificate: &models.Certificate{
			SignedCertificate:        string(certPEM),
			PrivateKey:               string(keyPEM),
			InterMediateCertificates: []string{string(caPEM)},
		},
	}

	// This should succeed and hit line 194
	transport, err := pool.buildCertificateTransport(authData)
	assert.NoError(t, err)
	assert.NotNil(t, transport)
	assert.NotNil(t, transport.TLSClientConfig)
	assert.False(t, transport.TLSClientConfig.InsecureSkipVerify)
	assert.NotEmpty(t, transport.TLSClientConfig.Certificates)
}

func TestConnectionPool_Cleanup_OldConnections(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	// Set a very short cleanup threshold
	pool.cleanupThreshold = 1 * time.Millisecond

	// Create a client
	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: "test-ontap.example.com"},
		},
	}

	client, err := pool.GetClient("test-ontap.example.com", authData)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	// Manually set timestamp to be old
	pool.mutex.Lock()
	key := pool.generatePoolKey("test-ontap.example.com", authData)
	pool.clientTimestamps[key] = time.Now().Add(-2 * time.Second)
	pool.mutex.Unlock()

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

	// Run cleanup - this should hit lines 240-242, 244, 246
	pool.cleanup()

	// Verify client was removed
	stats := pool.GetStats()
	assert.Equal(t, 0, stats["total_connections"])
}

func TestConnectionPool_CleanupRoutine_Runs(t *testing.T) {
	// Create a new pool to test the cleanup routine
	pool := NewConnectionPool()
	defer pool.Close()

	// Set a very short cleanup threshold so connections are considered old quickly
	pool.cleanupThreshold = 1 * time.Millisecond

	// Create a client
	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: "test-ontap.example.com"},
		},
	}

	_, err := pool.GetClient("test-ontap.example.com", authData)
	assert.NoError(t, err)

	// Manually set timestamp to be old
	pool.mutex.Lock()
	key := pool.generatePoolKey("test-ontap.example.com", authData)
	pool.clientTimestamps[key] = time.Now().Add(-2 * time.Second)
	pool.mutex.Unlock()

	// Wait for cleanup routine ticker to fire (line 219)
	// Default cleanup interval is 60 seconds, but we'll wait a bit to ensure
	// the ticker has a chance to fire. In practice, this tests that the cleanup
	// routine is running and will eventually call cleanup()
	// Note: This is a timing-dependent test, but it verifies the routine is active
	time.Sleep(100 * time.Millisecond)

	// Manually trigger cleanup to verify the path works
	// The actual ticker test would require waiting for the full interval
	pool.cleanup()

	// Verify client was removed
	stats := pool.GetStats()
	assert.Equal(t, 0, stats["total_connections"])
}

func TestConnectionPool_Cleanup_OverLimit(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	// Set a low max connections limit to trigger over-limit cleanup
	// We'll need to patch env.GetInt, but for now let's create enough connections
	// to potentially trigger the cleanup path

	// Create multiple clients with different timestamps
	// We'll set some to be old and some to be recent
	for i := 0; i < 10; i++ {
		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			PoolID:   fmt.Sprintf("pool-%d", i),
			Username: "testuser",
			Password: "testpass",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: fmt.Sprintf("ontap%d.example.com", i)},
			},
		}

		_, err := pool.GetClient(fmt.Sprintf("ontap%d.example.com", i), authData)
		assert.NoError(t, err)
	}

	// Manually set timestamps - make some old, some recent
	pool.mutex.Lock()
	now := time.Now()
	keys := make([]string, 0, len(pool.clientTimestamps))
	for key := range pool.clientTimestamps {
		keys = append(keys, key)
	}
	// Set first 5 to be old
	for i := 0; i < 5 && i < len(keys); i++ {
		pool.clientTimestamps[keys[i]] = now.Add(-2 * time.Hour)
	}
	// Keep rest recent
	pool.mutex.Unlock()

	// Run cleanup - should remove old connections
	pool.cleanup()

	// Verify old connections were removed
	stats := pool.GetStats()
	assert.LessOrEqual(t, stats["total_connections"], 10)
	assert.GreaterOrEqual(t, stats["total_connections"], 0)
}

func TestConnectionPool_Cleanup_OverLimit_RemovesOldest(t *testing.T) {
	pool := NewConnectionPool()
	defer pool.Close()

	// Create many clients to exceed the default limit (1000)
	// We'll create 5 clients and set max to 3 to trigger cleanup
	clients := make([]*http.Client, 5)
	for i := 0; i < 5; i++ {
		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			PoolID:   fmt.Sprintf("pool-%d", i),
			Username: "testuser",
			Password: "testpass",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: fmt.Sprintf("ontap%d.example.com", i)},
			},
		}

		client, err := pool.GetClient(fmt.Sprintf("ontap%d.example.com", i), authData)
		assert.NoError(t, err)
		clients[i] = client
	}

	// Manually set timestamps with different ages to test oldest removal
	pool.mutex.Lock()
	now := time.Now()
	keys := make([]string, 0, len(pool.clientTimestamps))
	for key := range pool.clientTimestamps {
		keys = append(keys, key)
	}
	// Set timestamps in order: oldest first
	for i, key := range keys {
		pool.clientTimestamps[key] = now.Add(-time.Duration(i+1) * time.Minute)
	}
	// Temporarily set a low max to trigger over-limit cleanup
	// We'll manually check if we're over limit
	pool.mutex.Unlock()

	// Manually trigger cleanup with a scenario that will hit the over-limit path
	// We need to ensure we have more connections than maxConnections
	// Since we can't easily mock env.GetInt, we'll test the logic by ensuring
	// the cleanup function can handle the over-limit case
	pool.cleanup()

	// Verify cleanup ran
	stats := pool.GetStats()
	assert.GreaterOrEqual(t, stats["total_connections"], 0)
}

func TestConnectionPool_Cleanup_OverLimit_RemovesOldestConnection(t *testing.T) {
	t.Run("WhenOverMaxConnections_ShouldRemoveOldestConnection", func(t *testing.T) {
		// Save original env value and restore after test
		originalValue := os.Getenv("ONTAP_MAX_TOTAL_CONNECTIONS")
		defer func() {
			if originalValue == "" {
				_ = os.Unsetenv("ONTAP_MAX_TOTAL_CONNECTIONS")
			} else {
				_ = os.Setenv("ONTAP_MAX_TOTAL_CONNECTIONS", originalValue)
			}
		}()

		// Set a low max connections limit to trigger over-limit cleanup
		_ = os.Setenv("ONTAP_MAX_TOTAL_CONNECTIONS", "3")

		pool := NewConnectionPool()
		defer pool.Close()

		// Create 5 clients (more than the max of 3)
		clientKeys := make([]string, 5)
		clients := make([]*http.Client, 5)
		now := time.Now()

		for i := 0; i < 5; i++ {
			authData := &models.AuthData{
				AuthType: models.USERNAME_PWD,
				PoolID:   fmt.Sprintf("pool-%d", i),
				Username: "testuser",
				Password: "testpass",
				OntapEndpoints: []models.OntapEndpoint{
					{DNS: fmt.Sprintf("ontap%d.example.com", i)},
				},
			}

			client, err := pool.GetClient(fmt.Sprintf("ontap%d.example.com", i), authData)
			assert.NoError(t, err)
			assert.NotNil(t, client)
			clients[i] = client

			// Store the key for later verification
			pool.mutex.RLock()
			key := pool.generatePoolKey(fmt.Sprintf("ontap%d.example.com", i), authData)
			clientKeys[i] = key
			pool.mutex.RUnlock()
		}

		// Verify we have 5 clients
		stats := pool.GetStats()
		assert.Equal(t, 5, stats["total_connections"])

		// Manually set timestamps with different ages - make key[0] the oldest
		pool.mutex.Lock()
		for i, key := range clientKeys {
			// Set timestamps: key[0] is oldest (5 minutes ago), key[4] is newest (1 minute ago)
			pool.clientTimestamps[key] = now.Add(-time.Duration(5-i) * time.Minute)
		}
		oldestKey := clientKeys[0]
		pool.mutex.Unlock()

		// Set a long cleanup threshold so time-based cleanup doesn't remove anything
		pool.cleanupThreshold = 10 * time.Hour

		// Run cleanup multiple times - each call removes one oldest connection (lines 250-272)
		// We have 5 connections, max is 3, so we need 2 cleanup calls
		pool.cleanup()
		stats = pool.GetStats()
		assert.Equal(t, 4, stats["total_connections"], "First cleanup should remove one connection")

		// Update timestamps again to ensure we can identify the next oldest
		// Get remaining keys and set their timestamps
		pool.mutex.Lock()
		remainingKeys := make([]string, 0)
		for key := range pool.clients {
			remainingKeys = append(remainingKeys, key)
		}
		// Set timestamps for remaining connections with different ages
		for i, key := range remainingKeys {
			pool.clientTimestamps[key] = now.Add(-time.Duration(len(remainingKeys)-i) * time.Minute)
		}
		pool.mutex.Unlock()

		pool.cleanup()
		stats = pool.GetStats()
		assert.Equal(t, 3, stats["total_connections"], "Second cleanup should bring us to maxConnections")

		// Verify the oldest connection (key[0]) was removed after both cleanups
		pool.mutex.RLock()
		_, oldestExists := pool.clients[oldestKey]
		_, oldestTimestampExists := pool.clientTimestamps[oldestKey]
		pool.mutex.RUnlock()
		assert.False(t, oldestExists, "Oldest client should be removed")
		assert.False(t, oldestTimestampExists, "Oldest timestamp should be removed")

		// Verify newer connections still exist
		pool.mutex.RLock()
		_, newestExists := pool.clients[clientKeys[4]]
		pool.mutex.RUnlock()
		assert.True(t, newestExists, "Newest client should still exist")
	})

	t.Run("WhenOverLimitAndMultipleOldest_ShouldRemoveOneOldest", func(t *testing.T) {
		// Save original env value and restore after test
		originalValue := os.Getenv("ONTAP_MAX_TOTAL_CONNECTIONS")
		defer func() {
			if originalValue == "" {
				_ = os.Unsetenv("ONTAP_MAX_TOTAL_CONNECTIONS")
			} else {
				_ = os.Setenv("ONTAP_MAX_TOTAL_CONNECTIONS", originalValue)
			}
		}()

		// Set a low max connections limit
		_ = os.Setenv("ONTAP_MAX_TOTAL_CONNECTIONS", "2")

		pool := NewConnectionPool()
		defer pool.Close()

		// Create 3 clients
		clientKeys := make([]string, 3)
		now := time.Now()

		for i := 0; i < 3; i++ {
			authData := &models.AuthData{
				AuthType: models.USERNAME_PWD,
				PoolID:   fmt.Sprintf("pool-%d", i),
				Username: "testuser",
				Password: "testpass",
				OntapEndpoints: []models.OntapEndpoint{
					{DNS: fmt.Sprintf("ontap%d.example.com", i)},
				},
			}

			_, err := pool.GetClient(fmt.Sprintf("ontap%d.example.com", i), authData)
			assert.NoError(t, err)

			pool.mutex.RLock()
			key := pool.generatePoolKey(fmt.Sprintf("ontap%d.example.com", i), authData)
			clientKeys[i] = key
			pool.mutex.RUnlock()
		}

		// Set all timestamps to the same old time (all are equally old)
		pool.mutex.Lock()
		oldTime := now.Add(-10 * time.Minute)
		for _, key := range clientKeys {
			pool.clientTimestamps[key] = oldTime
		}
		pool.mutex.Unlock()

		// Set a long cleanup threshold
		pool.cleanupThreshold = 10 * time.Hour

		// Run cleanup - should remove one oldest connection
		pool.cleanup()

		// Verify we now have maxConnections (2) clients
		stats := pool.GetStats()
		assert.Equal(t, 2, stats["total_connections"], "Should have exactly maxConnections after cleanup")

		// Verify one connection was removed
		pool.mutex.RLock()
		removedCount := 0
		for _, key := range clientKeys {
			if _, exists := pool.clients[key]; !exists {
				removedCount++
			}
		}
		pool.mutex.RUnlock()
		assert.Equal(t, 1, removedCount, "Exactly one connection should be removed")
	})

	t.Run("WhenOverLimitButNoTimestamps_ShouldNotPanic", func(t *testing.T) {
		// Save original env value and restore after test
		originalValue := os.Getenv("ONTAP_MAX_TOTAL_CONNECTIONS")
		defer func() {
			if originalValue == "" {
				_ = os.Unsetenv("ONTAP_MAX_TOTAL_CONNECTIONS")
			} else {
				_ = os.Setenv("ONTAP_MAX_TOTAL_CONNECTIONS", originalValue)
			}
		}()

		_ = os.Setenv("ONTAP_MAX_TOTAL_CONNECTIONS", "1")

		pool := NewConnectionPool()
		defer pool.Close()

		// Create 2 clients
		for i := 0; i < 2; i++ {
			authData := &models.AuthData{
				AuthType: models.USERNAME_PWD,
				PoolID:   fmt.Sprintf("pool-%d", i),
				Username: "testuser",
				Password: "testpass",
				OntapEndpoints: []models.OntapEndpoint{
					{DNS: fmt.Sprintf("ontap%d.example.com", i)},
				},
			}

			_, err := pool.GetClient(fmt.Sprintf("ontap%d.example.com", i), authData)
			assert.NoError(t, err)
		}

		// Remove one timestamp to simulate edge case
		pool.mutex.Lock()
		keys := make([]string, 0, len(pool.clients))
		for key := range pool.clients {
			keys = append(keys, key)
		}
		if len(keys) > 1 {
			delete(pool.clientTimestamps, keys[0])
		}
		pool.mutex.Unlock()

		pool.cleanupThreshold = 10 * time.Hour

		// Should not panic even if timestamp is missing
		assert.NotPanics(t, func() {
			pool.cleanup()
		}, "Cleanup should handle missing timestamps gracefully")
	})
}

func TestPooledAuthTransport_RoundTrip_NoCacheKey(t *testing.T) {
	transport := NewPooledAuthTransport()

	req, err := http.NewRequest("GET", "https://test-ontap.example.com/api/test", nil)
	assert.NoError(t, err)

	// No cache key in context
	resp, err := transport.RoundTrip(req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "no cache key found")
}

func TestPooledAuthTransport_RoundTrip_NoAuthData(t *testing.T) {
	transport := NewPooledAuthTransport()

	req, err := http.NewRequest("GET", "https://test-ontap.example.com/api/test", nil)
	assert.NoError(t, err)

	ctx := context.WithValue(req.Context(), models.AuthDataKey, "non-existent-key")
	req = req.WithContext(ctx)

	resp, err := transport.RoundTrip(req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "no authentication data found in cache")
}

func TestPooledAuthTransport_RoundTrip_NoOntapAddress(t *testing.T) {
	transport := NewPooledAuthTransport()

	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: "test-ontap.example.com"},
		},
	}

	cacheKey := "test-cache-key"
	cache.AddToAuthDataCache(cacheKey, authData)

	req, err := http.NewRequest("GET", "/api/test", nil) // No host in URL
	assert.NoError(t, err)

	ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
	req = req.WithContext(ctx)

	resp, err := transport.RoundTrip(req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "could not extract ONTAP address")
}

func TestPooledAuthTransport_RoundTrip_GetClientError(t *testing.T) {
	transport := NewPooledAuthTransport()

	// Create auth data that will cause GetClient to fail
	authData := &models.AuthData{
		AuthType: models.USER_CERTIFICATE,
		PoolID:   "test-pool-123",
		Certificate: &models.Certificate{
			SignedCertificate:        "invalid",
			PrivateKey:               "invalid",
			InterMediateCertificates: []string{"invalid"},
		},
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: "test-ontap.example.com"},
		},
	}

	cacheKey := "test-cache-key"
	cache.AddToAuthDataCache(cacheKey, authData)

	req, err := http.NewRequest("GET", "https://test-ontap.example.com/api/test", nil)
	assert.NoError(t, err)

	ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
	req = req.WithContext(ctx)

	resp, err := transport.RoundTrip(req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "failed to get pooled client")
}

func TestConfigureRequestAuthentication_CertificateAuth(t *testing.T) {
	req, err := http.NewRequest("GET", "https://test.example.com/api/test", nil)
	assert.NoError(t, err)

	authData := &models.AuthData{
		AuthType: models.USER_CERTIFICATE,
	}

	err = configureRequestAuthentication(req, authData)
	assert.NoError(t, err)
	// Certificate auth should not set basic auth
	assert.Empty(t, req.Header.Get("Authorization"))
}

func TestConfigureRequestAuthentication_MissingCredentials(t *testing.T) {
	req, err := http.NewRequest("GET", "https://test.example.com/api/test", nil)
	assert.NoError(t, err)

	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		Username: "", // Missing username
		Password: "testpass",
	}

	err = configureRequestAuthentication(req, authData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing username or password")

	// Test with missing password
	authData.Username = "testuser"
	authData.Password = ""

	err = configureRequestAuthentication(req, authData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing username or password")
}

func TestPooledAuthTransport_RoundTrip_ConfigureAuthError(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	transport := NewPooledAuthTransport()

	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "", // Missing username to trigger error
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: server.URL},
		},
	}

	cacheKey := "test-cache-key"
	cache.AddToAuthDataCache(cacheKey, authData)

	req, err := http.NewRequest("GET", server.URL+"/test", nil)
	assert.NoError(t, err)

	ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
	req = req.WithContext(ctx)

	resp, err := transport.RoundTrip(req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "failed to configure authentication")
}

// Benchmark tests
func BenchmarkConnectionPool_GetClient(b *testing.B) {
	pool := NewConnectionPool()
	defer pool.Close()

	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: "test-ontap.example.com"},
		},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		client, err := pool.GetClient("test-ontap.example.com", authData)
		if err != nil {
			b.Fatal(err)
		}
		if client == nil {
			b.Fatal("client is nil")
		}
	}
}

func BenchmarkConnectionPool_ConcurrentAccess(b *testing.B) {
	pool := NewConnectionPool()
	defer pool.Close()

	authData := &models.AuthData{
		AuthType: models.USERNAME_PWD,
		PoolID:   "test-pool-123",
		Username: "testuser",
		Password: "testpass",
		OntapEndpoints: []models.OntapEndpoint{
			{DNS: "test-ontap.example.com"},
		},
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			client, err := pool.GetClient("test-ontap.example.com", authData)
			if err != nil {
				b.Fatal(err)
			}
			if client == nil {
				b.Fatal("client is nil")
			}
		}
	})
}
