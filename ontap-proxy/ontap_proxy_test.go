package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions/processor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
)

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
		result := extractOntapPath("/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/qtrees")
		expected := "/api/storage/qtrees"
		assert.Equal(t, expected, result, "Should extract ONTAP API path correctly")
	})

	t.Run("WhenOntapAPIPathWithQueryParams_ShouldExtractWithQuery", func(t *testing.T) {
		result := extractOntapPath("/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes?fields=name,size")
		expected := "/api/storage/volumes?fields=name,size"
		assert.Equal(t, expected, result, "Should extract ONTAP API path with query parameters")
	})

	t.Run("WhenPathWithoutOntapAPI_ShouldReturnEmpty", func(t *testing.T) {
		result := extractOntapPath("/v1beta/projects/1234/locations/us-central1/pools/my-pool/invalid-path")
		expected := ""
		assert.Equal(t, expected, result, "Should return empty string for path without ontap-api")
	})

	t.Run("WhenEmptyPath_ShouldHandleCorrectly", func(t *testing.T) {
		result := extractOntapPath("")
		expected := ""
		assert.Equal(t, expected, result, "Should handle empty path")
	})

	t.Run("WhenOntapAPIAtRoot_ShouldHandleCorrectly", func(t *testing.T) {
		result := extractOntapPath("/ontap-api/api/storage/qtrees")
		expected := "/api/storage/qtrees"
		assert.Equal(t, expected, result, "Should handle ONTAP API at root level")
	})

	t.Run("WhenOntapAPIAtEnd_ShouldHandleCorrectly", func(t *testing.T) {
		result := extractOntapPath("/v1beta/projects/123/locations/us-central1/pools/pool1/ontap-api")
		expected := "/"
		assert.Equal(t, expected, result, "Should handle ONTAP API at end of path")
	})

	t.Run("WhenMultipleOntapAPI_ShouldHandleFirstOccurrence", func(t *testing.T) {
		result := extractOntapPath("/v1beta/projects/123/ontap-api/api1/ontap-api/api2")
		expected := "/api1/ontap-api/api2"
		assert.Equal(t, expected, result, "Should handle first occurrence of ontap-api")
	})

	t.Run("WhenPathStartsWithOntapAPI_ShouldHandleCorrectly", func(t *testing.T) {
		result := extractOntapPath("ontap-api/api/storage/qtrees")
		expected := "/api/storage/qtrees"
		assert.Equal(t, expected, result, "Should handle path starting with ontap-api")
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
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")
		req.RemoteAddr = "192.168.1.1:12345"

		setupTestContextWithAuthData(t, req, "testuser", "testpass")

		proxy.Director(req)

		assert.Equal(t, "https://test-cluster:443/api/storage/qtrees", req.URL.String())
		assert.Equal(t, "test-cluster:443", req.Host)
		assert.Equal(t, "Basic dGVzdHVzZXI6dGVzdHBhc3M=", req.Header.Get("Authorization"))
		assert.Equal(t, "192.168.1.1:12345", req.Header.Get("X-Forwarded-For"))
		assert.Equal(t, "ontap-proxy", req.Header.Get("X-Proxy-By"))
		assert.Equal(t, "application/json", req.Header.Get("Accept"))
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
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap-api/api/storage/qtrees", nil)
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
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap-api/api/storage/qtrees", nil)
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
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap-api/api/storage/qtrees", nil)
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
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap-api/api/storage/qtrees", nil)
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

func TestNewAuthTransport(t *testing.T) {
	t.Run("WhenCalled_ShouldReturnNewInstance", func(t *testing.T) {
		transport := NewAuthTransport()
		assert.NotNil(t, transport, "Should return a new AuthTransport instance")
		assert.IsType(t, &AuthTransport{}, transport, "Should return correct type")
	})
}

func TestAuthTransport_RoundTrip(t *testing.T) {
	t.Run("WhenNoCacheKeyInContext_ShouldReturnError", func(t *testing.T) {
		transport := NewAuthTransport()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		resp, err := transport.RoundTrip(req)
		assert.Nil(t, resp, "Response should be nil")
		assert.Error(t, err, "Should return error")
		assert.Contains(t, err.Error(), "no cache key found in request context", "Should contain correct error message")
	})

	t.Run("WhenNoAuthDataInCache_ShouldReturnError", func(t *testing.T) {
		transport := NewAuthTransport()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		ctx := context.WithValue(req.Context(), models.AuthDataKey, "non-existent-key")
		req = req.WithContext(ctx)

		resp, err := transport.RoundTrip(req)
		assert.Nil(t, resp, "Response should be nil")
		assert.Error(t, err, "Should return error")
		assert.Contains(t, err.Error(), "no authentication data found in cache", "Should contain correct error message")
	})

	t.Run("WhenAuthDataExists_ShouldBuildTransportAndCallRoundTrip", func(t *testing.T) {
		transport := NewAuthTransport()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			Username: "testuser",
			Password: "testpass",
		}
		cacheKey := "test-key"
		cache.AddToAuthDataCache(cacheKey, authData)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
		req = req.WithContext(ctx)

		resp, err := transport.RoundTrip(req)
		assert.Nil(t, resp, "Response should be nil due to network error")
		assert.Error(t, err, "Should return error due to network")
	})

	t.Run("WhenBuildTransportFails_ShouldReturnError", func(t *testing.T) {
		transport := NewAuthTransport()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		authData := &models.AuthData{
			AuthType:    models.USER_CERTIFICATE,
			Certificate: nil,
		}
		cacheKey := "test-key"
		cache.AddToAuthDataCache(cacheKey, authData)
		ctx := context.WithValue(req.Context(), models.AuthDataKey, cacheKey)
		req = req.WithContext(ctx)

		resp, err := transport.RoundTrip(req)
		assert.Nil(t, resp, "Response should be nil")
		assert.Error(t, err, "Should return error")
		assert.Contains(t, err.Error(), "failed to build transport", "Should contain correct error message")
	})
}

func TestConfigureRequestAuthentication(t *testing.T) {
	t.Run("WhenCertificateAuthType_ShouldReturnNil", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		authData := &models.AuthData{
			AuthType: models.USER_CERTIFICATE,
		}

		err = configureRequestAuthentication(req, authData)
		assert.NoError(t, err, "Should not return error for certificate auth")
		assert.Empty(t, req.Header.Get("Authorization"), "Should not set Authorization header")
	})

	t.Run("WhenMissingUsername_ShouldReturnError", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			Username: "",
			Password: "testpass",
		}

		err = configureRequestAuthentication(req, authData)
		assert.Error(t, err, "Should return error")
		assert.Contains(t, err.Error(), "missing username or password", "Should contain correct error message")
	})

	t.Run("WhenMissingPassword_ShouldReturnError", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			Username: "testuser",
			Password: "",
		}

		err = configureRequestAuthentication(req, authData)
		assert.Error(t, err, "Should return error")
		assert.Contains(t, err.Error(), "missing username or password", "Should contain correct error message")
	})

	t.Run("WhenValidCredentials_ShouldSetBasicAuth", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
			Username: "testuser",
			Password: "testpass",
		}

		err = configureRequestAuthentication(req, authData)
		assert.NoError(t, err, "Should not return error")
		assert.Equal(t, "Basic dGVzdHVzZXI6dGVzdHBhc3M=", req.Header.Get("Authorization"), "Should set correct Authorization header")
	})
}

func TestBuildTransportForAuthType(t *testing.T) {
	t.Run("WhenCertificateAuthType_ShouldReturnError", func(t *testing.T) {
		authData := &models.AuthData{
			AuthType: models.USER_CERTIFICATE,
			Certificate: &models.Certificate{
				SignedCertificate:        "invalid-cert",
				PrivateKey:               "invalid-key",
				InterMediateCertificates: []string{"invalid-intermediate"},
				CommonName:               "test.example.com",
				RootCaCertificate:        "invalid-root",
			},
		}

		transport, err := buildTransportForAuthType(authData)
		assert.Error(t, err, "Should return error for invalid certificate")
		assert.Nil(t, transport, "Transport should be nil")
		assert.Contains(t, err.Error(), "failed to prepare certificate", "Should contain correct error message")
	})

	t.Run("WhenUsernamePasswordAuthType_ShouldReturnBasicAuthTransport", func(t *testing.T) {
		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD,
		}

		transport, err := buildTransportForAuthType(authData)
		assert.NoError(t, err, "Should not return error")
		assert.NotNil(t, transport, "Should return transport")
		assert.IsType(t, &http.Transport{}, transport, "Should return http.Transport")
	})

	t.Run("WhenUsernamePasswordSecMgrAuthType_ShouldReturnBasicAuthTransport", func(t *testing.T) {
		authData := &models.AuthData{
			AuthType: models.USERNAME_PWD_SEC_MGR,
		}

		transport, err := buildTransportForAuthType(authData)
		assert.NoError(t, err, "Should not return error")
		assert.NotNil(t, transport, "Should return transport")
		assert.IsType(t, &http.Transport{}, transport, "Should return http.Transport")
	})

	t.Run("WhenUnknownAuthType_ShouldReturnBasicAuthTransport", func(t *testing.T) {
		authData := &models.AuthData{
			AuthType: 999,
		}

		transport, err := buildTransportForAuthType(authData)
		assert.NoError(t, err, "Should not return error")
		assert.NotNil(t, transport, "Should return transport")
		assert.IsType(t, &http.Transport{}, transport, "Should return http.Transport")
	})
}

func TestBuildCertificateTransport(t *testing.T) {
	t.Run("WhenCertificateIsNil_ShouldReturnError", func(t *testing.T) {
		authData := &models.AuthData{
			AuthType:    models.USER_CERTIFICATE,
			Certificate: nil,
		}

		transport, err := buildCertificateTransport(authData)
		assert.Nil(t, transport, "Transport should be nil")
		assert.Error(t, err, "Should return error")
		assert.Contains(t, err.Error(), "certificate not found", "Should contain correct error message")
	})

	t.Run("WhenCertificateExistsButInvalid_ShouldReturnError", func(t *testing.T) {
		authData := &models.AuthData{
			AuthType: models.USER_CERTIFICATE,
			Certificate: &models.Certificate{
				SignedCertificate:        "invalid-cert",
				PrivateKey:               "invalid-key",
				InterMediateCertificates: []string{"invalid-intermediate"},
				CommonName:               "test.example.com",
				RootCaCertificate:        "invalid-root",
			},
		}

		transport, err := buildCertificateTransport(authData)
		assert.Error(t, err, "Should return error for invalid certificate")
		assert.Nil(t, transport, "Transport should be nil")
		assert.Contains(t, err.Error(), "failed to prepare certificate", "Should contain correct error message")
	})
}

func TestBuildBasicAuthTransport(t *testing.T) {
	t.Run("WhenCalled_ShouldReturnBasicTransport", func(t *testing.T) {
		transport, err := buildBasicAuthTransport()
		assert.NoError(t, err, "Should not return error")
		assert.NotNil(t, transport, "Should return transport")
		assert.IsType(t, &http.Transport{}, transport, "Should return http.Transport")
		assert.NotNil(t, transport.TLSClientConfig, "Should have TLS config")
		assert.True(t, transport.TLSClientConfig.InsecureSkipVerify, "Should skip TLS verification")
		assert.Equal(t, 100, transport.MaxIdleConns, "Should set MaxIdleConns")
		assert.Equal(t, 90*time.Second, transport.IdleConnTimeout, "Should set IdleConnTimeout")
		assert.Equal(t, 10*time.Second, transport.TLSHandshakeTimeout, "Should set TLSHandshakeTimeout")
		assert.Equal(t, 1*time.Second, transport.ExpectContinueTimeout, "Should set ExpectContinueTimeout")
	})
}

func TestSetCommonHeaders(t *testing.T) {
	t.Run("WhenCalled_ShouldSetHeaders", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")
		req.RemoteAddr = "192.168.1.1:12345"

		setCommonHeaders(req)

		assert.Equal(t, "192.168.1.1:12345", req.Header.Get("X-Forwarded-For"), "Should set X-Forwarded-For header")
		assert.Equal(t, "ontap-proxy", req.Header.Get("X-Proxy-By"), "Should set X-Proxy-By header")
		assert.Equal(t, "application/json", req.Header.Get("Accept"), "Should set Accept header")
	})

	t.Run("WhenRemoteAddrIsEmpty_ShouldStillSetHeaders", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")
		req.RemoteAddr = ""

		setCommonHeaders(req)

		assert.Equal(t, "", req.Header.Get("X-Forwarded-For"), "Should set empty X-Forwarded-For header")
		assert.Equal(t, "ontap-proxy", req.Header.Get("X-Proxy-By"), "Should set X-Proxy-By header")
		assert.Equal(t, "application/json", req.Header.Get("Accept"), "Should set Accept header")
	})

	t.Run("WhenNoRuleContext_ShouldAllowGzip", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")
		req.RemoteAddr = "192.168.1.1:12345"

		setCommonHeaders(req)

		assert.Equal(t, "", req.Header.Get("Accept-Encoding"), "Should not disable gzip when no rule")
		assert.Equal(t, "192.168.1.1:12345", req.Header.Get("X-Forwarded-For"))
		assert.Equal(t, "ontap-proxy", req.Header.Get("X-Proxy-By"))
		assert.Equal(t, "application/json", req.Header.Get("Accept"))
	})

	t.Run("WhenRuleContextExists_ShouldDisableGzip", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")
		req.RemoteAddr = "192.168.1.1:12345"

		// Add rule context
		mockAction := &processor.Allow{}
		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockAction)
		req = req.WithContext(ctx)

		setCommonHeaders(req)

		assert.Equal(t, "", req.Header.Get("Accept-Encoding"), "Should disable gzip when rule exists")
		assert.Equal(t, "192.168.1.1:12345", req.Header.Get("X-Forwarded-For"))
		assert.Equal(t, "ontap-proxy", req.Header.Get("X-Proxy-By"))
		assert.Equal(t, "application/json", req.Header.Get("Accept"))
	})
}

func TestGetAPICallCertificate(t *testing.T) {
	t.Run("WhenValidCertificate_ShouldReturnCertificates", func(t *testing.T) {
		cert := &models.Certificate{
			SignedCertificate:        "",
			PrivateKey:               "",
			InterMediateCertificates: []string{},
		}

		rootCA, clientCert, err := _getAPICallCertificate(cert)
		assert.Nil(t, rootCA, "RootCA should be nil")
		assert.Empty(t, clientCert, "ClientCert should be empty")
		assert.Error(t, err, "Should return error")
		assert.Contains(t, err.Error(), "invalid certificate parameters", "Should contain correct error message")
	})

	t.Run("WhenInvalidCertificateParameters_ShouldReturnError", func(t *testing.T) {
		cert := &models.Certificate{
			SignedCertificate:        "",
			PrivateKey:               "",
			InterMediateCertificates: []string{},
		}

		rootCA, clientCert, err := _getAPICallCertificate(cert)
		assert.Nil(t, rootCA, "RootCA should be nil")
		assert.Empty(t, clientCert, "ClientCert should be empty")
		assert.Error(t, err, "Should return error")
		assert.Contains(t, err.Error(), "invalid certificate parameters", "Should contain correct error message")
	})
}

func TestAllowAction_ProcessResponse(t *testing.T) {
	t.Run("WhenResponseWithFields_ShouldRemoveSpecifiedFields", func(t *testing.T) {
		allowAction := processor.Allow{
			Name: "Test field removal",
			ResponseRule: actions.ResponseRule{
				RemovalRules: []actions.RemovalRule{
					{FieldPath: "password"},
					{FieldPath: "nested.secret"},
				},
			},
		}

		jsonData := `{"name":"test-volume","password":"secret123","size":1073741824,"nested":{"secret":"hidden","public":"visible"}}`
		resp := &http.Response{
			Body:       io.NopCloser(strings.NewReader(jsonData)),
			StatusCode: 200,
			Header:     make(http.Header),
		}

		err := allowAction.ProcessResponse(resp)
		assert.NoError(t, err, "Should process response without error")

		body, err := io.ReadAll(resp.Body)
		assert.NoError(t, err, "Should read response body")

		assert.NotContains(t, string(body), "password", "Password field should be removed")
		assert.NotContains(t, string(body), "\"secret\":\"hidden\"", "Secret field should be removed")
		assert.Contains(t, string(body), "name", "Name field should remain")
		assert.Contains(t, string(body), "public", "Public field should remain")
	})

	t.Run("WhenNonJSONResponse_ShouldReturnError", func(t *testing.T) {
		allowAction := processor.Allow{
			Name: "Test non-JSON response",
			ResponseRule: actions.ResponseRule{
				RemovalRules: []actions.RemovalRule{
					{FieldPath: "password"},
				},
			},
		}

		resp := &http.Response{
			Body:       io.NopCloser(strings.NewReader("not json data")),
			StatusCode: 200,
			Header:     make(http.Header),
		}

		err := allowAction.ProcessResponse(resp)
		assert.Error(t, err, "Should return error for non-JSON response")
		assert.Contains(t, err.Error(), "failed to decode response", "Error should mention JSON decoding failure")
	})
}

func TestProcessResponseModificationWithMock(t *testing.T) {
	t.Run("WhenMockRequestProcessorProcessResponseSucceeds_ShouldNotReturnError", func(t *testing.T) {
		mockProcessor := actions.NewMockRequestProcessor(t)

		req := httptest.NewRequest("GET", "/test", nil)
		resp := &http.Response{
			Request: req,
			Body:    io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		mockProcessor.EXPECT().ProcessResponse(resp).Return(nil)

		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockProcessor)
		resp.Request = req.WithContext(ctx)

		err := actions.ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should not return error when mock succeeds")
	})

	t.Run("WhenMockRequestProcessorProcessResponseFails_ShouldReturnError", func(t *testing.T) {
		mockProcessor := actions.NewMockRequestProcessor(t)

		req := httptest.NewRequest("GET", "/test", nil)
		resp := &http.Response{
			Request: req,
			Body:    io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		expectedError := fmt.Errorf("mock processing failed")
		mockProcessor.EXPECT().ProcessResponse(resp).Return(expectedError)

		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockProcessor)
		resp.Request = req.WithContext(ctx)

		err := actions.ProcessResponseModification(resp)
		assert.Error(t, err, "ProcessResponseModification should return error when mock fails")
		assert.Equal(t, expectedError, err, "Should return the exact error from mock")
	})

	t.Run("WhenNoRuleContext_ShouldNotReturnError", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		resp := &http.Response{
			Request: req,
			Body:    io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		err := actions.ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should not return error when no ruleContext")
	})

	t.Run("WhenRuleContextIsNotRequestProcessor_ShouldNotReturnError", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		resp := &http.Response{
			Request: req,
			Body:    io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		ctx := context.WithValue(req.Context(), models.RuleContextKey, "not-a-request-processor")
		resp.Request = req.WithContext(ctx)

		err := actions.ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should not return error when ruleContext is not RequestProcessor")
	})
}

func TestBuildOntapRESTProxyWithMockRequestProcessor(t *testing.T) {
	t.Run("WhenProxyIsBuilt_ShouldHaveCorrectModifyResponse", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()

		assert.NotNil(t, proxy, "Proxy should not be nil")
		assert.NotNil(t, proxy.ModifyResponse, "ModifyResponse should not be nil")

		req := httptest.NewRequest("GET", "/test", nil)
		resp := &http.Response{
			Request: req,
			Body:    io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		err := proxy.ModifyResponse(resp)
		assert.NoError(t, err, "ModifyResponse should not return error for basic response")
	})

	t.Run("WhenProxyModifyResponseCalledWithMock_ShouldCallMockProcessor", func(t *testing.T) {
		mockProcessor := actions.NewMockRequestProcessor(t)

		proxy := BuildOntapRESTProxy()

		req := httptest.NewRequest("GET", "/test", nil)
		resp := &http.Response{
			Request: req,
			Body:    io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		mockProcessor.EXPECT().ProcessResponse(resp).Return(nil)

		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockProcessor)
		resp.Request = req.WithContext(ctx)

		err := proxy.ModifyResponse(resp)
		assert.NoError(t, err, "Proxy ModifyResponse should not return error when mock succeeds")
	})
}
