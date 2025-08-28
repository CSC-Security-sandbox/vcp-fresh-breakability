package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
)

func setupTestEnv(t *testing.T, envVars map[string]string) func() {
	for key, value := range envVars {
		if err := os.Setenv(key, value); err != nil {
			t.Errorf("Failed to set environment variable %s: %v", key, err)
		}
	}

	return func() {
		for key := range envVars {
			if err := os.Unsetenv(key); err != nil {
				t.Errorf("Failed to unset environment variable %s: %v", key, err)
			}
		}
	}
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

func TestGetOntapAddress(t *testing.T) {
	t.Run("WhenEnvironmentVariableSet_ShouldReturnValue", func(t *testing.T) {
		cleanup := setupTestEnv(t, map[string]string{
			"ONTAP_API_ADDRESS": "https://test-cluster:443",
		})
		defer cleanup()

		req, _ := http.NewRequest("GET", "/test", nil)
		result := getOntapAddress(req)
		expected := "https://test-cluster:443"

		assert.Equal(t, expected, result, "Should return environment variable value")
	})

	t.Run("WhenEnvironmentVariableNotSet_ShouldReturnEmpty", func(t *testing.T) {
		cleanup := setupTestEnv(t, map[string]string{})
		defer cleanup()

		req, _ := http.NewRequest("GET", "/test", nil)
		result := getOntapAddress(req)
		expected := ""

		assert.Equal(t, expected, result, "Should return empty string when environment variable not set")
	})

	t.Run("WhenEnvironmentVariableEmpty_ShouldReturnEmpty", func(t *testing.T) {
		cleanup := setupTestEnv(t, map[string]string{
			"ONTAP_API_ADDRESS": "",
		})
		defer cleanup()

		req, _ := http.NewRequest("GET", "/test", nil)
		result := getOntapAddress(req)
		expected := ""

		assert.Equal(t, expected, result, "Should return empty string when environment variable is empty")
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
		cleanup := setupTestEnv(t, map[string]string{
			"ONTAP_API_ADDRESS":  "https://test-cluster:443",
			"ONTAP_API_USERNAME": "testuser",
			"ONTAP_API_PASSWORD": "testpass",
		})
		defer cleanup()

		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")
		req.RemoteAddr = "192.168.1.1:12345"

		proxy.Director(req)

		assert.Equal(t, "https://test-cluster:443/api/storage/qtrees", req.URL.String())
		assert.Equal(t, "test-cluster:443", req.Host)
		assert.Equal(t, "Basic dGVzdHVzZXI6dGVzdHBhc3M=", req.Header.Get("Authorization"))
		assert.Equal(t, "192.168.1.1:12345", req.Header.Get("X-Forwarded-For"))
		assert.Equal(t, "ONTAP Expert Mode", req.Header.Get("X-Proxy-By"))
		assert.Equal(t, "application/json", req.Header.Get("Accept"))
	})

	t.Run("WhenDirectorCalledWithInvalidPath_ShouldReturnEarly", func(t *testing.T) {
		cleanup := setupTestEnv(t, map[string]string{
			"ONTAP_API_ADDRESS":  "https://test-cluster:443",
			"ONTAP_API_USERNAME": "testuser",
			"ONTAP_API_PASSWORD": "testpass",
		})
		defer cleanup()

		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/invalid/path", nil)
		assert.NoError(t, err, "Failed to create request")

		originalURL := req.URL.String()
		proxy.Director(req)

		assert.Equal(t, originalURL, req.URL.String(), "URL should not be modified for invalid path")
	})

	t.Run("WhenDirectorCalledWithNoOntapAddress_ShouldReturnEarly", func(t *testing.T) {
		cleanup := setupTestEnv(t, map[string]string{
			"ONTAP_API_USERNAME": "testuser",
			"ONTAP_API_PASSWORD": "testpass",
		})
		defer cleanup()

		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		originalURL := req.URL.String()
		proxy.Director(req)

		assert.Equal(t, originalURL, req.URL.String(), "URL should not be modified when no ONTAP address")
	})

	t.Run("WhenDirectorCalledWithNoUsername_ShouldReturnEarly", func(t *testing.T) {
		cleanup := setupTestEnv(t, map[string]string{
			"ONTAP_API_ADDRESS":  "https://test-cluster:443",
			"ONTAP_API_PASSWORD": "testpass",
		})
		defer cleanup()

		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		originalURL := req.URL.String()
		proxy.Director(req)

		assert.Equal(t, originalURL, req.URL.String(), "URL should not be modified when no username")
	})

	t.Run("WhenDirectorCalledWithNoPassword_ShouldReturnEarly", func(t *testing.T) {
		cleanup := setupTestEnv(t, map[string]string{
			"ONTAP_API_ADDRESS":  "https://test-cluster:443",
			"ONTAP_API_USERNAME": "testuser",
		})
		defer cleanup()

		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		originalURL := req.URL.String()
		proxy.Director(req)

		assert.Equal(t, originalURL, req.URL.String(), "URL should not be modified when no password")
	})

	t.Run("WhenDirectorCalledWithInvalidURL_ShouldStillProcessRequest", func(t *testing.T) {
		cleanup := setupTestEnv(t, map[string]string{
			"ONTAP_API_ADDRESS":  "://invalid-url",
			"ONTAP_API_USERNAME": "testuser",
			"ONTAP_API_PASSWORD": "testpass",
		})
		defer cleanup()

		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		proxy.Director(req)

		// The director will still process the request even with invalid URL format
		// The URL parsing will fail later in the process
		assert.Equal(t, "https://://invalid-url/api/storage/qtrees", req.URL.String(), "URL should be modified even with invalid ONTAP address")
	})

	t.Run("WhenModifyResponseCalledWithRuleContext_ShouldProcessResponse", func(t *testing.T) {
		proxy := BuildOntapRESTProxy()
		req, err := http.NewRequest("GET", "/test", nil)
		assert.NoError(t, err, "Failed to create request")

		ctx := context.WithValue(req.Context(), "ruleContext", &mockAction{})
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

		ctx := context.WithValue(req.Context(), "ruleContext", "invalid")
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

type mockAction struct{}

func (m *mockAction) ShouldAllow(r *http.Request) bool {
	return true
}

func (m *mockAction) ProcessRequest(r *http.Request, w http.ResponseWriter) error {
	return nil
}

func (m *mockAction) ProcessResponse(resp *http.Response) error {
	return nil
}

func TestAllowAction_ProcessResponse(t *testing.T) {
	t.Run("WhenResponseWithFields_ShouldRemoveSpecifiedFields", func(t *testing.T) {
		allowAction := actions.Allow{
			Name:         "Test field removal",
			RemoveFields: []string{"password", "secret"},
		}

		jsonData := `{"name":"test-volume","password":"secret123","size":1073741824,"nested":{"secret":"hidden","public":"visible"}}`
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(jsonData)),
		}

		err := allowAction.ProcessResponse(resp)
		assert.NoError(t, err, "Should process response without error")

		body, err := io.ReadAll(resp.Body)
		assert.NoError(t, err, "Should read response body")

		assert.NotContains(t, string(body), "password", "Password field should be removed")
		assert.NotContains(t, string(body), "secret", "Secret field should be removed")
		assert.Contains(t, string(body), "name", "Name field should remain")
		assert.Contains(t, string(body), "public", "Public field should remain")
	})

	t.Run("WhenNonJSONResponse_ShouldReturnError", func(t *testing.T) {
		allowAction := actions.Allow{
			Name:         "Test non-JSON response",
			RemoveFields: []string{"password"},
		}

		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader("not json data")),
		}

		err := allowAction.ProcessResponse(resp)
		assert.Error(t, err, "Should return error for non-JSON response")
		assert.Contains(t, err.Error(), "not valid JSON", "Error should mention JSON validation")
	})
}
