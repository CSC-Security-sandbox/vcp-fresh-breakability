package utils

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ontapserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
)

func TestExtractOntapPath(t *testing.T) {
	tests := []struct {
		name     string
		fullPath string
		expected string
	}{
		{
			name:     "Valid ONTAP API path with full project path",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/qtrees",
			expected: "/api/storage/qtrees",
		},
		{
			name:     "ONTAP API path with query parameters",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/volumes?fields=name,size",
			expected: "/api/storage/volumes?fields=name,size",
		},
		{
			name:     "Path without ontap segment",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/invalid-path",
			expected: "",
		},
		{
			name:     "Empty path",
			fullPath: "",
			expected: "",
		},
		{
			name:     "ONTAP API at root level",
			fullPath: "/ontap/api/storage/qtrees",
			expected: "/api/storage/qtrees",
		},
		{
			name:     "ONTAP API at end of path",
			fullPath: "/v1beta/projects/123/locations/us-central1/pools/pool1/ontap",
			expected: "/",
		},
		{
			name:     "Multiple ontap segments - should use first occurrence",
			fullPath: "/v1beta/projects/123/ontap/api1/ontap/api2",
			expected: "/api1/ontap/api2",
		},
		{
			name:     "Path starting with ontap (no leading slash)",
			fullPath: "ontap/api/storage/qtrees",
			expected: "/api/storage/qtrees",
		},
		{
			name:     "ONTAP API path with UUID",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000",
			expected: "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "ONTAP API path with nested paths",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/storage/volumes/12345/snapshots",
			expected: "/api/storage/volumes/12345/snapshots",
		},
		{
			name:     "Path with only ontap segment",
			fullPath: "/ontap",
			expected: "/",
		},
		{
			name:     "Path with ontap and trailing slash",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/",
			expected: "/",
		},
		{
			name:     "Path with private API",
			fullPath: "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap/api/private/cli/snapmirror/break",
			expected: "/api/private/cli/snapmirror/break",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractOntapPath(tt.fullPath)
			assert.Equal(t, tt.expected, result, "ExtractOntapPath(%q) = %q, want %q", tt.fullPath, result, tt.expected)
		})
	}
}

func TestWriteErrorResponse(t *testing.T) {
	tests := []struct {
		name           string
		code           int
		message        string
		expectedCode   int
		expectedBody   ontapserver.Error
		expectedHeader string
	}{
		{
			name:           "Bad Request (400)",
			code:           http.StatusBadRequest,
			message:        "Invalid request parameters",
			expectedCode:   http.StatusBadRequest,
			expectedBody:   ontapserver.Error{Code: http.StatusBadRequest, Message: "Invalid request parameters"},
			expectedHeader: "application/json; charset=utf-8",
		},
		{
			name:           "Unauthorized (401)",
			code:           http.StatusUnauthorized,
			message:        "Unauthorized access",
			expectedCode:   http.StatusUnauthorized,
			expectedBody:   ontapserver.Error{Code: http.StatusUnauthorized, Message: "Unauthorized access"},
			expectedHeader: "application/json; charset=utf-8",
		},
		{
			name:           "Forbidden (403)",
			code:           http.StatusForbidden,
			message:        "Forbidden access",
			expectedCode:   http.StatusForbidden,
			expectedBody:   ontapserver.Error{Code: http.StatusForbidden, Message: "Forbidden access"},
			expectedHeader: "application/json; charset=utf-8",
		},
		{
			name:           "Not Found (404)",
			code:           http.StatusNotFound,
			message:        "Pool not found",
			expectedCode:   http.StatusNotFound,
			expectedBody:   ontapserver.Error{Code: http.StatusNotFound, Message: "Pool not found"},
			expectedHeader: "application/json; charset=utf-8",
		},
		{
			name:           "Method Not Allowed (405)",
			code:           http.StatusMethodNotAllowed,
			message:        "Method not allowed",
			expectedCode:   http.StatusMethodNotAllowed,
			expectedBody:   ontapserver.Error{Code: http.StatusMethodNotAllowed, Message: "Method not allowed"},
			expectedHeader: "application/json; charset=utf-8",
		},
		{
			name:           "Internal Server Error (500)",
			code:           http.StatusInternalServerError,
			message:        "Internal server error",
			expectedCode:   http.StatusInternalServerError,
			expectedBody:   ontapserver.Error{Code: http.StatusInternalServerError, Message: "Internal server error"},
			expectedHeader: "application/json; charset=utf-8",
		},
		{
			name:           "Bad Gateway (502)",
			code:           http.StatusBadGateway,
			message:        "Cannot connect to ONTAP cluster",
			expectedCode:   http.StatusBadGateway,
			expectedBody:   ontapserver.Error{Code: http.StatusBadGateway, Message: "Cannot connect to ONTAP cluster"},
			expectedHeader: "application/json; charset=utf-8",
		},
		{
			name:           "Service Unavailable (503)",
			code:           http.StatusServiceUnavailable,
			message:        "Service unavailable",
			expectedCode:   http.StatusServiceUnavailable,
			expectedBody:   ontapserver.Error{Code: http.StatusServiceUnavailable, Message: "Service unavailable"},
			expectedHeader: "application/json; charset=utf-8",
		},
		{
			name:           "Gateway Timeout (504)",
			code:           http.StatusGatewayTimeout,
			message:        "Request timeout - ONTAP cluster not responding",
			expectedCode:   http.StatusGatewayTimeout,
			expectedBody:   ontapserver.Error{Code: http.StatusGatewayTimeout, Message: "Request timeout - ONTAP cluster not responding"},
			expectedHeader: "application/json; charset=utf-8",
		},
		{
			name:           "Empty message",
			code:           http.StatusBadRequest,
			message:        "",
			expectedCode:   http.StatusBadRequest,
			expectedBody:   ontapserver.Error{Code: http.StatusBadRequest, Message: ""},
			expectedHeader: "application/json; charset=utf-8",
		},
		{
			name:           "Long error message",
			code:           http.StatusBadRequest,
			message:        "This is a very long error message that contains multiple words and should be properly encoded in the JSON response without any issues",
			expectedCode:   http.StatusBadRequest,
			expectedBody:   ontapserver.Error{Code: http.StatusBadRequest, Message: "This is a very long error message that contains multiple words and should be properly encoded in the JSON response without any issues"},
			expectedHeader: "application/json; charset=utf-8",
		},
		{
			name:           "Error message with special characters",
			code:           http.StatusBadRequest,
			message:        "Error: Invalid input \"test\" with 'quotes' and <tags>",
			expectedCode:   http.StatusBadRequest,
			expectedBody:   ontapserver.Error{Code: http.StatusBadRequest, Message: "Error: Invalid input \"test\" with 'quotes' and <tags>"},
			expectedHeader: "application/json; charset=utf-8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new recorder for each test
			w := httptest.NewRecorder()

			// Call WriteErrorResponse
			WriteErrorResponse(w, tt.code, tt.message)

			// Verify HTTP status code
			assert.Equal(t, tt.expectedCode, w.Code, "HTTP status code should match expected value")

			// Verify Content-Type header
			contentType := w.Header().Get("Content-Type")
			assert.Equal(t, tt.expectedHeader, contentType, "Content-Type header should be set to application/json")

			// Verify response body is valid JSON
			var errorResponse ontapserver.Error
			err := json.Unmarshal(w.Body.Bytes(), &errorResponse)
			require.NoError(t, err, "Response body should be valid JSON")

			// Verify error code in JSON body matches HTTP status code
			assert.Equal(t, tt.expectedBody.Code, errorResponse.Code, "Error code in JSON body should match HTTP status code")
			assert.Equal(t, tt.expectedBody.Message, errorResponse.Message, "Error message in JSON body should match expected message")

			// Verify that the code in the JSON body matches the HTTP status code
			assert.Equal(t, w.Code, errorResponse.Code, "JSON body code should match HTTP status code")
		})
	}
}

func TestWriteErrorResponse_CodeAndStatusAlignment(t *testing.T) {
	// This test specifically verifies that the code in the JSON body
	// always matches the HTTP status code, which is a key requirement
	t.Run("Code in JSON body matches HTTP status code", func(t *testing.T) {
		statusCodes := []int{
			http.StatusBadRequest,
			http.StatusUnauthorized,
			http.StatusForbidden,
			http.StatusNotFound,
			http.StatusMethodNotAllowed,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout,
		}

		for _, statusCode := range statusCodes {
			t.Run(http.StatusText(statusCode), func(t *testing.T) {
				w := httptest.NewRecorder()
				message := "Test error message"

				WriteErrorResponse(w, statusCode, message)

				// Verify HTTP status code
				assert.Equal(t, statusCode, w.Code, "HTTP status code should match input")

				// Parse JSON response
				var errorResponse ontapserver.Error
				err := json.Unmarshal(w.Body.Bytes(), &errorResponse)
				require.NoError(t, err, "Response should be valid JSON")

				// Verify code in JSON body matches HTTP status code
				assert.Equal(t, statusCode, errorResponse.Code, "Code in JSON body should match HTTP status code")
				assert.Equal(t, message, errorResponse.Message, "Message should match input")
			})
		}
	})
}

func TestWriteErrorResponse_JSONFormat(t *testing.T) {
	t.Run("Response body is properly formatted JSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		code := http.StatusBadRequest
		message := "Test error message"

		WriteErrorResponse(w, code, message)

		// Verify it's valid JSON
		var errorResponse ontapserver.Error
		err := json.Unmarshal(w.Body.Bytes(), &errorResponse)
		require.NoError(t, err, "Response should be valid JSON")

		// Verify JSON structure
		assert.Equal(t, code, errorResponse.Code)
		assert.Equal(t, message, errorResponse.Message)

		// Verify JSON can be re-marshaled (round-trip test)
		jsonBytes, err := json.Marshal(errorResponse)
		require.NoError(t, err, "Should be able to marshal back to JSON")

		var roundTripError ontapserver.Error
		err = json.Unmarshal(jsonBytes, &roundTripError)
		require.NoError(t, err, "Should be able to unmarshal round-trip JSON")
		assert.Equal(t, errorResponse, roundTripError, "Round-trip JSON should match original")
	})
}
