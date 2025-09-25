package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
)

func TestRuleEngineMiddleware(t *testing.T) {
	t.Run("WhenValidOntapAPIRequest_ShouldAllow", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})

	t.Run("WhenInvalidPath_ShouldDeny", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/invalid/path", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.False(t, nextCalled, "Next handler should not be called")
		assert.Equal(t, http.StatusBadRequest, rr.Code, "Should return 400 Bad Request")
		assert.Contains(t, rr.Body.String(), "Invalid path", "Should contain error message")
	})

	t.Run("WhenPathWithoutOntapAPI_ShouldDeny", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.False(t, nextCalled, "Next handler should not be called")
		assert.Equal(t, http.StatusBadRequest, rr.Code, "Should return 400 Bad Request")
	})

	t.Run("WhenNonConfiguredEndpoint_ShouldDeny", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/non-existent", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.False(t, nextCalled, "Next handler should not be called")
		assert.Equal(t, http.StatusNotFound, rr.Code, "Should return 404 Not Found")
		assert.Contains(t, rr.Body.String(), "No rule configured", "Should contain error message")
	})

	t.Run("WhenPOSTRequestToQtreeEndpoint_ShouldAllow", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("POST", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})

	t.Run("WhenDifferentHTTPMethods_ShouldHandleAll", func(t *testing.T) {
		methods := []string{"GET", "POST", "PATCH", "DELETE"}

		for _, method := range methods {
			t.Run(method, func(t *testing.T) {
				nextCalled := false
				nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					nextCalled = true
					w.WriteHeader(http.StatusOK)
				})

				middleware := RuleEngineMiddleware()
				handler := middleware(nextHandler)

				req, err := http.NewRequest(method, "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/qtrees", nil)
				assert.NoError(t, err, "Failed to create request for method %s", method)

				rr := httptest.NewRecorder()

				handler.ServeHTTP(rr, req)

				assert.True(t, nextCalled, "Next handler should be called for %s", method)
				assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK for %s", method)
			})
		}
	})

	t.Run("WhenUnsupportedHTTPMethods_ShouldReject", func(t *testing.T) {
		unsupportedMethods := []string{"PUT", "HEAD", "OPTIONS", "TRACE"}

		for _, method := range unsupportedMethods {
			t.Run(method, func(t *testing.T) {
				nextCalled := false
				nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					nextCalled = true
					w.WriteHeader(http.StatusOK)
				})

				middleware := RuleEngineMiddleware()
				handler := middleware(nextHandler)

				req, err := http.NewRequest(method, "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/qtrees", nil)
				assert.NoError(t, err, "Failed to create request for method %s", method)

				rr := httptest.NewRecorder()

				handler.ServeHTTP(rr, req)

				assert.False(t, nextCalled, "Next handler should not be called for unsupported method %s", method)
				assert.Equal(t, http.StatusMethodNotAllowed, rr.Code, "Should return 405 Method Not Allowed for %s", method)
			})
		}
	})

	t.Run("WhenVolumesEndpoint_ShouldHandleWithFieldRemoval", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})

	t.Run("WhenAggregatesEndpoint_ShouldHandle", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/aggregates", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})

	t.Run("WhenSpecificVolumeEndpoint_ShouldHandle", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes/{uuid}", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})

	t.Run("WhenActionIsStoredInContext_ShouldBeAccessible", func(t *testing.T) {
		actionFound := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if ctx := r.Context().Value("ruleContext"); ctx != nil {
				if _, ok := ctx.(actions.RequestProcessor); ok {
					actionFound = true
				}
			}
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, actionFound, "Action should be stored in context")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})

	t.Run("WhenActionShouldNotAllow_ShouldDenyRequest", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("POST", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.False(t, nextCalled, "Next handler should not be called")
		assert.Equal(t, http.StatusForbidden, rr.Code, "Should return 403 Forbidden")
	})

	t.Run("WhenActionProcessRequestReturnsError_ShouldHandleGracefully", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("PATCH", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.False(t, nextCalled, "Next handler should not be called")
		assert.Equal(t, http.StatusForbidden, rr.Code, "Should return 403 Forbidden")
	})

	t.Run("WhenActionProcessRequestSucceeds_ShouldCallNextHandler", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})

	t.Run("WhenActionProcessRequestFails_ShouldReturnInternalServerError", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("DELETE", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.False(t, nextCalled, "Next handler should not be called")
		assert.Equal(t, http.StatusForbidden, rr.Code, "Should return 403 Forbidden")
	})

	t.Run("WhenNoRuleFound_ShouldReturn404NotFound", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/nonexistent", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.False(t, nextCalled, "Next handler should not be called")
		assert.Equal(t, http.StatusNotFound, rr.Code, "Should return 404 Not Found")
	})

	t.Run("WhenMethodNotAllowed_ShouldReturnMethodNotAllowed", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("PUT", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.False(t, nextCalled, "Next handler should not be called")
		assert.Equal(t, http.StatusMethodNotAllowed, rr.Code, "Should return 405 Method Not Allowed")
		assert.Contains(t, rr.Body.String(), "Method not allowed", "Should contain error message")
	})

	t.Run("WhenActionProcessRequestSucceeds_ShouldReturn200OK", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})
}

func TestExtractOntapPath(t *testing.T) {
	t.Run("WhenValidOntapAPIPath_ShouldExtractCorrectly", func(t *testing.T) {
		fullPath := "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/qtrees"
		expected := "/api/storage/qtrees"

		result := extractOntapPath(fullPath)
		assert.Equal(t, expected, result, "Should extract ONTAP API path correctly")
	})

	t.Run("WhenPathWithQueryParameters_ShouldExtractWithQuery", func(t *testing.T) {
		fullPath := "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes?fields=name,size"
		expected := "/api/storage/volumes?fields=name,size"

		result := extractOntapPath(fullPath)
		assert.Equal(t, expected, result, "Should extract ONTAP API path with query parameters")
	})

	t.Run("WhenPathWithoutOntapAPI_ShouldReturnEmpty", func(t *testing.T) {
		fullPath := "/v1beta/projects/1234/locations/us-central1/pools/my-pool/api/storage/qtrees"

		result := extractOntapPath(fullPath)
		assert.Equal(t, "", result, "Should return empty string for path without ontap-api")
	})

	t.Run("WhenRootPath_ShouldReturnEmpty", func(t *testing.T) {
		fullPath := "/"

		result := extractOntapPath(fullPath)
		assert.Equal(t, "", result, "Should return empty string for root path")
	})

	t.Run("WhenEmptyPath_ShouldReturnEmpty", func(t *testing.T) {
		fullPath := ""

		result := extractOntapPath(fullPath)
		assert.Equal(t, "", result, "Should return empty string for empty path")
	})

	t.Run("WhenOntapAPIAtRootLevel_ShouldHandleCorrectly", func(t *testing.T) {
		fullPath := "/ontap-api/api/storage/qtrees"
		expected := "/api/storage/qtrees"

		result := extractOntapPath(fullPath)
		assert.Equal(t, expected, result, "Should handle ONTAP API at root level")
	})

	t.Run("WhenComplexPathStructure_ShouldHandleCorrectly", func(t *testing.T) {
		fullPath := "/v1beta/projects/my-project/locations/us-west1/pools/my-pool/ontap-api/api/storage/volumes/12345/snapshots"
		expected := "/api/storage/volumes/12345/snapshots"

		result := extractOntapPath(fullPath)
		assert.Equal(t, expected, result, "Should handle complex path structure")
	})

	t.Run("WhenSingleOntapAPIOccurrence_ShouldExtractCorrectly", func(t *testing.T) {
		fullPath := "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/qtrees"
		expected := "/api/storage/qtrees"

		result := extractOntapPath(fullPath)
		assert.Equal(t, expected, result, "Should extract ONTAP API path from single ontap-api occurrence")
	})

	t.Run("WhenPathEndingWithOntapAPI_ShouldHandleCorrectly", func(t *testing.T) {
		fullPath := "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api"
		expected := "/"

		result := extractOntapPath(fullPath)
		assert.Equal(t, expected, result, "Should handle path ending with ontap-api")
	})
}
