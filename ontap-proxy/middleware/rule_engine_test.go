package middleware

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions/processor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/rules"
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

	t.Run("WhenInvalidPath_ShouldPassToNext", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/invalid/path", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})

	t.Run("WhenPathWithoutOntapAPI_ShouldPassToNext", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/api/storage/qtrees", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})

	t.Run("WhenNonConfiguredEndpoint_ShouldPassToNext", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/non-existent", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
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

	t.Run("WhenUnsupportedHTTPMethods_ShouldPassToNext", func(t *testing.T) {
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

				assert.True(t, nextCalled, "Next handler should be called for %s", method)
				assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK for %s", method)
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

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes", nil)
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

		req, err := http.NewRequest("POST", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes", strings.NewReader(`{"name": "test"}`))
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.False(t, nextCalled, "Next handler should not be called")
		assert.Equal(t, http.StatusBadRequest, rr.Code, "Should return 400 Bad Request")
	})

	t.Run("WhenActionProcessRequestReturnsError_ShouldHandleGracefully", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("PATCH", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes/028baa66-41bd-11e9-81d5-00a0986138f7", strings.NewReader(`{"name": "test"}`))
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.False(t, nextCalled, "Next handler should not be called")
		assert.Equal(t, http.StatusBadRequest, rr.Code, "Should return 400 Bad Request")
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

	t.Run("WhenActionProcessRequestSucceeds_ShouldCallNextHandler", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("DELETE", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes/028baa66-41bd-11e9-81d5-00a0986138f7", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})

	t.Run("WhenNoRuleFound_ShouldPassToNext", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("GET", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/nonexistent", nil)
		assert.NoError(t, err, "Failed to create request")

		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled, "Next handler should be called")
		assert.Equal(t, http.StatusOK, rr.Code, "Should return 200 OK")
	})

	t.Run("WhenMethodNotAllowed_ShouldReturnMethodNotAllowed", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req, err := http.NewRequest("PUT", "/v1beta/projects/1234/locations/us-central1/pools/my-pool/ontap-api/api/storage/volumes", nil)
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

func TestRuleMap_VolumeActions(t *testing.T) {
	proxyRules := rules.GetProxyRules()

	t.Run("VolumeListing_GET_ShouldAllowAndRemoveSensitiveFields", func(t *testing.T) {
		rule := proxyRules["/api/storage/volumes"]
		volumeAction := rule.GET.(*processor.VolumeAction)

		// Test ShouldAllow for GET request
		req, _ := http.NewRequest("GET", "/api/storage/volumes", nil)
		allowed, err := volumeAction.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, allowed)

		// Test ProcessRequest for GET request (should do nothing)
		w := httptest.NewRecorder()
		err = volumeAction.ProcessRequest(req, w)
		assert.NoError(t, err)

		// Test ProcessResponse - should remove sensitive fields
		originalBody := `{
			"records": [
				{
					"name": "vol1",
					"size": 1073741824,
					"efficiency": "sensitive-data-1",
					"space": {
						"logical_space": {
							"size": 1073741824
						},
						"physical_used": "sensitive-physical-data-1"
					}
				},
				{
					"name": "vol2",
					"size": 2147483648,
					"efficiency": "sensitive-data-2",
					"space": {
						"logical_space": {
							"size": 2147483648
						},
						"physical_used": "sensitive-physical-data-2"
					}
				}
			]
		}`
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(originalBody)),
			Header:     make(http.Header),
		}

		err = volumeAction.ProcessResponse(resp)
		assert.NoError(t, err)

		// Verify sensitive fields are removed
		body, _ := io.ReadAll(resp.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		records, ok := result["records"].([]interface{})
		assert.True(t, ok)
		assert.Len(t, records, 2)

		// Check first record
		record1, ok := records[0].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "vol1", record1["name"])
		assert.Equal(t, float64(1073741824), record1["size"])
		assert.NotContains(t, record1, "efficiency")

		space1, ok := record1["space"].(map[string]interface{})
		assert.True(t, ok)
		assert.NotContains(t, space1, "physical_used")
		logicalSpace1, ok := space1["logical_space"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, float64(1073741824), logicalSpace1["size"])

		// Check second record
		record2, ok := records[1].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "vol2", record2["name"])
		assert.Equal(t, float64(2147483648), record2["size"])
		assert.NotContains(t, record2, "efficiency")

		space2, ok := record2["space"].(map[string]interface{})
		assert.True(t, ok)
		assert.NotContains(t, space2, "physical_used")
	})

	t.Run("VolumeCreation_POST_ShouldValidateAndInjectFields", func(t *testing.T) {
		rule := proxyRules["/api/storage/volumes"]
		volumeAction := rule.POST.(*processor.VolumeAction)

		t.Run("ValidPOSTRequest_ShouldPass", func(t *testing.T) {
			reqBody := `{
				"name": "test-volume",
				"size": 1073741824,
				"guarantee": {
					"type": "none"
				},
				"space": {
					"logical_space": {
						"enforcement": true,
						"reporting": true
					}
				}
			}`
			req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

			// Test ShouldAllow
			allowed, err := volumeAction.ShouldAllow(req)
			assert.NoError(t, err)
			assert.True(t, allowed)

			// Test ProcessRequest - should inject enforcement field
			w := httptest.NewRecorder()
			err = volumeAction.ProcessRequest(req, w)
			assert.NoError(t, err)

			// Verify injection
			body, _ := io.ReadAll(req.Body)
			var result map[string]interface{}
			err = json.Unmarshal(body, &result)
			assert.NoError(t, err)

			assert.Equal(t, "test-volume", result["name"])
			assert.Equal(t, float64(1073741824), result["size"])

			space, ok := result["space"].(map[string]interface{})
			assert.True(t, ok)
			logicalSpace, ok := space["logical_space"].(map[string]interface{})
			assert.True(t, ok)
			assert.Equal(t, true, logicalSpace["enforcement"]) // Should be injected
			assert.Equal(t, true, logicalSpace["reporting"])   // Should remain as provided
		})

		t.Run("MissingRequiredFields_ShouldFail", func(t *testing.T) {
			reqBody := `{
				"name": "test-volume"
			}`
			req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

			allowed, err := volumeAction.ShouldAllow(req)
			assert.Error(t, err)
			assert.False(t, allowed)
			assert.Contains(t, err.Error(), "required field 'size' is missing")
		})

		t.Run("InvalidGuaranteeType_ShouldFail", func(t *testing.T) {
			reqBody := `{
				"name": "test-volume",
				"size": 1073741824,
				"guarantee": {
					"type": "invalid-type"
				}
			}`
			req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

			allowed, err := volumeAction.ShouldAllow(req)
			assert.Error(t, err)
			assert.False(t, allowed)
			assert.Contains(t, err.Error(), "field 'guarantee.type' has invalid value")
		})

		t.Run("InvalidEnforcementValue_ShouldFail", func(t *testing.T) {
			reqBody := `{
				"name": "test-volume",
				"size": 1073741824,
				"guarantee": {
					"type": "none"
				},
				"space": {
					"logical_space": {
						"enforcement": false
					}
				}
			}`
			req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

			allowed, err := volumeAction.ShouldAllow(req)
			assert.Error(t, err)
			assert.False(t, allowed)
			assert.Contains(t, err.Error(), "field 'space.logical_space.enforcement' has invalid value")
		})

		t.Run("InvalidReportingValue_ShouldFail", func(t *testing.T) {
			reqBody := `{
				"name": "test-volume",
				"size": 1073741824,
				"guarantee": {
					"type": "none"
				},
				"space": {
					"logical_space": {
						"enforcement": true,
						"reporting": false
					}
				}
			}`
			req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

			allowed, err := volumeAction.ShouldAllow(req)
			assert.Error(t, err)
			assert.False(t, allowed)
			assert.Contains(t, err.Error(), "field 'space.logical_space.reporting' has invalid value")
		})
	})

	t.Run("SpecificVolumeDetails_GET_ShouldAllowAndRemoveSensitiveFields", func(t *testing.T) {
		rule := proxyRules["/api/storage/volumes/{uuid}"]
		volumeAction := rule.GET.(*processor.VolumeAction)

		// Test ShouldAllow for GET request
		req, _ := http.NewRequest("GET", "/api/storage/volumes/uuid-123", nil)
		allowed, err := volumeAction.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, allowed)

		// Test ProcessResponse - should remove sensitive fields
		originalBody := `{
			"name": "test-volume",
			"size": 1073741824,
			"efficiency": "sensitive-data",
			"space": {
				"logical_space": {
					"size": 1073741824
				},
				"physical_used": "sensitive-physical-data"
			}
		}`
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(originalBody)),
			Header:     make(http.Header),
		}

		err = volumeAction.ProcessResponse(resp)
		assert.NoError(t, err)

		// Verify sensitive fields are removed
		body, _ := io.ReadAll(resp.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		assert.Equal(t, "test-volume", result["name"])
		assert.Equal(t, float64(1073741824), result["size"])
		assert.NotContains(t, result, "efficiency")

		space, ok := result["space"].(map[string]interface{})
		assert.True(t, ok)
		assert.NotContains(t, space, "physical_used")
		logicalSpace, ok := space["logical_space"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, float64(1073741824), logicalSpace["size"])
	})

	t.Run("VolumeModification_PATCH_ShouldValidateRequiredFields", func(t *testing.T) {
		rule := proxyRules["/api/storage/volumes/{uuid}"]
		volumeAction := rule.PATCH.(*processor.VolumeAction)

		t.Run("ValidPATCHRequest_ShouldPass", func(t *testing.T) {
			reqBody := `{
				"name": "updated-volume",
				"size": 2147483648,
				"guarantee": {
					"type": "volume"
				},
				"space": {
					"logical_space": {
						"enforcement": true
					}
				}
			}`
			req, _ := http.NewRequest("PATCH", "/api/storage/volumes/uuid-123", strings.NewReader(reqBody))

			// Test ShouldAllow
			allowed, err := volumeAction.ShouldAllow(req)
			assert.NoError(t, err)
			assert.True(t, allowed)

			// Test ProcessRequest (no injection rules for PATCH)
			w := httptest.NewRecorder()
			err = volumeAction.ProcessRequest(req, w)
			assert.NoError(t, err)

			// Verify no changes to request body
			body, _ := io.ReadAll(req.Body)
			var result map[string]interface{}
			err = json.Unmarshal(body, &result)
			assert.NoError(t, err)

			assert.Equal(t, "updated-volume", result["name"])
			assert.Equal(t, float64(2147483648), result["size"])
		})

		t.Run("MissingRequiredFields_ShouldFail", func(t *testing.T) {
			reqBody := `{
				"name": "updated-volume"
			}`
			req, _ := http.NewRequest("PATCH", "/api/storage/volumes/uuid-123", strings.NewReader(reqBody))

			allowed, err := volumeAction.ShouldAllow(req)
			assert.Error(t, err)
			assert.False(t, allowed)
			assert.Contains(t, err.Error(), "required field 'size' is missing")
		})

		t.Run("InvalidGuaranteeType_ShouldFail", func(t *testing.T) {
			reqBody := `{
				"name": "updated-volume",
				"size": 2147483648,
				"guarantee": {
					"type": "invalid-type"
				}
			}`
			req, _ := http.NewRequest("PATCH", "/api/storage/volumes/uuid-123", strings.NewReader(reqBody))

			allowed, err := volumeAction.ShouldAllow(req)
			assert.Error(t, err)
			assert.False(t, allowed)
			assert.Contains(t, err.Error(), "field 'guarantee.type' has invalid value")
		})

		t.Run("InvalidEnforcementValue_ShouldFail", func(t *testing.T) {
			reqBody := `{
				"name": "updated-volume",
				"size": 2147483648,
				"guarantee": {
					"type": "none"
				},
				"space": {
					"logical_space": {
						"enforcement": false
					}
				}
			}`
			req, _ := http.NewRequest("PATCH", "/api/storage/volumes/uuid-123", strings.NewReader(reqBody))

			allowed, err := volumeAction.ShouldAllow(req)
			assert.Error(t, err)
			assert.False(t, allowed)
			assert.Contains(t, err.Error(), "field 'space.logical_space.enforcement' has invalid value")
		})
	})

	t.Run("VolumeDeletion_DELETE_ShouldAllowAndRemoveSensitiveFields", func(t *testing.T) {
		rule := proxyRules["/api/storage/volumes/{uuid}"]
		volumeAction := rule.DELETE.(*processor.VolumeAction)

		// Test ShouldAllow for DELETE request (no body validation)
		req, _ := http.NewRequest("DELETE", "/api/storage/volumes/uuid-123", nil)
		allowed, err := volumeAction.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, allowed)

		// Test ProcessRequest for DELETE request (should do nothing)
		w := httptest.NewRecorder()
		err = volumeAction.ProcessRequest(req, w)
		assert.NoError(t, err)

		// Test ProcessResponse - should remove sensitive fields
		originalBody := `{
			"name": "deleted-volume",
			"efficiency": "sensitive-data",
			"space": {
				"physical_used": "sensitive-physical-data"
			}
		}`
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(originalBody)),
			Header:     make(http.Header),
		}

		err = volumeAction.ProcessResponse(resp)
		assert.NoError(t, err)

		// Verify sensitive fields are removed
		body, _ := io.ReadAll(resp.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		assert.Equal(t, "deleted-volume", result["name"])
		assert.NotContains(t, result, "efficiency")

		space, ok := result["space"].(map[string]interface{})
		assert.True(t, ok)
		assert.NotContains(t, space, "physical_used")
	})
}

func TestRuleMap_VolumeActions_Integration(t *testing.T) {
	proxyRules := rules.GetProxyRules()

	t.Run("CompleteVolumeLifecycle_ShouldWorkEndToEnd", func(t *testing.T) {
		// 1. Create a volume (POST)
		createRule := proxyRules["/api/storage/volumes"]
		createAction := createRule.POST.(*processor.VolumeAction)

		createBody := `{
			"name": "lifecycle-test-volume",
			"size": 1073741824,
			"guarantee": {
				"type": "none"
			},
			"space": {
				"logical_space": {
					"enforcement": true,
					"reporting": true
				}
			}
		}`
		createReq, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(createBody))

		// Validate creation request
		allowed, err := createAction.ShouldAllow(createReq)
		assert.NoError(t, err)
		assert.True(t, allowed)

		// Process creation request (inject enforcement)
		w := httptest.NewRecorder()
		err = createAction.ProcessRequest(createReq, w)
		assert.NoError(t, err)

		// 2. List volumes (GET)
		listAction := createRule.GET.(*processor.VolumeAction)
		listReq, _ := http.NewRequest("GET", "/api/storage/volumes", nil)

		allowed, err = listAction.ShouldAllow(listReq)
		assert.NoError(t, err)
		assert.True(t, allowed)

		// 3. Get specific volume (GET with UUID)
		specificRule := proxyRules["/api/storage/volumes/{uuid}"]
		getAction := specificRule.GET.(*processor.VolumeAction)
		getReq, _ := http.NewRequest("GET", "/api/storage/volumes/uuid-123", nil)

		allowed, err = getAction.ShouldAllow(getReq)
		assert.NoError(t, err)
		assert.True(t, allowed)

		// 4. Modify volume (PATCH)
		patchAction := specificRule.PATCH.(*processor.VolumeAction)
		patchBody := `{
			"name": "lifecycle-test-volume-updated",
			"size": 2147483648,
			"guarantee": {
				"type": "volume"
			},
			"space": {
				"logical_space": {
					"enforcement": true
				}
			}
		}`
		patchReq, _ := http.NewRequest("PATCH", "/api/storage/volumes/uuid-123", strings.NewReader(patchBody))

		allowed, err = patchAction.ShouldAllow(patchReq)
		assert.NoError(t, err)
		assert.True(t, allowed)

		// 5. Delete volume (DELETE)
		deleteAction := specificRule.DELETE.(*processor.VolumeAction)
		deleteReq, _ := http.NewRequest("DELETE", "/api/storage/volumes/uuid-123", nil)

		allowed, err = deleteAction.ShouldAllow(deleteReq)
		assert.NoError(t, err)
		assert.True(t, allowed)
	})

	t.Run("AllVolumeActions_ShouldHaveCorrectResponseRules", func(t *testing.T) {
		// Verify all volume actions have the correct response rules
		expectedRemovalRules := []actions.RemovalRule{
			{FieldPath: "efficiency"},
			{FieldPath: "space.physical_used"},
		}

		expectedSpecificVolumeRemovalRules := []actions.RemovalRule{
			{FieldPath: "efficiency"},
			{FieldPath: "space.physical_used"},
			{FieldPath: "space.logical_space.enforcement"},
			{FieldPath: "space.logical_space.reporting"},
		}

		// Check volume listing
		listRule := proxyRules["/api/storage/volumes"]
		listAction := listRule.GET.(*processor.VolumeAction)
		assert.Equal(t, expectedRemovalRules, listAction.ResponseRule.RemovalRules)

		// Check volume creation
		createAction := listRule.POST.(*processor.VolumeAction)
		assert.Equal(t, expectedRemovalRules, createAction.ResponseRule.RemovalRules)

		// Check specific volume details
		specificRule := proxyRules["/api/storage/volumes/{uuid}"]
		getAction := specificRule.GET.(*processor.VolumeAction)
		assert.Equal(t, expectedSpecificVolumeRemovalRules, getAction.ResponseRule.RemovalRules)

		// Check volume modification
		patchAction := specificRule.PATCH.(*processor.VolumeAction)
		assert.Equal(t, expectedRemovalRules, patchAction.ResponseRule.RemovalRules)

		// Check volume deletion
		deleteAction := specificRule.DELETE.(*processor.VolumeAction)
		assert.Equal(t, expectedRemovalRules, deleteAction.ResponseRule.RemovalRules)
	})
}

func TestRuleMap_AggregateActions(t *testing.T) {
	proxyRules := rules.GetProxyRules()

	t.Run("GET /api/storage/aggregates - Allowed", func(t *testing.T) {
		rule := proxyRules["/api/storage/aggregates"]
		action := rule.GET.(*processor.Allow)

		req, _ := http.NewRequest("GET", "/api/storage/aggregates", nil)
		allowed, err := action.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, allowed)
	})

	t.Run("POST /api/storage/aggregates - Denied", func(t *testing.T) {
		rule := proxyRules["/api/storage/aggregates"]
		action := rule.POST.(*processor.Deny)

		req, _ := http.NewRequest("POST", "/api/storage/aggregates", strings.NewReader(`{}`))
		allowed, err := action.ShouldAllow(req)
		assert.NoError(t, err) // Deny returns true, nil for ShouldAllow, actual denial happens in rule_engine
		assert.False(t, allowed)
	})

	t.Run("PATCH /api/storage/aggregates - Denied", func(t *testing.T) {
		rule := proxyRules["/api/storage/aggregates"]
		action := rule.PATCH.(*processor.Deny)

		req, _ := http.NewRequest("PATCH", "/api/storage/aggregates", strings.NewReader(`{}`))
		allowed, err := action.ShouldAllow(req)
		assert.NoError(t, err)
		assert.False(t, allowed)
	})

	t.Run("DELETE /api/storage/aggregates - Denied", func(t *testing.T) {
		rule := proxyRules["/api/storage/aggregates"]
		action := rule.DELETE.(*processor.Deny)

		req, _ := http.NewRequest("DELETE", "/api/storage/aggregates", nil)
		allowed, err := action.ShouldAllow(req)
		assert.NoError(t, err)
		assert.False(t, allowed)
	})
}
