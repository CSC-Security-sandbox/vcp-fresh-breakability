package processor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeny(t *testing.T) {
	t.Run("ShouldAllow_AlwaysReturnsFalse", func(t *testing.T) {
		deny := &Deny{
			Name: "Test deny action",
		}

		// Test with different HTTP methods
		methods := []string{http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodDelete, http.MethodPut}

		for _, method := range methods {
			t.Run("Method_"+method, func(t *testing.T) {
				req := httptest.NewRequest(method, "/test", strings.NewReader(`{"test": "data"}`))

				allowed, err := deny.ShouldAllow(req)

				assert.False(t, allowed, "ShouldAllow should always return false for %s", method)
				assert.NoError(t, err, "ShouldAllow should not return error for %s", method)
			})
		}
	})

	t.Run("ShouldAllow_WithNilRequest_ReturnsFalse", func(t *testing.T) {
		deny := &Deny{
			Name: "Test deny action",
		}

		allowed, err := deny.ShouldAllow(nil)

		assert.False(t, allowed, "ShouldAllow should return false even with nil request")
		assert.NoError(t, err, "ShouldAllow should not return error with nil request")
	})

	t.Run("ShouldAllow_WithEmptyRequest_ReturnsFalse", func(t *testing.T) {
		deny := &Deny{
			Name: "Test deny action",
		}

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		allowed, err := deny.ShouldAllow(req)

		assert.False(t, allowed, "ShouldAllow should return false with empty request")
		assert.NoError(t, err, "ShouldAllow should not return error with empty request")
	})

	t.Run("ShouldAllow_WithComplexRequest_ReturnsFalse", func(t *testing.T) {
		deny := &Deny{
			Name: "Test deny action",
		}

		// Complex request with headers, query params, etc.
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes?param=value",
			strings.NewReader(`{"name": "test-volume", "size": 1073741824}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		allowed, err := deny.ShouldAllow(req)

		assert.False(t, allowed, "ShouldAllow should return false regardless of request complexity")
		assert.NoError(t, err, "ShouldAllow should not return error with complex request")
	})

	t.Run("ProcessRequest_NeverReached_ReturnsNil", func(t *testing.T) {
		deny := &Deny{
			Name: "Test deny action",
		}

		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"test": "data"}`))
		w := httptest.NewRecorder()

		err := deny.ProcessRequest(req, w)

		assert.NoError(t, err, "ProcessRequest should return nil (though never reached)")
	})

	t.Run("ProcessRequest_WithNilRequest_ReturnsNil", func(t *testing.T) {
		deny := &Deny{
			Name: "Test deny action",
		}

		w := httptest.NewRecorder()
		err := deny.ProcessRequest(nil, w)

		assert.NoError(t, err, "ProcessRequest should return nil even with nil request")
	})

	t.Run("ProcessRequest_WithNilResponseWriter_ReturnsNil", func(t *testing.T) {
		deny := &Deny{
			Name: "Test deny action",
		}

		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"test": "data"}`))
		err := deny.ProcessRequest(req, nil)

		assert.NoError(t, err, "ProcessRequest should return nil even with nil response writer")
	})

	t.Run("ProcessResponse_NeverReached_ReturnsNil", func(t *testing.T) {
		deny := &Deny{
			Name: "Test deny action",
		}

		resp := &http.Response{
			Body:       io.NopCloser(strings.NewReader(`{"test": "data"}`)),
			StatusCode: 200,
			Header:     make(http.Header),
		}

		err := deny.ProcessResponse(resp)

		assert.NoError(t, err, "ProcessResponse should return nil (though never reached)")
	})

	t.Run("ProcessResponse_WithNilResponse_ReturnsNil", func(t *testing.T) {
		deny := &Deny{
			Name: "Test deny action",
		}

		err := deny.ProcessResponse(nil)

		assert.NoError(t, err, "ProcessResponse should return nil even with nil response")
	})

	t.Run("ProcessResponse_WithEmptyResponse_ReturnsNil", func(t *testing.T) {
		deny := &Deny{
			Name: "Test deny action",
		}

		resp := &http.Response{
			Body:       io.NopCloser(strings.NewReader("")),
			StatusCode: 404,
			Header:     make(http.Header),
		}

		err := deny.ProcessResponse(resp)

		assert.NoError(t, err, "ProcessResponse should return nil with empty response")
	})

	t.Run("DenyStruct_Initialization", func(t *testing.T) {
		t.Run("WithName_ShouldSetCorrectly", func(t *testing.T) {
			name := "Custom deny action"
			deny := &Deny{
				Name: name,
			}

			assert.Equal(t, name, deny.Name, "Name should be set correctly")
		})

		t.Run("WithEmptyName_ShouldBeEmpty", func(t *testing.T) {
			deny := &Deny{}

			assert.Empty(t, deny.Name, "Name should be empty when not set")
		})

		t.Run("WithLongName_ShouldSetCorrectly", func(t *testing.T) {
			longName := strings.Repeat("very-long-name-", 100)
			deny := &Deny{
				Name: longName,
			}

			assert.Equal(t, longName, deny.Name, "Long name should be set correctly")
		})
	})

	t.Run("DenyBehavior_Consistency", func(t *testing.T) {
		deny := &Deny{
			Name: "Consistent deny action",
		}

		// Test multiple calls to ensure consistent behavior
		for i := 0; i < 10; i++ {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			allowed, err := deny.ShouldAllow(req)

			assert.False(t, allowed, "ShouldAllow should consistently return false (call %d)", i+1)
			assert.NoError(t, err, "ShouldAllow should consistently return no error (call %d)", i+1)
		}
	})

	t.Run("DenyWithDifferentRequestTypes", func(t *testing.T) {
		deny := &Deny{
			Name: "Test deny action",
		}

		testCases := []struct {
			name    string
			method  string
			url     string
			body    string
			headers map[string]string
		}{
			{
				name:   "Simple GET",
				method: http.MethodGet,
				url:    "/api/storage/volumes",
				body:   "",
			},
			{
				name:   "POST with JSON",
				method: http.MethodPost,
				url:    "/api/storage/volumes",
				body:   `{"name": "test-volume", "size": 1073741824}`,
				headers: map[string]string{
					"Content-Type": "application/json",
				},
			},
			{
				name:   "PATCH with query params",
				method: http.MethodPatch,
				url:    "/api/storage/volumes/123?return_records=true",
				body:   `{"size": 2147483648}`,
				headers: map[string]string{
					"Content-Type":  "application/json",
					"Authorization": "Bearer token",
				},
			},
			{
				name:   "DELETE with headers",
				method: http.MethodDelete,
				url:    "/api/storage/volumes/123",
				body:   "",
				headers: map[string]string{
					"X-Custom-Header": "value",
					"User-Agent":      "test-agent",
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(tc.method, tc.url, strings.NewReader(tc.body))

				// Set headers
				for key, value := range tc.headers {
					req.Header.Set(key, value)
				}

				allowed, err := deny.ShouldAllow(req)

				assert.False(t, allowed, "ShouldAllow should return false for %s", tc.name)
				assert.NoError(t, err, "ShouldAllow should not return error for %s", tc.name)
			})
		}
	})

	t.Run("DenyEdgeCases", func(t *testing.T) {
		deny := &Deny{
			Name: "Edge case deny action",
		}

		t.Run("RequestWithInvalidURL", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/invalid-url", nil)
			allowed, err := deny.ShouldAllow(req)

			assert.False(t, allowed, "ShouldAllow should return false even with invalid URL")
			assert.NoError(t, err, "ShouldAllow should not return error with invalid URL")
		})

		t.Run("RequestWithVeryLongURL", func(t *testing.T) {
			longURL := "/api/storage/volumes/" + strings.Repeat("very-long-uuid-", 100)
			req := httptest.NewRequest(http.MethodGet, longURL, nil)
			allowed, err := deny.ShouldAllow(req)

			assert.False(t, allowed, "ShouldAllow should return false even with very long URL")
			assert.NoError(t, err, "ShouldAllow should not return error with very long URL")
		})

		t.Run("RequestWithSpecialCharacters", func(t *testing.T) {
			specialURL := "/api/storage/volumes/123?param=value&other=test%20with%20spaces"
			req := httptest.NewRequest(http.MethodGet, specialURL, nil)
			allowed, err := deny.ShouldAllow(req)

			assert.False(t, allowed, "ShouldAllow should return false even with special characters")
			assert.NoError(t, err, "ShouldAllow should not return error with special characters")
		})
	})
}
