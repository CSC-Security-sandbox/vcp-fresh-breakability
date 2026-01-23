package dsl

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoModification(t *testing.T) {
	t.Run("Apply_DoesNothing", func(t *testing.T) {
		mod := NoModification{}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		err := mod.Apply(req)

		assert.NoError(t, err)
	})
}

func TestSetHeaders(t *testing.T) {
	t.Run("Apply_SetsHeaders", func(t *testing.T) {
		mod := SetHeaders{
			Headers: map[string]string{
				"X-Custom-Header": "custom-value",
				"Authorization":   "Bearer token",
			},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		err := mod.Apply(req)

		assert.NoError(t, err)
		assert.Equal(t, "custom-value", req.Header.Get("X-Custom-Header"))
		assert.Equal(t, "Bearer token", req.Header.Get("Authorization"))
	})

	t.Run("Apply_OverwritesExistingHeaders", func(t *testing.T) {
		mod := SetHeaders{
			Headers: map[string]string{
				"X-Existing": "new-value",
			},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Existing", "old-value")

		err := mod.Apply(req)

		assert.NoError(t, err)
		assert.Equal(t, "new-value", req.Header.Get("X-Existing"))
	})

	t.Run("Apply_ReturnsErrorForNonRequest", func(t *testing.T) {
		mod := SetHeaders{
			Headers: map[string]string{"X-Test": "value"},
		}

		err := mod.Apply("not a request")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected *http.Request")
	})
}

func TestSetQueryParams(t *testing.T) {
	t.Run("Apply_SetsQueryParams", func(t *testing.T) {
		mod := SetQueryParams{
			Params: map[string]string{
				"fields":      "name,size",
				"max_records": "100",
			},
		}
		req := httptest.NewRequest(http.MethodGet, "/test?existing=value", nil)

		err := mod.Apply(req)

		assert.NoError(t, err)
		assert.Equal(t, "name,size", req.URL.Query().Get("fields"))
		assert.Equal(t, "100", req.URL.Query().Get("max_records"))
		assert.Equal(t, "value", req.URL.Query().Get("existing"))
	})

	t.Run("Apply_OverwritesExistingParams", func(t *testing.T) {
		mod := SetQueryParams{
			Params: map[string]string{
				"existing": "new-value",
			},
		}
		req := httptest.NewRequest(http.MethodGet, "/test?existing=old-value", nil)

		err := mod.Apply(req)

		assert.NoError(t, err)
		assert.Equal(t, "new-value", req.URL.Query().Get("existing"))
	})

	t.Run("Apply_ReturnsErrorForNonRequest", func(t *testing.T) {
		mod := SetQueryParams{
			Params: map[string]string{"key": "value"},
		}

		err := mod.Apply("not a request")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected *http.Request")
	})
}

func TestSetRequestFields(t *testing.T) {
	t.Run("Apply_SetsFieldInRequestBody", func(t *testing.T) {
		mod := SetRequestFields{
			Fields: map[string]interface{}{
				"injected": "value",
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(`{"existing": "data"}`))
		req.Header.Set("Content-Type", "application/json")

		err := mod.Apply(req)

		require.NoError(t, err)
		body, _ := io.ReadAll(req.Body)
		assert.Contains(t, string(body), `"injected":"value"`)
		assert.Contains(t, string(body), `"existing":"data"`)
	})

	t.Run("Apply_SetsNestedField", func(t *testing.T) {
		mod := SetRequestFields{
			Fields: map[string]interface{}{
				"space.logical_space.enforcement": true,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(`{"name": "test"}`))
		req.Header.Set("Content-Type", "application/json")

		err := mod.Apply(req)

		require.NoError(t, err)
		body, _ := io.ReadAll(req.Body)
		assert.Contains(t, string(body), `"space"`)
		assert.Contains(t, string(body), `"logical_space"`)
		assert.Contains(t, string(body), `"enforcement":true`)
	})

	t.Run("Apply_OverwritesExistingField", func(t *testing.T) {
		mod := SetRequestFields{
			Fields: map[string]interface{}{
				"status": "modified",
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(`{"status": "original"}`))
		req.Header.Set("Content-Type", "application/json")

		err := mod.Apply(req)

		require.NoError(t, err)
		body, _ := io.ReadAll(req.Body)
		assert.Contains(t, string(body), `"status":"modified"`)
		assert.NotContains(t, string(body), "original")
	})

	t.Run("Apply_HandlesEmptyBody", func(t *testing.T) {
		mod := SetRequestFields{
			Fields: map[string]interface{}{
				"field": "value",
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(``))

		err := mod.Apply(req)

		assert.NoError(t, err)
	})

	t.Run("Apply_ReturnsErrorForNonRequest", func(t *testing.T) {
		mod := SetRequestFields{
			Fields: map[string]interface{}{"field": "value"},
		}

		err := mod.Apply("not a request")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected *http.Request")
	})
}

func TestSetFields(t *testing.T) {
	t.Run("Apply_SetsFieldFromLiteral", func(t *testing.T) {
		mod := SetFields{
			Fields: map[string]string{
				"$.status": "\"active\"",
			},
		}
		resp := createResponseWithBody(`{"status": "inactive"}`)

		err := mod.Apply(resp)

		require.NoError(t, err)
		body := readResponseBody(t, resp)
		assert.Contains(t, body, `"status":"active"`)
	})

	t.Run("Apply_SetsFieldFromJSONPath", func(t *testing.T) {
		mod := SetFields{
			Fields: map[string]string{
				"$.display_name": "$.internal_name",
			},
		}
		resp := createResponseWithBody(`{"internal_name": "test-volume", "display_name": ""}`)

		err := mod.Apply(resp)

		require.NoError(t, err)
		body := readResponseBody(t, resp)
		assert.Contains(t, body, `"display_name":"test-volume"`)
	})

	t.Run("Apply_SetsNestedField", func(t *testing.T) {
		mod := SetFields{
			Fields: map[string]string{
				"$.space.used": "1000",
			},
		}
		resp := createResponseWithBody(`{"space": {"available": 5000}}`)

		err := mod.Apply(resp)

		require.NoError(t, err)
		body := readResponseBody(t, resp)
		assert.Contains(t, body, `"used":1000`)
	})

	t.Run("Apply_HandlesEmptyBody", func(t *testing.T) {
		mod := SetFields{
			Fields: map[string]string{
				"$.status": "\"active\"",
			},
		}
		resp := createResponseWithBody(``)

		err := mod.Apply(resp)

		assert.NoError(t, err)
	})

	t.Run("Apply_HandlesNonJSONBody", func(t *testing.T) {
		mod := SetFields{
			Fields: map[string]string{
				"$.status": "\"active\"",
			},
		}
		resp := createResponseWithBody(`not json`)

		err := mod.Apply(resp)

		assert.NoError(t, err)
	})

	t.Run("Apply_AppliesModificationsToRecordsArray", func(t *testing.T) {
		mod := SetFields{
			Fields: map[string]string{
				"$.status": "\"processed\"",
			},
		}
		resp := createResponseWithBody(`{"records": [{"name": "vol1", "status": "pending"}, {"name": "vol2", "status": "pending"}]}`)

		err := mod.Apply(resp)

		require.NoError(t, err)
		body := readResponseBody(t, resp)
		assert.Contains(t, body, `"status":"processed"`)
	})

	t.Run("Apply_ReturnsErrorForNonResponse", func(t *testing.T) {
		mod := SetFields{
			Fields: map[string]string{"$.status": "\"active\""},
		}

		err := mod.Apply("not a response")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected *http.Response")
	})
}

func TestRemoveFields(t *testing.T) {
	t.Run("Apply_RemovesTopLevelField", func(t *testing.T) {
		mod := RemoveFields{
			Fields: []string{"$.sensitive"},
		}
		resp := createResponseWithBody(`{"name": "test", "sensitive": "secret"}`)

		err := mod.Apply(resp)

		require.NoError(t, err)
		body := readResponseBody(t, resp)
		assert.Contains(t, body, `"name":"test"`)
		assert.NotContains(t, body, "sensitive")
	})

	t.Run("Apply_RemovesNestedField", func(t *testing.T) {
		mod := RemoveFields{
			Fields: []string{"$.space.physical_used"},
		}
		resp := createResponseWithBody(`{"space": {"available": 1000, "physical_used": 500}}`)

		err := mod.Apply(resp)

		require.NoError(t, err)
		body := readResponseBody(t, resp)
		assert.Contains(t, body, `"available":1000`)
		assert.NotContains(t, body, "physical_used")
	})

	t.Run("Apply_RemovesMultipleFields", func(t *testing.T) {
		mod := RemoveFields{
			Fields: []string{
				"$.field1",
				"$.field2",
				"$.nested.field3",
			},
		}
		resp := createResponseWithBody(`{"field1": "a", "field2": "b", "nested": {"field3": "c", "field4": "d"}}`)

		err := mod.Apply(resp)

		require.NoError(t, err)
		body := readResponseBody(t, resp)
		assert.NotContains(t, body, "field1")
		assert.NotContains(t, body, "field2")
		assert.NotContains(t, body, "field3")
		assert.Contains(t, body, `"field4":"d"`)
	})

	t.Run("Apply_HandlesNonExistentField", func(t *testing.T) {
		mod := RemoveFields{
			Fields: []string{"$.nonexistent"},
		}
		resp := createResponseWithBody(`{"name": "test"}`)

		err := mod.Apply(resp)

		require.NoError(t, err)
		body := readResponseBody(t, resp)
		assert.Contains(t, body, `"name":"test"`)
	})

	t.Run("Apply_AppliesRemovalsToRecordsArray", func(t *testing.T) {
		mod := RemoveFields{
			Fields: []string{"$.efficiency"},
		}
		resp := createResponseWithBody(`{"records": [{"name": "vol1", "efficiency": "high"}, {"name": "vol2", "efficiency": "low"}]}`)

		err := mod.Apply(resp)

		require.NoError(t, err)
		body := readResponseBody(t, resp)
		assert.NotContains(t, body, "efficiency")
	})

	t.Run("Apply_ReturnsErrorForNonResponse", func(t *testing.T) {
		mod := RemoveFields{
			Fields: []string{"$.field"},
		}

		err := mod.Apply("not a response")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected *http.Response")
	})
}

func TestAllOf(t *testing.T) {
	t.Run("Apply_AppliesModificationsInSequence", func(t *testing.T) {
		mod := AllOf(
			RemoveFields{Fields: []string{"$.sensitive"}},
			SetFields{Fields: map[string]string{"$.status": "\"redacted\""}},
		)
		resp := createResponseWithBody(`{"name": "test", "sensitive": "secret", "status": "active"}`)

		err := mod.Apply(resp)

		require.NoError(t, err)
		body := readResponseBody(t, resp)
		assert.NotContains(t, body, "sensitive")
		assert.Contains(t, body, `"status":"redacted"`)
	})

	t.Run("Apply_StopsOnError", func(t *testing.T) {
		// First modification will fail because target is not a response
		mod := AllOf(
			RemoveFields{Fields: []string{"$.field"}},
			SetFields{Fields: map[string]string{"$.status": "\"active\""}},
		)

		err := mod.Apply("not a response")

		assert.Error(t, err)
	})

	t.Run("Apply_WithEmptyList_DoesNothing", func(t *testing.T) {
		mod := AllOf()
		resp := createResponseWithBody(`{"name":"test"}`)

		err := mod.Apply(resp)

		assert.NoError(t, err)
		body := readResponseBody(t, resp)
		assert.Contains(t, body, `"name":"test"`)
	})
}

func TestNormalizeJSONPath(t *testing.T) {
	t.Run("RemovesDollarDotPrefix", func(t *testing.T) {
		result := normalizeJSONPath("$.field.nested")
		assert.Equal(t, "field.nested", result)
	})

	t.Run("LeavesPathWithoutPrefixUnchanged", func(t *testing.T) {
		result := normalizeJSONPath("field.nested")
		assert.Equal(t, "field.nested", result)
	})
}

// Helper functions

func createResponseWithBody(body string) *http.Response {
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
	}
}

func readResponseBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}
