package dsl

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRequestBody(t *testing.T) {
	t.Run("WhenNilRequest_ShouldReturnNil", func(t *testing.T) {
		result := ParseRequestBody(nil)
		assert.Nil(t, result)
	})

	t.Run("WhenGETRequest_ShouldSkipParsing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		result := ParseRequestBody(req)

		assert.False(t, HasParsedBody(result))
	})

	t.Run("WhenHEADRequest_ShouldSkipParsing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/test", nil)
		result := ParseRequestBody(req)

		assert.False(t, HasParsedBody(result))
	})

	t.Run("WhenDELETERequest_ShouldSkipParsing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/test", nil)
		result := ParseRequestBody(req)

		assert.False(t, HasParsedBody(result))
	})

	t.Run("WhenValidJSON_ShouldParseSuccessfully", func(t *testing.T) {
		body := `{"name": "test", "size": 1024}`
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(body))
		result := ParseRequestBody(req)

		assert.True(t, HasParsedBody(result))
		data, err := GetParsedBody(result)
		assert.Empty(t, err)
		assert.Equal(t, "test", data["name"])
		assert.Equal(t, float64(1024), data["size"])
	})

	t.Run("WhenInvalidJSON_ShouldStoreParseError", func(t *testing.T) {
		body := `{invalid json}`
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(body))
		result := ParseRequestBody(req)

		assert.True(t, HasParsedBody(result))
		data, err := GetParsedBody(result)
		assert.Nil(t, data)
		assert.Equal(t, "invalid JSON in request body", err)
	})

	t.Run("WhenEmptyBody_ShouldStoreParseError", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(""))
		result := ParseRequestBody(req)

		assert.True(t, HasParsedBody(result))
		data, err := GetParsedBody(result)
		assert.Nil(t, data)
		assert.Equal(t, "request body is empty", err)
	})

	t.Run("WhenAlreadyParsed_ShouldReturnSameRequest", func(t *testing.T) {
		body := `{"name": "test"}`
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(body))
		result1 := ParseRequestBody(req)
		result2 := ParseRequestBody(result1)

		// Should return the same request context
		assert.Equal(t, result1.Context(), result2.Context())
	})

	t.Run("WhenBodyExceedsLimit_ShouldStoreParseError", func(t *testing.T) {
		// Create a body larger than MaxRequestBodySize (5MB)
		largeBody := strings.Repeat("x", MaxRequestBodySize+100)
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(largeBody))
		result := ParseRequestBody(req)

		assert.True(t, HasParsedBody(result))
		data, err := GetParsedBody(result)
		assert.Nil(t, data)
		assert.Equal(t, "request body too large (max 5MB)", err)
	})
}

func TestGetParsedBody(t *testing.T) {
	t.Run("WhenNilRequest_ShouldReturnError", func(t *testing.T) {
		data, err := GetParsedBody(nil)
		assert.Nil(t, data)
		assert.Equal(t, "request is nil", err)
	})

	t.Run("WhenNotParsed_ShouldParseOnDemand", func(t *testing.T) {
		body := `{"name": "fallback"}`
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(body))

		// Don't call ParseRequestBody, GetParsedBody should fallback to parsing
		data, err := GetParsedBody(req)
		assert.Empty(t, err)
		assert.Equal(t, "fallback", data["name"])
	})

	t.Run("WhenParsedWithError_ShouldReturnError", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(""))
		result := ParseRequestBody(req)

		data, err := GetParsedBody(result)
		assert.Nil(t, data)
		assert.NotEmpty(t, err)
	})
}

func TestGetRawBody(t *testing.T) {
	t.Run("WhenNilRequest_ShouldReturnNil", func(t *testing.T) {
		result := GetRawBody(nil)
		assert.Nil(t, result)
	})

	t.Run("WhenNotParsed_ShouldReturnNil", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader("test"))
		result := GetRawBody(req)
		assert.Nil(t, result)
	})

	t.Run("WhenParsed_ShouldReturnRawBytes", func(t *testing.T) {
		body := `{"name": "test"}`
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(body))
		result := ParseRequestBody(req)

		rawBody := GetRawBody(result)
		assert.Equal(t, []byte(body), rawBody)
	})
}

func TestBodyParsingOptimization(t *testing.T) {
	t.Run("WhenMultipleConditionsCheck_ShouldParseOnce", func(t *testing.T) {
		body := `{"name": "test", "size": 1024, "type": "volume"}`
		req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(body))

		// Parse once at middleware level
		req = ParseRequestBody(req)

		// Multiple condition checks should use cached body
		cond1 := HasFields("name", "size")
		cond2 := HasFieldValue("type", "volume")
		cond3 := HasFieldValueIn("size", float64(1024), float64(2048))

		ok1, _ := cond1(req)
		ok2, _ := cond2(req)
		ok3, _ := cond3(req)

		assert.True(t, ok1)
		assert.True(t, ok2)
		assert.True(t, ok3)
	})
}
