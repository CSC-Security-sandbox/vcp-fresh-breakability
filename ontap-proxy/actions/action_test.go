package actions

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllow_ShouldAllow(t *testing.T) {
	allow := Allow{Name: "Test Allow"}
	req, _ := http.NewRequest("GET", "/test", nil)

	result := allow.ShouldAllow(req)
	assert.True(t, result, "Allow action should always return true")
}

func TestAllow_ProcessRequest(t *testing.T) {
	allow := Allow{Name: "Test Allow"}
	req, _ := http.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	err := allow.ProcessRequest(req, w)
	assert.NoError(t, err, "ProcessRequest should return nil")
	assert.Equal(t, 200, w.Code, "Response should remain unchanged")
}

func TestAllow_ProcessResponse(t *testing.T) {
	t.Run("WhenNoModificationsNeeded_ShouldProcessResponse", func(t *testing.T) {
		allow := Allow{Name: "Test Allow"}
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"name":"test","value":123}`)),
		}

		err := allow.ProcessResponse(resp)
		assert.NoError(t, err, "ProcessResponse should return nil when no modifications needed")
	})

	t.Run("WhenFieldsAreSpecified_ShouldRemoveThem", func(t *testing.T) {
		allow := Allow{
			Name:         "Test Allow",
			RemoveFields: []string{"password", "secret"},
		}
		jsonData := `{"name":"test","password":"secret123","secret":"hidden","public":"visible"}`
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(jsonData)),
		}

		err := allow.ProcessResponse(resp)
		assert.NoError(t, err, "ProcessResponse should return nil")

		body, err := io.ReadAll(resp.Body)
		assert.NoError(t, err, "Should read response body")

		assert.NotContains(t, string(body), "password", "Password field should be removed")
		assert.NotContains(t, string(body), "secret", "Secret field should be removed")
		assert.Contains(t, string(body), "name", "Name field should remain")
		assert.Contains(t, string(body), "public", "Public field should remain")
	})

	t.Run("WhenResponseIsNotJSON_ShouldReturnError", func(t *testing.T) {
		allow := Allow{
			Name:         "Test Allow",
			RemoveFields: []string{"password"},
		}
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader("not json data")),
		}

		err := allow.ProcessResponse(resp)
		assert.Error(t, err, "ProcessResponse should return error for non-JSON response")
		assert.Contains(t, err.Error(), "not valid JSON", "Error should mention JSON validation")
	})

	t.Run("WhenResponseBodyIsEmpty_ShouldReturnError", func(t *testing.T) {
		allow := Allow{
			Name:         "Test Allow",
			RemoveFields: []string{"password"},
		}
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader("")),
		}

		err := allow.ProcessResponse(resp)
		assert.Error(t, err, "ProcessResponse should return error for empty JSON")
	})

	t.Run("WhenNestedJSONFieldsExist_ShouldRemoveAllSpecifiedFields", func(t *testing.T) {
		allow := Allow{
			Name:         "Test Allow",
			RemoveFields: []string{"password", "secret"},
		}
		jsonData := `{
			"name": "test",
			"user": {
				"name": "john",
				"password": "secret123",
				"profile": {
					"secret": "hidden",
					"public": "visible"
				}
			},
			"records": [
				{"name": "record1", "password": "pass1"},
				{"name": "record2", "password": "pass2"}
			]
		}`
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(jsonData)),
		}

		err := allow.ProcessResponse(resp)
		assert.NoError(t, err, "ProcessResponse should return nil")

		body, err := io.ReadAll(resp.Body)
		assert.NoError(t, err, "Should read response body")

		assert.NotContains(t, string(body), "password", "All password fields should be removed")
		assert.NotContains(t, string(body), "secret", "All secret fields should be removed")
		assert.Contains(t, string(body), "name", "Name fields should remain")
		assert.Contains(t, string(body), "public", "Public fields should remain")
	})
}

func TestDeny_ShouldAllow(t *testing.T) {
	deny := Deny{Name: "Test Deny"}
	req, _ := http.NewRequest("GET", "/test", nil)

	result := deny.ShouldAllow(req)
	assert.False(t, result, "Deny action should always return false")
}

func TestDeny_ProcessRequest(t *testing.T) {
	deny := Deny{Name: "Test Deny"}
	req, _ := http.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	err := deny.ProcessRequest(req, w)
	assert.NoError(t, err, "ProcessRequest should return nil")

	assert.Equal(t, http.StatusForbidden, w.Code, "Should return 403 Forbidden")
	assert.Contains(t, w.Body.String(), "Forbidden", "Response body should contain 'Forbidden'")
}

func TestDeny_ProcessResponse(t *testing.T) {
	deny := Deny{Name: "Test Deny"}
	originalData := `{"name":"test","value":123}`
	resp := &http.Response{
		Body: io.NopCloser(strings.NewReader(originalData)),
	}

	err := deny.ProcessResponse(resp)
	assert.NoError(t, err, "ProcessResponse should return nil")

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Should read response body")
	assert.Equal(t, originalData, string(body), "Response should remain unchanged")
}

func TestDenyAll(t *testing.T) {
	action := DenyAll()
	assert.NotNil(t, action, "DenyAll should return non-nil action")

	deny, ok := action.(Deny)
	assert.True(t, ok, "DenyAll should return Deny type")
	assert.Equal(t, "Access denied", deny.Name, "Should have default name")

	// Test that it behaves like a Deny action
	req, _ := http.NewRequest("GET", "/test", nil)
	result := action.ShouldAllow(req)
	assert.False(t, result, "DenyAll action should deny all requests")

	w := httptest.NewRecorder()
	err := action.ProcessRequest(req, w)
	assert.NoError(t, err, "ProcessRequest should return nil")
	assert.Equal(t, http.StatusForbidden, w.Code, "Should return 403 Forbidden")
}

func TestRequestProcessorInterface(t *testing.T) {
	t.Run("WhenAllowIsUsed_ShouldImplementRequestProcessorInterface", func(t *testing.T) {
		var _ RequestProcessor = Allow{Name: "Test"}
	})

	t.Run("WhenDenyIsUsed_ShouldImplementRequestProcessorInterface", func(t *testing.T) {
		var _ RequestProcessor = Deny{Name: "Test"}
	})

	t.Run("WhenDenyAllIsCalled_ShouldReturnRequestProcessor", func(t *testing.T) {
		var _ RequestProcessor = DenyAll()
	})
}
