package processor

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
)

func TestAllow_ShouldAllow(t *testing.T) {
	t.Run("WhenNoValidationRules_ShouldReturnTrue", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{},
			},
		}
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(`{"name": "test"}`))

		result, err := allow.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("WhenGETRequest_ShouldReturnTrue", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "name", Required: true},
				},
			},
		}
		req, _ := http.NewRequest("GET", "/test", nil)

		result, err := allow.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("WhenValidRequest_ShouldReturnTrue", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "name", Required: true},
					{FieldPath: "type", Values: []interface{}{"volume", "snapshot"}},
				},
			},
		}
		reqBody := `{"name": "test-volume", "type": "volume"}`
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(reqBody))

		result, err := allow.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("WhenRequiredFieldMissing_ShouldReturnFalse", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "name", Required: true},
				},
			},
		}
		reqBody := `{"type": "volume"}`
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(reqBody))

		result, err := allow.ShouldAllow(req)
		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "required field 'name' is missing")
	})

	t.Run("WhenInvalidValue_ShouldReturnFalse", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "type", Values: []interface{}{"volume", "snapshot"}},
				},
			},
		}
		reqBody := `{"type": "invalid"}`
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(reqBody))

		result, err := allow.ShouldAllow(req)
		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "field 'type' has invalid value")
	})

	t.Run("WhenInvalidJSON_ShouldReturnError", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "name", Required: true},
				},
			},
		}
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(`invalid json`))

		result, err := allow.ShouldAllow(req)
		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "invalid JSON in request body")
	})

	t.Run("WhenNestedFieldValidation_ShouldWork", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "space.logical_space.enforcement", Required: true},
					{FieldPath: "guarantee.type", Values: []interface{}{"none", "volume"}},
				},
			},
		}
		reqBody := `{
			"space": {
				"logical_space": {
					"enforcement": true
				}
			},
			"guarantee": {
				"type": "none"
			}
		}`
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(reqBody))

		result, err := allow.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, result)
	})
}

func TestAllow_ProcessRequest(t *testing.T) {
	t.Run("WhenNoInjectionRules_ShouldReturnNil", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				InjectionRules: []actions.InjectionRule{},
			},
		}
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(`{"name": "test"}`))
		w := httptest.NewRecorder()

		err := allow.ProcessRequest(req, w)
		assert.NoError(t, err)
	})

	t.Run("WhenGETRequest_ShouldReturnNil", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "injected", Value: "test"},
				},
			},
		}
		req, _ := http.NewRequest("GET", "/test", nil)
		w := httptest.NewRecorder()

		err := allow.ProcessRequest(req, w)
		assert.NoError(t, err)
	})

	t.Run("WhenValidRequest_ShouldInjectFields", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "injected", Value: "test-value"},
					{FieldPath: "space.logical_space.enforcement", Value: true},
				},
			},
		}
		reqBody := `{"name": "test-volume"}`
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(reqBody))
		w := httptest.NewRecorder()

		err := allow.ProcessRequest(req, w)
		assert.NoError(t, err)

		// Verify the request body was modified
		body, _ := io.ReadAll(req.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		assert.Equal(t, "test-volume", result["name"])
		assert.Equal(t, "test-value", result["injected"])

		space, ok := result["space"].(map[string]interface{})
		assert.True(t, ok)
		logicalSpace, ok := space["logical_space"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, true, logicalSpace["enforcement"])
	})

	t.Run("WhenInvalidJSON_ShouldReturnError", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "injected", Value: "test"},
				},
			},
		}
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(`invalid json`))
		w := httptest.NewRecorder()

		err := allow.ProcessRequest(req, w)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid JSON in request body")
	})
}

func TestAllow_ProcessResponse(t *testing.T) {
	t.Run("WhenNoResponseRules_ShouldReturnNil", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			ResponseRule: actions.ResponseRule{
				InjectionRules: []actions.InjectionRule{},
				RemovalRules:   []actions.RemovalRule{},
			},
		}
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(`{"name": "test"}`)),
		}

		err := allow.ProcessResponse(resp)
		assert.NoError(t, err)
	})

	t.Run("WhenNonSuccessStatusCode_ShouldReturnNil", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			ResponseRule: actions.ResponseRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "injected", Value: "test"},
				},
			},
		}
		resp := &http.Response{
			StatusCode: 404,
			Body:       io.NopCloser(strings.NewReader(`{"error": "not found"}`)),
		}

		err := allow.ProcessResponse(resp)
		assert.NoError(t, err)
	})

	t.Run("WhenSuccessStatusCode_ShouldProcessResponse", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			ResponseRule: actions.ResponseRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "injected", Value: "test-value"},
				},
				RemovalRules: []actions.RemovalRule{
					{FieldPath: "sensitive"},
				},
			},
		}
		originalBody := `{"name": "test-volume", "sensitive": "secret-data"}`
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(originalBody)),
			Header:     make(http.Header),
		}

		err := allow.ProcessResponse(resp)
		assert.NoError(t, err)

		// Verify the response body was modified
		body, _ := io.ReadAll(resp.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		assert.Equal(t, "test-volume", result["name"])
		assert.Equal(t, "test-value", result["injected"])
		assert.NotContains(t, result, "sensitive")
	})

	t.Run("WhenRecordsArray_ShouldProcessEachRecord", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			ResponseRule: actions.ResponseRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "injected", Value: "test-value"},
				},
				RemovalRules: []actions.RemovalRule{
					{FieldPath: "sensitive"},
				},
			},
		}
		originalBody := `{
			"records": [
				{"name": "vol1", "sensitive": "secret1"},
				{"name": "vol2", "sensitive": "secret2"}
			]
		}`
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(originalBody)),
			Header:     make(http.Header),
		}

		err := allow.ProcessResponse(resp)
		assert.NoError(t, err)

		// Verify the response body was modified
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
		assert.Equal(t, "test-value", record1["injected"])
		assert.NotContains(t, record1, "sensitive")

		// Check second record
		record2, ok := records[1].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "vol2", record2["name"])
		assert.Equal(t, "test-value", record2["injected"])
		assert.NotContains(t, record2, "sensitive")
	})

	t.Run("WhenInvalidJSON_ShouldReturnError", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			ResponseRule: actions.ResponseRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "injected", Value: "test"},
				},
			},
		}
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(`invalid json`)),
		}

		err := allow.ProcessResponse(resp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode response")
	})
}

func TestAllow_EdgeCases(t *testing.T) {
	t.Run("WhenEmptyRequestBody_ShouldHandleGracefully", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "name", Required: true},
				},
			},
		}
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(`{}`))

		result, err := allow.ShouldAllow(req)
		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "required field 'name' is missing")
	})

	t.Run("WhenNestedFieldDoesNotExist_ShouldHandleGracefully", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "space.logical_space.enforcement", Required: true},
				},
			},
		}
		reqBody := `{"space": {"logical_space": {}}}`
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(reqBody))

		result, err := allow.ShouldAllow(req)
		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "required field 'space.logical_space.enforcement' is missing")
	})

	t.Run("WhenInjectionIntoNonExistentPath_ShouldCreatePath", func(t *testing.T) {
		allow := &Allow{
			Name: "Test Allow",
			RequestRule: actions.RequestRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "new.nested.field", Value: "test-value"},
				},
			},
		}
		reqBody := `{"name": "test"}`
		req, _ := http.NewRequest("POST", "/test", strings.NewReader(reqBody))
		w := httptest.NewRecorder()

		err := allow.ProcessRequest(req, w)
		assert.NoError(t, err)

		// Verify the request body was modified
		body, _ := io.ReadAll(req.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		assert.Equal(t, "test", result["name"])

		newField, ok := result["new"].(map[string]interface{})
		assert.True(t, ok)
		nestedField, ok := newField["nested"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "test-value", nestedField["field"])
	})
}
