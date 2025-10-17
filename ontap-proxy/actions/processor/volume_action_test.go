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

func TestVolumeAction_ShouldAllow(t *testing.T) {
	t.Run("WhenGETRequest_ShouldReturnTrue", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "name", Required: true},
				},
			},
		}
		req, _ := http.NewRequest("GET", "/api/storage/volumes", nil)

		result, err := volumeAction.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("WhenValidPOSTRequest_ShouldReturnTrue", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "name", Required: true},
					{FieldPath: "size", Required: true},
					{FieldPath: "guarantee.type", Values: []interface{}{"none", "volume"}},
					{FieldPath: "is_svm_root", Values: []interface{}{true}},
				},
			},
		}
		reqBody := `{
			"name": "test-volume",
			"size": 1073741824,
			"guarantee": {
				"type": "none"
			},
			"is_svm_root": true
		}`
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

		result, err := volumeAction.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("WhenValidPATCHRequest_ShouldReturnTrue", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "max_dir_size", MinValue: 20, MaxValue: 1000},
					{FieldPath: "is_svm_root", Values: []interface{}{true}},
				},
			},
		}
		reqBody := `{
			"max_dir_size": 100,
			"is_svm_root": true
		}`
		req, _ := http.NewRequest("PATCH", "/api/storage/volumes/uuid", strings.NewReader(reqBody))

		result, err := volumeAction.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("WhenRequiredFieldMissing_ShouldReturnFalse", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "name", Required: true},
					{FieldPath: "size", Required: true},
				},
			},
		}
		reqBody := `{"name": "test-volume"}`
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

		result, err := volumeAction.ShouldAllow(req)
		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "required field 'size' is missing")
	})

	t.Run("WhenInvalidGuaranteeType_ShouldReturnFalse", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "guarantee.type", Values: []interface{}{"none", "volume"}},
				},
			},
		}
		reqBody := `{
			"guarantee": {
				"type": "invalid-type"
			}
		}`
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

		result, err := volumeAction.ShouldAllow(req)
		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "field 'guarantee.type' has invalid value")
	})

	t.Run("WhenIsSvmRootIsFalse_ShouldReturnFalse", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "is_svm_root", Values: []interface{}{true}},
				},
			},
		}
		reqBody := `{"is_svm_root": false}`
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

		result, err := volumeAction.ShouldAllow(req)
		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "field 'is_svm_root' has invalid value")
	})

	t.Run("WhenInvalidJSON_ShouldReturnError", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "name", Required: true},
				},
			},
		}
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(`invalid json`))

		result, err := volumeAction.ShouldAllow(req)
		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "invalid JSON in request body")
	})

	t.Run("WhenNestedFieldValidation_ShouldWork", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "space.logical_space.enforcement", Required: true},
					{FieldPath: "space.logical_space.enforcement", Values: []interface{}{true}},
				},
			},
		}
		reqBody := `{
			"space": {
				"logical_space": {
					"enforcement": true
				}
			}
		}`
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

		result, err := volumeAction.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, result)
	})
}

func TestVolumeAction_ProcessRequest(t *testing.T) {
	t.Run("WhenGETRequest_ShouldReturnNil", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "space.logical_space.enforcement", Value: true},
				},
			},
		}
		req, _ := http.NewRequest("GET", "/api/storage/volumes", nil)
		w := httptest.NewRecorder()

		err := volumeAction.ProcessRequest(req, w)
		assert.NoError(t, err)
	})

	t.Run("WhenPOSTRequest_ShouldInjectFields", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "space.logical_space.enforcement", Value: true},
					{FieldPath: "is_svm_root", Value: true},
				},
			},
		}
		reqBody := `{
			"name": "test-volume",
			"size": 1073741824
		}`
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))
		w := httptest.NewRecorder()

		err := volumeAction.ProcessRequest(req, w)
		assert.NoError(t, err)

		// Verify the request body was modified
		body, _ := io.ReadAll(req.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		assert.Equal(t, "test-volume", result["name"])
		assert.Equal(t, float64(1073741824), result["size"])
		assert.Equal(t, true, result["is_svm_root"])

		// Check nested field injection
		space, ok := result["space"].(map[string]interface{})
		assert.True(t, ok)
		logicalSpace, ok := space["logical_space"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, true, logicalSpace["enforcement"])
	})

	t.Run("WhenPATCHRequest_ShouldInjectFields", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "is_svm_root", Value: true},
				},
			},
		}
		reqBody := `{
			"max_dir_size": 100
		}`
		req, _ := http.NewRequest("PATCH", "/api/storage/volumes/uuid", strings.NewReader(reqBody))
		w := httptest.NewRecorder()

		err := volumeAction.ProcessRequest(req, w)
		assert.NoError(t, err)

		// Verify the request body was modified
		body, _ := io.ReadAll(req.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		assert.Equal(t, float64(100), result["max_dir_size"])
		assert.Equal(t, true, result["is_svm_root"])
	})

	t.Run("WhenInvalidJSON_ShouldReturnError", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "space.logical_space.enforcement", Value: true},
				},
			},
		}
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(`invalid json`))
		w := httptest.NewRecorder()

		err := volumeAction.ProcessRequest(req, w)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid JSON in request body")
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("WhenInjectionIntoExistingNestedField_ShouldOverride", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "space.logical_space.enforcement", Value: true},
				},
			},
		}
		reqBody := `{
			"name": "test-volume",
			"space": {
				"logical_space": {
					"enforcement": false
				}
			}
		}`
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))
		w := httptest.NewRecorder()

		err := volumeAction.ProcessRequest(req, w)
		assert.NoError(t, err)

		// Verify the request body was modified
		body, _ := io.ReadAll(req.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		space, ok := result["space"].(map[string]interface{})
		assert.True(t, ok)
		logicalSpace, ok := space["logical_space"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, true, logicalSpace["enforcement"]) // Should be overridden to true
	})
}

func TestVolumeAction_ProcessResponse(t *testing.T) {
	t.Run("WhenNoResponseRules_ShouldReturnNil", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			ResponseRule: actions.ResponseRule{
				InjectionRules: []actions.InjectionRule{},
				RemovalRules:   []actions.RemovalRule{},
			},
		}
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(`{"name": "test-volume"}`)),
		}

		err := volumeAction.ProcessResponse(resp)
		assert.NoError(t, err)
	})

	t.Run("WhenNonSuccessStatusCode_ShouldReturnNil", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
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

		err := volumeAction.ProcessResponse(resp)
		assert.NoError(t, err)
	})

	t.Run("WhenSuccessStatusCode_ShouldProcessResponse", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			ResponseRule: actions.ResponseRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "space.logical_space.enforcement", Value: true},
				},
				RemovalRules: []actions.RemovalRule{
					{FieldPath: "efficiency"},
					{FieldPath: "space.physical_used"},
				},
			},
		}
		originalBody := `{
			"name": "test-volume",
			"size": 1073741824,
			"efficiency": "some-sensitive-data",
			"space": {
				"logical_space": {
					"enforcement": false
				},
				"physical_used": "sensitive-physical-data"
			}
		}`
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(originalBody)),
			Header:     make(http.Header),
		}

		err := volumeAction.ProcessResponse(resp)
		assert.NoError(t, err)

		// Verify the response body was modified
		body, _ := io.ReadAll(resp.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		assert.Equal(t, "test-volume", result["name"])
		assert.Equal(t, float64(1073741824), result["size"])
		assert.NotContains(t, result, "efficiency") // Should be removed

		// Check space modifications
		space, ok := result["space"].(map[string]interface{})
		assert.True(t, ok)
		assert.NotContains(t, space, "physical_used") // Should be removed

		logicalSpace, ok := space["logical_space"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, true, logicalSpace["enforcement"]) // Should be injected
	})

	t.Run("WhenRecordsArray_ShouldProcessEachRecord", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			ResponseRule: actions.ResponseRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "space.logical_space.enforcement", Value: true},
				},
				RemovalRules: []actions.RemovalRule{
					{FieldPath: "efficiency"},
				},
			},
		}
		originalBody := `{
			"records": [
				{
					"name": "vol1",
					"size": 1073741824,
					"efficiency": "sensitive1",
					"space": {
						"logical_space": {
							"enforcement": false
						}
					}
				},
				{
					"name": "vol2",
					"size": 2147483648,
					"efficiency": "sensitive2",
					"space": {
						"logical_space": {
							"enforcement": false
						}
					}
				}
			]
		}`
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(originalBody)),
			Header:     make(http.Header),
		}

		err := volumeAction.ProcessResponse(resp)
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
		assert.Equal(t, float64(1073741824), record1["size"])
		assert.NotContains(t, record1, "efficiency")

		space1, ok := record1["space"].(map[string]interface{})
		assert.True(t, ok)
		logicalSpace1, ok := space1["logical_space"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, true, logicalSpace1["enforcement"])

		// Check second record
		record2, ok := records[1].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "vol2", record2["name"])
		assert.Equal(t, float64(2147483648), record2["size"])
		assert.NotContains(t, record2, "efficiency")

		space2, ok := record2["space"].(map[string]interface{})
		assert.True(t, ok)
		logicalSpace2, ok := space2["logical_space"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, true, logicalSpace2["enforcement"])
	})

	t.Run("WhenInvalidJSON_ShouldReturnError", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
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

		err := volumeAction.ProcessResponse(resp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode response")
	})
}

func TestVolumeAction_EdgeCases(t *testing.T) {
	t.Run("WhenEmptyRequestBody_ShouldHandleGracefully", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "name", Required: true},
				},
			},
		}
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(`{}`))

		result, err := volumeAction.ShouldAllow(req)
		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "required field 'name' is missing")
	})

	t.Run("WhenNestedFieldDoesNotExist_ShouldHandleGracefully", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "space.logical_space.enforcement", Required: true},
				},
			},
		}
		reqBody := `{"space": {"logical_space": {}}}`
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

		result, err := volumeAction.ShouldAllow(req)
		assert.Error(t, err)
		assert.False(t, result)
		assert.Contains(t, err.Error(), "required field 'space.logical_space.enforcement' is missing")
	})

	t.Run("WhenInjectionIntoNonExistentPath_ShouldCreatePath", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "space.logical_space.enforcement", Value: true},
				},
			},
		}
		reqBody := `{"name": "test-volume"}`
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))
		w := httptest.NewRecorder()

		err := volumeAction.ProcessRequest(req, w)
		assert.NoError(t, err)

		// Verify the request body was modified
		body, _ := io.ReadAll(req.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		assert.Equal(t, "test-volume", result["name"])

		space, ok := result["space"].(map[string]interface{})
		assert.True(t, ok)
		logicalSpace, ok := space["logical_space"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, true, logicalSpace["enforcement"])
	})

	t.Run("WhenMultipleValidationRules_ShouldValidateAll", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			RequestRule: actions.RequestRule{
				ValidationRules: []actions.ValidationRule{
					{FieldPath: "name", Required: true},
					{FieldPath: "size", Required: true},
					{FieldPath: "guarantee.type", Values: []interface{}{"none", "volume"}},
					{FieldPath: "is_svm_root", Values: []interface{}{true}},
					{FieldPath: "space.logical_space.enforcement", Values: []interface{}{true}},
				},
			},
		}
		reqBody := `{
			"name": "test-volume",
			"size": 1073741824,
			"guarantee": {
				"type": "volume"
			},
			"is_svm_root": true,
			"space": {
				"logical_space": {
					"enforcement": true
				}
			}
		}`
		req, _ := http.NewRequest("POST", "/api/storage/volumes", strings.NewReader(reqBody))

		result, err := volumeAction.ShouldAllow(req)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("WhenComplexResponseProcessing_ShouldWork", func(t *testing.T) {
		volumeAction := &VolumeAction{
			Name: "Test Volume Action",
			ResponseRule: actions.ResponseRule{
				InjectionRules: []actions.InjectionRule{
					{FieldPath: "space.logical_space.enforcement", Value: true},
					{FieldPath: "is_svm_root", Value: true},
				},
				RemovalRules: []actions.RemovalRule{
					{FieldPath: "efficiency"},
					{FieldPath: "space.physical_used"},
					{FieldPath: "space.physical_used_percent"},
				},
			},
		}
		originalBody := `{
			"name": "test-volume",
			"size": 1073741824,
			"efficiency": "sensitive-data",
			"is_svm_root": false,
			"space": {
				"logical_space": {
					"enforcement": false,
					"size": 1073741824
				},
				"physical_used": "sensitive-physical-data",
				"physical_used_percent": "sensitive-percent-data"
			}
		}`
		resp := &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(originalBody)),
			Header:     make(http.Header),
		}

		err := volumeAction.ProcessResponse(resp)
		assert.NoError(t, err)

		// Verify the response body was modified
		body, _ := io.ReadAll(resp.Body)
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		assert.NoError(t, err)

		assert.Equal(t, "test-volume", result["name"])
		assert.Equal(t, float64(1073741824), result["size"])
		assert.Equal(t, true, result["is_svm_root"]) // Should be injected
		assert.NotContains(t, result, "efficiency")  // Should be removed

		space, ok := result["space"].(map[string]interface{})
		assert.True(t, ok)
		assert.NotContains(t, space, "physical_used")         // Should be removed
		assert.NotContains(t, space, "physical_used_percent") // Should be removed

		logicalSpace, ok := space["logical_space"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, true, logicalSpace["enforcement"])         // Should be injected
		assert.Equal(t, float64(1073741824), logicalSpace["size"]) // Should remain unchanged
	})
}
