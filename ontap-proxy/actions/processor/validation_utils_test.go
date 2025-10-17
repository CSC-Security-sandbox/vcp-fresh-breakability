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

func TestApplyValidationRules(t *testing.T) {
	t.Run("WhenNoRules_ShouldReturnNil", func(t *testing.T) {
		requestBody := map[string]interface{}{
			"name": "test-volume",
			"size": 1073741824,
		}

		err := ApplyValidationRules(requestBody, []actions.ValidationRule{})
		assert.NoError(t, err, "Should return nil when no validation rules")
	})

	t.Run("WhenRequiredFieldExists_ShouldReturnNil", func(t *testing.T) {
		requestBody := map[string]interface{}{
			"name": "test-volume",
			"size": 1073741824,
		}

		rules := []actions.ValidationRule{
			{FieldPath: "name", Required: true},
			{FieldPath: "size", Required: true},
		}

		err := ApplyValidationRules(requestBody, rules)
		assert.NoError(t, err, "Should return nil when required fields exist")
	})

	t.Run("WhenRequiredFieldMissing_ShouldReturnError", func(t *testing.T) {
		requestBody := map[string]interface{}{
			"name": "test-volume",
		}

		rules := []actions.ValidationRule{
			{FieldPath: "size", Required: true},
		}

		err := ApplyValidationRules(requestBody, rules)
		assert.Error(t, err, "Should return error when required field is missing")
		assert.Contains(t, err.Error(), "required field 'size' is missing", "Error should mention missing field")
	})

	t.Run("WhenNestedRequiredFieldExists_ShouldReturnNil", func(t *testing.T) {
		requestBody := map[string]interface{}{
			"space": map[string]interface{}{
				"logical_space": map[string]interface{}{
					"enforcement": true,
				},
			},
		}

		rules := []actions.ValidationRule{
			{FieldPath: "space.logical_space.enforcement", Required: true},
		}

		err := ApplyValidationRules(requestBody, rules)
		assert.NoError(t, err, "Should return nil when nested required field exists")
	})

	t.Run("WhenNestedRequiredFieldMissing_ShouldReturnError", func(t *testing.T) {
		requestBody := map[string]interface{}{
			"space": map[string]interface{}{
				"logical_space": map[string]interface{}{},
			},
		}

		rules := []actions.ValidationRule{
			{FieldPath: "space.logical_space.enforcement", Required: true},
		}

		err := ApplyValidationRules(requestBody, rules)
		assert.Error(t, err, "Should return error when nested required field is missing")
		assert.Contains(t, err.Error(), "required field 'space.logical_space.enforcement' is missing", "Error should mention missing nested field")
	})

	t.Run("WhenFieldValueAllowed_ShouldReturnNil", func(t *testing.T) {
		requestBody := map[string]interface{}{
			"guarantee": map[string]interface{}{
				"type": "none",
			},
		}

		rules := []actions.ValidationRule{
			{FieldPath: "guarantee.type", Values: []interface{}{"none", "volume"}},
		}

		err := ApplyValidationRules(requestBody, rules)
		assert.NoError(t, err, "Should return nil when field value is allowed")
	})

	t.Run("WhenFieldValueNotAllowed_ShouldReturnError", func(t *testing.T) {
		requestBody := map[string]interface{}{
			"guarantee": map[string]interface{}{
				"type": "invalid",
			},
		}

		rules := []actions.ValidationRule{
			{FieldPath: "guarantee.type", Values: []interface{}{"none", "volume"}},
		}

		err := ApplyValidationRules(requestBody, rules)
		assert.Error(t, err, "Should return error when field value is not allowed")
		assert.Contains(t, err.Error(), "field 'guarantee.type' has invalid value", "Error should mention invalid value")
		assert.Contains(t, err.Error(), "[none volume]", "Error should mention allowed values")
	})

	t.Run("WhenFieldNotExists_ShouldReturnNilForValues", func(t *testing.T) {
		requestBody := map[string]interface{}{
			"name": "test-volume",
		}

		rules := []actions.ValidationRule{
			{FieldPath: "guarantee.type", Values: []interface{}{"none", "volume"}},
		}

		err := ApplyValidationRules(requestBody, rules)
		assert.NoError(t, err, "Should return nil when field doesn't exist (values validation is optional)")
	})

	t.Run("WhenMultipleRules_ShouldValidateAll", func(t *testing.T) {
		requestBody := map[string]interface{}{
			"name": "test-volume",
			"size": 1073741824,
			"guarantee": map[string]interface{}{
				"type": "none",
			},
			"space": map[string]interface{}{
				"logical_space": map[string]interface{}{
					"enforcement": true,
				},
			},
		}

		rules := []actions.ValidationRule{
			{FieldPath: "name", Required: true},
			{FieldPath: "size", Required: true},
			{FieldPath: "guarantee.type", Values: []interface{}{"none", "volume"}},
			{FieldPath: "space.logical_space.enforcement", Required: true},
		}

		err := ApplyValidationRules(requestBody, rules)
		assert.NoError(t, err, "Should return nil when all rules pass")
	})

	t.Run("WhenOneRuleFails_ShouldReturnFirstError", func(t *testing.T) {
		requestBody := map[string]interface{}{
			"name": "test-volume",
			// size is missing
			"guarantee": map[string]interface{}{
				"type": "invalid", // invalid value
			},
		}

		rules := []actions.ValidationRule{
			{FieldPath: "name", Required: true},
			{FieldPath: "size", Required: true}, // This should fail first
			{FieldPath: "guarantee.type", Values: []interface{}{"none", "volume"}},
		}

		err := ApplyValidationRules(requestBody, rules)
		assert.Error(t, err, "Should return error when any rule fails")
		assert.Contains(t, err.Error(), "required field 'size' is missing", "Should return first error")
	})
}

func TestApplyResponseRules(t *testing.T) {
	t.Run("WhenNoRules_ShouldNotModifyData", func(t *testing.T) {
		responseData := map[string]interface{}{
			"name": "test-volume",
			"size": 1073741824,
		}

		originalData := map[string]interface{}{
			"name": "test-volume",
			"size": 1073741824,
		}

		ApplyResponseRules(responseData, []actions.InjectionRule{}, []actions.RemovalRule{})

		assert.Equal(t, originalData, responseData, "Data should not be modified when no rules")
	})

	t.Run("WhenSingleRecord_ShouldApplyInjectionRules", func(t *testing.T) {
		responseData := map[string]interface{}{
			"name": "test-volume",
			"size": 1073741824,
		}

		injectionRules := []actions.InjectionRule{
			{FieldPath: "space.logical_space.enforcement", Value: true},
		}

		ApplyResponseRules(responseData, injectionRules, []actions.RemovalRule{})

		expected := map[string]interface{}{
			"name": "test-volume",
			"size": 1073741824,
			"space": map[string]interface{}{
				"logical_space": map[string]interface{}{
					"enforcement": true,
				},
			},
		}

		assert.Equal(t, expected, responseData, "Should inject fields into single record")
	})

	t.Run("WhenSingleRecord_ShouldApplyRemovalRules", func(t *testing.T) {
		responseData := map[string]interface{}{
			"name":       "test-volume",
			"size":       1073741824,
			"efficiency": 0.8,
			"space": map[string]interface{}{
				"physical_used": 536870912,
			},
		}

		removalRules := []actions.RemovalRule{
			{FieldPath: "efficiency"},
			{FieldPath: "space.physical_used"},
		}

		ApplyResponseRules(responseData, []actions.InjectionRule{}, removalRules)

		expected := map[string]interface{}{
			"name":  "test-volume",
			"size":  1073741824,
			"space": map[string]interface{}{
				// physical_used should be removed
			},
		}

		assert.Equal(t, expected, responseData, "Should remove fields from single record")
	})

	t.Run("WhenRecordsArray_ShouldApplyRulesToEachRecord", func(t *testing.T) {
		responseData := map[string]interface{}{
			"records": []interface{}{
				map[string]interface{}{
					"name":       "volume1",
					"efficiency": 0.8,
				},
				map[string]interface{}{
					"name":       "volume2",
					"efficiency": 0.9,
				},
			},
		}

		injectionRules := []actions.InjectionRule{
			{FieldPath: "space.logical_space.enforcement", Value: true},
		}
		removalRules := []actions.RemovalRule{
			{FieldPath: "efficiency"},
		}

		ApplyResponseRules(responseData, injectionRules, removalRules)

		expected := map[string]interface{}{
			"records": []interface{}{
				map[string]interface{}{
					"name": "volume1",
					"space": map[string]interface{}{
						"logical_space": map[string]interface{}{
							"enforcement": true,
						},
					},
				},
				map[string]interface{}{
					"name": "volume2",
					"space": map[string]interface{}{
						"logical_space": map[string]interface{}{
							"enforcement": true,
						},
					},
				},
			},
		}

		assert.Equal(t, expected, responseData, "Should apply rules to each record in array")
	})

	t.Run("WhenRecordsArrayWithNonMapRecords_ShouldSkipNonMapRecords", func(t *testing.T) {
		responseData := map[string]interface{}{
			"records": []interface{}{
				map[string]interface{}{
					"name": "volume1",
				},
				"invalid-record", // Non-map record
				map[string]interface{}{
					"name": "volume2",
				},
			},
		}

		removalRules := []actions.RemovalRule{
			{FieldPath: "efficiency"},
		}

		// Should not panic and should process valid records
		assert.NotPanics(t, func() {
			ApplyResponseRules(responseData, []actions.InjectionRule{}, removalRules)
		}, "Should not panic with non-map records")
	})

	t.Run("WhenBothInjectionAndRemovalRules_ShouldApplyBoth", func(t *testing.T) {
		responseData := map[string]interface{}{
			"name":       "test-volume",
			"efficiency": 0.8,
		}

		injectionRules := []actions.InjectionRule{
			{FieldPath: "space.logical_space.enforcement", Value: true},
		}
		removalRules := []actions.RemovalRule{
			{FieldPath: "efficiency"},
		}

		ApplyResponseRules(responseData, injectionRules, removalRules)

		expected := map[string]interface{}{
			"name": "test-volume",
			"space": map[string]interface{}{
				"logical_space": map[string]interface{}{
					"enforcement": true,
				},
			},
		}

		assert.Equal(t, expected, responseData, "Should apply both injection and removal rules")
	})
}

func TestUnmarshalRequestBody(t *testing.T) {
	t.Run("WhenValidJSON_ShouldReturnParsedData", func(t *testing.T) {
		jsonData := `{"name": "test-volume", "size": 1073741824}`
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(jsonData))

		result, err := UnmarshalRequestBody(req)

		assert.NoError(t, err, "Should not return error for valid JSON")
		assert.Equal(t, "test-volume", result["name"], "Should parse name correctly")
		assert.Equal(t, float64(1073741824), result["size"], "Should parse size correctly")
	})

	t.Run("WhenInvalidJSON_ShouldReturnError", func(t *testing.T) {
		invalidJSON := `{"name": "test-volume", "size":}`
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(invalidJSON))

		result, err := UnmarshalRequestBody(req)

		assert.Error(t, err, "Should return error for invalid JSON")
		assert.Nil(t, result, "Result should be nil when error occurs")
		assert.Contains(t, err.Error(), "invalid JSON in request body", "Error should mention JSON validation")
	})

	t.Run("WhenEmptyBody_ShouldReturnEmptyMap", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader("{}"))

		result, err := UnmarshalRequestBody(req)

		assert.NoError(t, err, "Should not return error for empty JSON object")
		assert.NotNil(t, result, "Result should not be nil")
		assert.Empty(t, result, "Result should be empty map")
	})

	t.Run("WhenNestedJSON_ShouldParseCorrectly", func(t *testing.T) {
		jsonData := `{
			"name": "test-volume",
			"space": {
				"logical_space": {
					"enforcement": true
				}
			}
		}`
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(jsonData))

		result, err := UnmarshalRequestBody(req)

		assert.NoError(t, err, "Should not return error for nested JSON")

		space, ok := result["space"].(map[string]interface{})
		assert.True(t, ok, "Should parse nested space object")

		logicalSpace, ok := space["logical_space"].(map[string]interface{})
		assert.True(t, ok, "Should parse nested logical_space object")

		assert.Equal(t, true, logicalSpace["enforcement"], "Should parse nested enforcement value")
	})

	t.Run("WhenBodyIsRestored_ShouldBeReadableAgain", func(t *testing.T) {
		jsonData := `{"name": "test-volume"}`
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(jsonData))

		// First unmarshal
		result1, err1 := UnmarshalRequestBody(req)
		assert.NoError(t, err1, "First unmarshal should succeed")

		// Second unmarshal (should work because body was restored)
		result2, err2 := UnmarshalRequestBody(req)
		assert.NoError(t, err2, "Second unmarshal should succeed")

		assert.Equal(t, result1, result2, "Both unmarshals should return same data")
	})

	t.Run("WhenNilRequest_ShouldReturnError", func(t *testing.T) {
		result, err := UnmarshalRequestBody(nil)

		assert.Error(t, err, "Should return error for nil request")
		assert.Nil(t, result, "Result should be nil when error occurs")
		assert.Contains(t, err.Error(), "request is nil", "Error should mention nil request")
	})
}

func TestMarshalAndRestoreBody(t *testing.T) {
	t.Run("WhenValidData_ShouldMarshalAndRestore", func(t *testing.T) {
		originalData := map[string]interface{}{
			"name": "test-volume",
			"size": 1073741824,
		}

		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"original": "data"}`))
		w := httptest.NewRecorder()

		err := MarshalAndRestoreBody(req, w, originalData)

		assert.NoError(t, err, "Should not return error for valid data")

		// Verify body was restored
		body, err := io.ReadAll(req.Body)
		assert.NoError(t, err, "Should be able to read restored body")

		var restoredData map[string]interface{}
		err = json.Unmarshal(body, &restoredData)
		assert.NoError(t, err, "Should be able to unmarshal restored body")
		assert.Equal(t, originalData["name"], restoredData["name"], "Restored name should match original")
		assert.Equal(t, float64(originalData["size"].(int)), restoredData["size"], "Restored size should match original (as float64)")
	})

	t.Run("WhenInvalidData_ShouldReturnError", func(t *testing.T) {
		// Create data that can't be marshaled (circular reference)
		invalidData := make(map[string]interface{})
		invalidData["self"] = invalidData // Circular reference

		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{}`))
		w := httptest.NewRecorder()

		err := MarshalAndRestoreBody(req, w, invalidData)

		assert.Error(t, err, "Should return error for invalid data")
		assert.Contains(t, err.Error(), "failed to marshal request body", "Error should mention marshaling")
	})

	t.Run("WhenNilRequest_ShouldReturnError", func(t *testing.T) {
		data := map[string]interface{}{"test": "data"}
		w := httptest.NewRecorder()

		err := MarshalAndRestoreBody(nil, w, data)

		assert.Error(t, err, "Should return error for nil request")
		assert.Contains(t, err.Error(), "request is nil", "Error should mention nil request")
	})

	t.Run("WhenNilResponseWriter_ShouldStillWork", func(t *testing.T) {
		data := map[string]interface{}{"test": "data"}
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{}`))

		err := MarshalAndRestoreBody(req, nil, data)

		assert.NoError(t, err, "Should not return error even with nil response writer")
	})
}

func TestUnmarshalResponseBody(t *testing.T) {
	t.Run("WhenValidJSON_ShouldReturnParsedData", func(t *testing.T) {
		jsonData := `{"name": "test-volume", "size": 1073741824}`
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(jsonData)),
		}

		result, err := UnmarshalResponseBody(resp)

		assert.NoError(t, err, "Should not return error for valid JSON")
		assert.Equal(t, "test-volume", result["name"], "Should parse name correctly")
		assert.Equal(t, float64(1073741824), result["size"], "Should parse size correctly")
	})

	t.Run("WhenInvalidJSON_ShouldReturnError", func(t *testing.T) {
		invalidJSON := `{"name": "test-volume", "size":}`
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(invalidJSON)),
		}

		result, err := UnmarshalResponseBody(resp)

		assert.Error(t, err, "Should return error for invalid JSON")
		assert.Nil(t, result, "Result should be nil when error occurs")
		assert.Contains(t, err.Error(), "failed to decode response", "Error should mention response decoding")
	})

	t.Run("WhenEmptyBody_ShouldReturnEmptyMap", func(t *testing.T) {
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader("{}")),
		}

		result, err := UnmarshalResponseBody(resp)

		assert.NoError(t, err, "Should not return error for empty JSON object")
		assert.NotNil(t, result, "Result should not be nil")
		assert.Empty(t, result, "Result should be empty map")
	})

	t.Run("WhenNilResponse_ShouldReturnError", func(t *testing.T) {
		result, err := UnmarshalResponseBody(nil)

		assert.Error(t, err, "Should return error for nil response")
		assert.Nil(t, result, "Result should be nil when error occurs")
		assert.Contains(t, err.Error(), "response is nil", "Error should mention nil response")
	})
}

func TestMarshalAndRestoreResponse(t *testing.T) {
	t.Run("WhenValidData_ShouldMarshalAndRestore", func(t *testing.T) {
		originalData := map[string]interface{}{
			"name": "test-volume",
			"size": 1073741824,
		}

		resp := &http.Response{
			Body:       io.NopCloser(strings.NewReader(`{"original": "data"}`)),
			StatusCode: 200,
			Header:     make(http.Header),
		}

		err := MarshalAndRestoreResponse(resp, originalData)

		assert.NoError(t, err, "Should not return error for valid data")

		// Verify body was restored
		body, err := io.ReadAll(resp.Body)
		assert.NoError(t, err, "Should be able to read restored body")

		var restoredData map[string]interface{}
		err = json.Unmarshal(body, &restoredData)
		assert.NoError(t, err, "Should be able to unmarshal restored body")
		assert.Equal(t, originalData["name"], restoredData["name"], "Restored name should match original")
		assert.Equal(t, float64(originalData["size"].(int)), restoredData["size"], "Restored size should match original (as float64)")

		// Verify Content-Length header was set
		assert.Equal(t, "40", resp.Header.Get("Content-Length"), "Content-Length header should be set")
	})

	t.Run("WhenInvalidData_ShouldReturnError", func(t *testing.T) {
		// Create data that can't be marshaled (circular reference)
		invalidData := make(map[string]interface{})
		invalidData["self"] = invalidData // Circular reference

		resp := &http.Response{
			Body:       io.NopCloser(strings.NewReader(`{}`)),
			StatusCode: 200,
			Header:     make(http.Header),
		}

		err := MarshalAndRestoreResponse(resp, invalidData)

		assert.Error(t, err, "Should return error for invalid data")
		assert.Contains(t, err.Error(), "failed to marshal response", "Error should mention marshaling")
	})

	t.Run("WhenNilResponse_ShouldReturnError", func(t *testing.T) {
		data := map[string]interface{}{"test": "data"}

		err := MarshalAndRestoreResponse(nil, data)

		assert.Error(t, err, "Should return error for nil response")
		assert.Contains(t, err.Error(), "response is nil", "Error should mention nil response")
	})

	t.Run("WhenNilHeader_ShouldPanic", func(t *testing.T) {
		data := map[string]interface{}{"test": "data"}
		resp := &http.Response{
			Body:       io.NopCloser(strings.NewReader(`{}`)),
			StatusCode: 200,
			Header:     nil, // This should cause a panic
		}

		assert.Panics(t, func() {
			_ = MarshalAndRestoreResponse(resp, data)
		}, "Should panic when Header is nil")
	})
}

func TestValidateRequiredFields(t *testing.T) {
	t.Run("WhenFieldExists_ShouldReturnTrue", func(t *testing.T) {
		data := map[string]interface{}{
			"name": "test-volume",
		}

		result := validateRequiredFields(data, "name")
		assert.True(t, result, "Should return true when field exists")
	})

	t.Run("WhenFieldMissing_ShouldReturnFalse", func(t *testing.T) {
		data := map[string]interface{}{
			"name": "test-volume",
		}

		result := validateRequiredFields(data, "size")
		assert.False(t, result, "Should return false when field is missing")
	})

	t.Run("WhenNestedFieldExists_ShouldReturnTrue", func(t *testing.T) {
		data := map[string]interface{}{
			"space": map[string]interface{}{
				"logical_space": map[string]interface{}{
					"enforcement": true,
				},
			},
		}

		result := validateRequiredFields(data, "space.logical_space.enforcement")
		assert.True(t, result, "Should return true when nested field exists")
	})

	t.Run("WhenNestedFieldMissing_ShouldReturnFalse", func(t *testing.T) {
		data := map[string]interface{}{
			"space": map[string]interface{}{
				"logical_space": map[string]interface{}{},
			},
		}

		result := validateRequiredFields(data, "space.logical_space.enforcement")
		assert.False(t, result, "Should return false when nested field is missing")
	})

	t.Run("WhenIntermediatePathMissing_ShouldReturnFalse", func(t *testing.T) {
		data := map[string]interface{}{
			"name": "test-volume",
		}

		result := validateRequiredFields(data, "space.logical_space.enforcement")
		assert.False(t, result, "Should return false when intermediate path is missing")
	})

	t.Run("WhenNonMapInPath_ShouldReturnFalse", func(t *testing.T) {
		data := map[string]interface{}{
			"space": "not-a-map",
		}

		result := validateRequiredFields(data, "space.logical_space.enforcement")
		assert.False(t, result, "Should return false when intermediate value is not a map")
	})
}

func TestValidateAllowedValues(t *testing.T) {
	t.Run("WhenValueIsAllowed_ShouldReturnTrue", func(t *testing.T) {
		data := map[string]interface{}{
			"guarantee": map[string]interface{}{
				"type": "none",
			},
		}

		allowedValues := []interface{}{"none", "volume"}
		result := validateAllowedValues(data, "guarantee.type", allowedValues)
		assert.True(t, result, "Should return true when value is allowed")
	})

	t.Run("WhenValueNotAllowed_ShouldReturnFalse", func(t *testing.T) {
		data := map[string]interface{}{
			"guarantee": map[string]interface{}{
				"type": "invalid",
			},
		}

		allowedValues := []interface{}{"none", "volume"}
		result := validateAllowedValues(data, "guarantee.type", allowedValues)
		assert.False(t, result, "Should return false when value is not allowed")
	})

	t.Run("WhenFieldNotExists_ShouldReturnTrue", func(t *testing.T) {
		data := map[string]interface{}{
			"name": "test-volume",
		}

		allowedValues := []interface{}{"none", "volume"}
		result := validateAllowedValues(data, "guarantee.type", allowedValues)
		assert.True(t, result, "Should return true when field doesn't exist (optional validation)")
	})

	t.Run("WhenNestedFieldValueIsAllowed_ShouldReturnTrue", func(t *testing.T) {
		data := map[string]interface{}{
			"space": map[string]interface{}{
				"logical_space": map[string]interface{}{
					"enforcement": true,
				},
			},
		}

		allowedValues := []interface{}{true, false}
		result := validateAllowedValues(data, "space.logical_space.enforcement", allowedValues)
		assert.True(t, result, "Should return true when nested field value is allowed")
	})
}

func TestInjectField(t *testing.T) {
	t.Run("WhenInjectingTopLevelField_ShouldSetValue", func(t *testing.T) {
		data := map[string]interface{}{
			"name": "test-volume",
		}

		injectField(data, "size", 1073741824)

		assert.Equal(t, 1073741824, data["size"], "Should inject top-level field")
	})

	t.Run("WhenInjectingNestedField_ShouldCreatePath", func(t *testing.T) {
		data := map[string]interface{}{
			"name": "test-volume",
		}

		injectField(data, "space.logical_space.enforcement", true)

		expected := map[string]interface{}{
			"name": "test-volume",
			"space": map[string]interface{}{
				"logical_space": map[string]interface{}{
					"enforcement": true,
				},
			},
		}

		assert.Equal(t, expected, data, "Should create nested path and inject value")
	})

	t.Run("WhenInjectingIntoExistingNestedPath_ShouldUseExistingPath", func(t *testing.T) {
		data := map[string]interface{}{
			"space": map[string]interface{}{
				"logical_space": map[string]interface{}{
					"reporting": true,
				},
			},
		}

		injectField(data, "space.logical_space.enforcement", true)

		logicalSpace := data["space"].(map[string]interface{})["logical_space"].(map[string]interface{})
		assert.Equal(t, true, logicalSpace["enforcement"], "Should inject into existing nested path")
		assert.Equal(t, true, logicalSpace["reporting"], "Should preserve existing values")
	})

	t.Run("WhenInjectingIntoNonMapValue_ShouldNotModify", func(t *testing.T) {
		data := map[string]interface{}{
			"space": "not-a-map",
		}

		injectField(data, "space.logical_space.enforcement", true)

		assert.Equal(t, "not-a-map", data["space"], "Should not modify when intermediate value is not a map")
	})
}

func TestRemoveField(t *testing.T) {
	t.Run("WhenRemovingTopLevelField_ShouldDeleteField", func(t *testing.T) {
		data := map[string]interface{}{
			"name":       "test-volume",
			"efficiency": 0.8,
		}

		removeField(data, "efficiency")

		expected := map[string]interface{}{
			"name": "test-volume",
		}

		assert.Equal(t, expected, data, "Should remove top-level field")
	})

	t.Run("WhenRemovingNestedField_ShouldDeleteNestedField", func(t *testing.T) {
		data := map[string]interface{}{
			"name": "test-volume",
			"space": map[string]interface{}{
				"logical_space": map[string]interface{}{
					"enforcement": true,
					"reporting":   true,
				},
			},
		}

		removeField(data, "space.logical_space.enforcement")

		logicalSpace := data["space"].(map[string]interface{})["logical_space"].(map[string]interface{})
		assert.NotContains(t, logicalSpace, "enforcement", "Should remove nested field")
		assert.Equal(t, true, logicalSpace["reporting"], "Should preserve other fields")
	})

	t.Run("WhenRemovingNonExistentField_ShouldNotError", func(t *testing.T) {
		data := map[string]interface{}{
			"name": "test-volume",
		}

		// Should not panic
		assert.NotPanics(t, func() {
			removeField(data, "non-existent-field")
		}, "Should not panic when removing non-existent field")
	})

	t.Run("WhenRemovingFromNonMapValue_ShouldNotModify", func(t *testing.T) {
		data := map[string]interface{}{
			"space": "not-a-map",
		}

		removeField(data, "space.logical_space.enforcement")

		assert.Equal(t, "not-a-map", data["space"], "Should not modify when intermediate value is not a map")
	})

	t.Run("WhenRemovingFromMissingPath_ShouldNotError", func(t *testing.T) {
		data := map[string]interface{}{
			"name": "test-volume",
		}

		// Should not panic
		assert.NotPanics(t, func() {
			removeField(data, "space.logical_space.enforcement")
		}, "Should not panic when removing from missing path")
	})
}
