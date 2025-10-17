package processor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
)

// ApplyValidationRules applies all validation rules to the request body
func ApplyValidationRules(requestBody map[string]interface{}, validationRules []actions.ValidationRule) error {
	for _, rule := range validationRules {
		if rule.Required {
			if !validateRequiredFields(requestBody, rule.FieldPath) {
				return fmt.Errorf("required field '%s' is missing", rule.FieldPath)
			}
		}

		if len(rule.Values) > 0 {
			if !validateAllowedValues(requestBody, rule.FieldPath, rule.Values) {
				return fmt.Errorf("field '%s' has invalid value, allowed values: %v", rule.FieldPath, rule.Values)
			}
		}
	}
	return nil
}

func ApplyResponseRules(responseData map[string]interface{}, injectionRules []actions.InjectionRule, removalRules []actions.RemovalRule) {
	if records, ok := responseData["records"].([]interface{}); ok {
		for _, record := range records {
			if recordMap, ok := record.(map[string]interface{}); ok {
				// Apply injection rules
				for _, rule := range injectionRules {
					injectField(recordMap, rule.FieldPath, rule.Value)
				}
				// Apply removal rules
				for _, rule := range removalRules {
					removeField(recordMap, rule.FieldPath)
				}
			}
		}
	} else {
		for _, rule := range injectionRules {
			injectField(responseData, rule.FieldPath, rule.Value)
		}
		for _, rule := range removalRules {
			removeField(responseData, rule.FieldPath)
		}
	}
}

// UnmarshalRequestBody reads and unmarshals the request body
func UnmarshalRequestBody(r *http.Request) (map[string]interface{}, error) {
	if r == nil {
		return nil, fmt.Errorf("request is nil")
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	// Restore body for later use
	r.Body = io.NopCloser(bytes.NewReader(body))

	var requestBody map[string]interface{}
	if err := json.Unmarshal(body, &requestBody); err != nil {
		return nil, fmt.Errorf("invalid JSON in request body: %w", err)
	}
	return requestBody, nil
}

// MarshalAndRestoreBody marshals the request body and restores it to the request
func MarshalAndRestoreBody(r *http.Request, w http.ResponseWriter, requestBody map[string]interface{}) error {
	if r == nil {
		return fmt.Errorf("request is nil")
	}

	newBody, err := json.Marshal(requestBody)
	if err != nil {
		http.Error(w, "Failed to marshal request body", http.StatusInternalServerError)
		return fmt.Errorf("failed to marshal request body: %w", err)
	}
	r.Body = io.NopCloser(bytes.NewReader(newBody))
	r.ContentLength = int64(len(newBody))
	return nil
}

// UnmarshalResponseBody reads and unmarshals the response body
func UnmarshalResponseBody(resp *http.Response) (map[string]interface{}, error) {
	if resp == nil {
		return nil, fmt.Errorf("response is nil")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var responseData map[string]interface{}
	if err := json.Unmarshal(body, &responseData); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	return responseData, nil
}

// MarshalAndRestoreResponse marshals the response data and restores it to the response
func MarshalAndRestoreResponse(resp *http.Response, responseData map[string]interface{}) error {
	if resp == nil {
		return fmt.Errorf("response is nil")
	}

	newBody, err := json.Marshal(responseData)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}
	resp.Body = io.NopCloser(bytes.NewReader(newBody))
	resp.ContentLength = int64(len(newBody))
	resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(newBody)))
	return nil
}

// validateRequiredFields validates if a field exists in a generic map structure
func validateRequiredFields(data map[string]interface{}, fieldPath string) bool {
	parts := strings.Split(fieldPath, ".")
	current := data

	for i, part := range parts {
		value, exists := current[part]
		if !exists {
			return false
		}

		if i == len(parts)-1 {
			return true
		}

		if nested, ok := value.(map[string]interface{}); ok {
			current = nested
		} else {
			return false
		}
	}

	return true
}

// validateAllowedValues checks if a field value is in the allowed values list
func validateAllowedValues(data map[string]interface{}, fieldPath string, allowedValues []interface{}) bool {
	parts := strings.Split(fieldPath, ".")
	current := data

	for i, part := range parts {
		value, exists := current[part]
		if !exists {
			return true
		}

		if i == len(parts)-1 {
			allowedMap := make(map[interface{}]bool)
			for _, allowed := range allowedValues {
				allowedMap[allowed] = true
			}
			return allowedMap[value]
		}
		if nested, ok := value.(map[string]interface{}); ok {
			current = nested
		} else {
			return false
		}
	}

	return true
}

// injectField injects a value into a generic map structure
func injectField(data map[string]interface{}, fieldPath string, value interface{}) {
	parts := strings.Split(fieldPath, ".")
	current := data

	for i, part := range parts {
		if i == len(parts)-1 {
			current[part] = value
			return
		}
		if nested, exists := current[part]; exists {
			if nestedMap, ok := nested.(map[string]interface{}); ok {
				current = nestedMap
			} else {
				return
			}
		} else {
			newMap := make(map[string]interface{})
			current[part] = newMap
			current = newMap
		}
	}
}

// removeField removes a field from a generic map structure
func removeField(data map[string]interface{}, fieldPath string) {
	parts := strings.Split(fieldPath, ".")
	current := data

	for i, part := range parts {
		if i == len(parts)-1 {
			delete(current, part)
			return
		}
		if nested, exists := current[part]; exists {
			if nestedMap, ok := nested.(map[string]interface{}); ok {
				current = nestedMap
			} else {
				return
			}
		} else {
			return
		}
	}
}
