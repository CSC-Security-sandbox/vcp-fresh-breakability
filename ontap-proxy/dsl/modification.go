package dsl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Modification defines the interface for request/response modifications.
// The Apply method receives either an *http.Request or *http.Response.
type Modification interface {
	Apply(target interface{}) error
}

// RequestModification is an embedded type that marks modifications as request-only.
type RequestModification struct{}

// ResponseModification is an embedded type that marks modifications as response-only.
type ResponseModification struct{}

// NoModification is a no-op modification that does nothing.
// Useful as a placeholder when no modification is needed.
type NoModification struct{}

func (n NoModification) Apply(target interface{}) error {
	return nil
}

// SetHeaders sets or overwrites HTTP headers on the request.
type SetHeaders struct {
	RequestModification
	Headers map[string]string
}

func (s SetHeaders) Apply(target interface{}) error {
	req, ok := target.(*http.Request)
	if !ok {
		return fmt.Errorf("SetHeaders: expected *http.Request, got %T", target)
	}

	for key, value := range s.Headers {
		req.Header.Set(key, value)
	}
	return nil
}

// SetQueryParams adds or modifies query parameters on the request.
type SetQueryParams struct {
	RequestModification
	Params map[string]string
}

func (s SetQueryParams) Apply(target interface{}) error {
	req, ok := target.(*http.Request)
	if !ok {
		return fmt.Errorf("SetQueryParams: expected *http.Request, got %T", target)
	}

	query := req.URL.Query()
	for key, value := range s.Params {
		query.Set(key, value)
	}
	req.URL.RawQuery = query.Encode()
	return nil
}

// SetRequestFields modifies fields in a JSON request body.
// Keys are field paths (e.g., "field.nested" or "$.field.nested").
// Values are the values to set (will be parsed as JSON if valid).
type SetRequestFields struct {
	RequestModification
	Fields map[string]interface{}
}

func (s SetRequestFields) Apply(target interface{}) error {
	req, ok := target.(*http.Request)
	if !ok {
		return fmt.Errorf("SetRequestFields: expected *http.Request, got %T", target)
	}

	if req.Body == nil {
		return nil
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		return fmt.Errorf("SetRequestFields: failed to read request body: %w", err)
	}
	_ = req.Body.Close()

	if len(body) == 0 {
		req.Body = io.NopCloser(bytes.NewReader(body))
		return nil
	}

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		req.Body = io.NopCloser(bytes.NewReader(body))
		return nil
	}

	// Apply field modifications
	for fieldPath, value := range s.Fields {
		path := normalizeJSONPath(fieldPath)
		setFieldValue(data, path, value)
	}

	newBody, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("SetRequestFields: failed to marshal request body: %w", err)
	}

	req.Body = io.NopCloser(bytes.NewReader(newBody))
	req.ContentLength = int64(len(newBody))
	return nil
}

// SetFields modifies fields in a JSON response body.
// Keys are JSONPath expressions (e.g., "$.field.nested").
// Values starting with "$." are treated as JSONPath expressions (value copied from that path).
// Other values are treated as literals (parsed as JSON if valid, otherwise as strings).
type SetFields struct {
	ResponseModification
	Fields map[string]string
}

func (s SetFields) Apply(target interface{}) error {
	resp, ok := target.(*http.Response)
	if !ok {
		return fmt.Errorf("SetFields: expected *http.Response, got %T", target)
	}

	if resp.Body == nil {
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("SetFields: failed to read response body: %w", err)
	}
	_ = resp.Body.Close()

	if len(body) == 0 {
		resp.Body = io.NopCloser(bytes.NewReader(body))
		return nil
	}

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		// Not JSON, restore body and return
		resp.Body = io.NopCloser(bytes.NewReader(body))
		return nil
	}

	// Apply field modifications
	for keyPath, valuePath := range s.Fields {
		path := normalizeJSONPath(keyPath)

		var value interface{}
		if strings.HasPrefix(valuePath, "$.") {
			// Value is a JSONPath - copy from that location
			srcPath := normalizeJSONPath(valuePath)
			value = getFieldValue(data, srcPath)
		} else {
			// Value is a literal - try to parse as JSON
			if err := json.Unmarshal([]byte(valuePath), &value); err != nil {
				// Not valid JSON, use as string
				value = valuePath
			}
		}

		setFieldValue(data, path, value)
	}

	// Apply to records array if present
	if records, ok := data["records"].([]interface{}); ok {
		for _, record := range records {
			if recordMap, ok := record.(map[string]interface{}); ok {
				for keyPath, valuePath := range s.Fields {
					path := normalizeJSONPath(keyPath)

					var value interface{}
					if strings.HasPrefix(valuePath, "$.") {
						srcPath := normalizeJSONPath(valuePath)
						value = getFieldValue(recordMap, srcPath)
					} else {
						if err := json.Unmarshal([]byte(valuePath), &value); err != nil {
							value = valuePath
						}
					}

					setFieldValue(recordMap, path, value)
				}
			}
		}
	}

	newBody, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("SetFields: failed to marshal response: %w", err)
	}

	resp.Body = io.NopCloser(bytes.NewReader(newBody))
	resp.ContentLength = int64(len(newBody))
	resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(newBody)))
	return nil
}

// RemoveFields removes fields from a JSON response body.
type RemoveFields struct {
	ResponseModification
	Fields []string
}

func (r RemoveFields) Apply(target interface{}) error {
	resp, ok := target.(*http.Response)
	if !ok {
		return fmt.Errorf("RemoveFields: expected *http.Response, got %T", target)
	}

	if resp.Body == nil {
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("RemoveFields: failed to read response body: %w", err)
	}
	_ = resp.Body.Close()

	if len(body) == 0 {
		resp.Body = io.NopCloser(bytes.NewReader(body))
		return nil
	}

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		// Not JSON, restore body and return
		resp.Body = io.NopCloser(bytes.NewReader(body))
		return nil
	}

	// Remove fields
	for _, fieldPath := range r.Fields {
		path := normalizeJSONPath(fieldPath)
		removeFieldValue(data, path)
	}

	// Apply to records array if present
	if records, ok := data["records"].([]interface{}); ok {
		for _, record := range records {
			if recordMap, ok := record.(map[string]interface{}); ok {
				for _, fieldPath := range r.Fields {
					path := normalizeJSONPath(fieldPath)
					removeFieldValue(recordMap, path)
				}
			}
		}
	}

	newBody, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("RemoveFields: failed to marshal response: %w", err)
	}

	resp.Body = io.NopCloser(bytes.NewReader(newBody))
	resp.ContentLength = int64(len(newBody))
	resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(newBody)))
	return nil
}

// allOf chains multiple modifications to be applied in sequence.
type allOf struct {
	modifications []Modification
}

// AllOf creates a modification that applies multiple modifications in sequence.
// If any modification fails, the chain stops and returns the error.
func AllOf(mods ...Modification) Modification {
	return allOf{modifications: mods}
}

func (a allOf) Apply(target interface{}) error {
	for _, mod := range a.modifications {
		if err := mod.Apply(target); err != nil {
			return err
		}
	}
	return nil
}

// Helper functions for JSON path manipulation

// normalizeJSONPath removes the "$." prefix if present
func normalizeJSONPath(path string) string {
	if strings.HasPrefix(path, "$.") {
		return path[2:]
	}
	return path
}

// getFieldValue retrieves a value from a nested map using dot notation
func getFieldValue(data map[string]interface{}, fieldPath string) interface{} {
	parts := strings.Split(fieldPath, ".")
	current := data

	for i, part := range parts {
		value, exists := current[part]
		if !exists {
			return nil
		}

		if i == len(parts)-1 {
			return value
		}

		if nested, ok := value.(map[string]interface{}); ok {
			current = nested
		} else {
			return nil
		}
	}
	return nil
}

// setFieldValue sets a value in a nested map using dot notation
func setFieldValue(data map[string]interface{}, fieldPath string, value interface{}) {
	if value == nil {
		return
	}

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
				// Create new map if not already a map
				newMap := make(map[string]interface{})
				current[part] = newMap
				current = newMap
			}
		} else {
			newMap := make(map[string]interface{})
			current[part] = newMap
			current = newMap
		}
	}
}

// removeFieldValue removes a field from a nested map using dot notation
func removeFieldValue(data map[string]interface{}, fieldPath string) {
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
