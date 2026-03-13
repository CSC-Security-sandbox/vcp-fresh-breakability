package dsl

import (
	"fmt"
	"net/http"
	"strings"
)

// Condition is a function that evaluates a request and returns:
// - (true, "") if the condition passes
// - (false, "reason") if the condition fails, with a specific reason
type Condition func(r *http.Request) (bool, string)

// checkTwoFieldsExist normalizes fieldA and fieldB and reports whether each exists in data.
func checkTwoFieldsExist(data map[string]interface{}, fieldA, fieldB string) (hasA, hasB bool) {
	pathA := normalizeJSONPath(fieldA)
	pathB := normalizeJSONPath(fieldB)
	return fieldExists(data, pathA), fieldExists(data, pathB)
}

// HasExactlyOneOf creates a condition that checks that exactly one of the two fields exists.
// Fails with missingReason if neither exists, or bothReason if both exist.
// Uses cached parsed body from context if available.
func HasExactlyOneOf(fieldA, fieldB, missingReason, bothReason string) Condition {
	return func(r *http.Request) (bool, string) {
		data, parseErr := GetParsedBody(r)
		if parseErr != "" {
			return false, parseErr
		}
		hasA, hasB := checkTwoFieldsExist(data, fieldA, fieldB)
		if hasA && hasB {
			return false, bothReason
		}
		if !hasA && !hasB {
			return false, missingReason
		}
		return true, ""
	}
}

// HasAtMostOneOf creates a condition that checks that at most one of the two fields exists.
// Fails with bothReason only when both exist; passes when zero or one is present.
// Uses cached parsed body from context if available.
func HasAtMostOneOf(fieldA, fieldB, bothReason string) Condition {
	return func(r *http.Request) (bool, string) {
		data, parseErr := GetParsedBody(r)
		if parseErr != "" {
			return false, parseErr
		}
		hasA, hasB := checkTwoFieldsExist(data, fieldA, fieldB)
		if hasA && hasB {
			return false, bothReason
		}
		return true, ""
	}
}

// HasFields creates a condition that checks if the request body contains the specified fields.
// Returns a failure reason listing which fields are missing.
// Uses cached parsed body from context if available.
func HasFields(fields ...string) Condition {
	return func(r *http.Request) (bool, string) {
		data, parseErr := GetParsedBody(r)
		if parseErr != "" {
			return false, parseErr
		}

		var missing []string
		for _, field := range fields {
			fieldPath := normalizeJSONPath(field)
			if !fieldExists(data, fieldPath) {
				missing = append(missing, field)
			}
		}

		if len(missing) > 0 {
			return false, fmt.Sprintf("missing required field(s): %s", strings.Join(missing, ", "))
		}
		return true, ""
	}
}

// HasFieldValue creates a condition that checks if a field has a specific value.
// Uses cached parsed body from context if available.
func HasFieldValue(field string, expectedValue interface{}) Condition {
	return func(r *http.Request) (bool, string) {
		data, parseErr := GetParsedBody(r)
		if parseErr != "" {
			return false, parseErr
		}

		fieldPath := normalizeJSONPath(field)
		actualValue := getFieldValue(data, fieldPath)
		if actualValue != expectedValue {
			return false, fmt.Sprintf("field '%s' must be %v, got %v", field, expectedValue, actualValue)
		}
		return true, ""
	}
}

// HasFieldValueIn creates a condition that checks if a field value is one of the allowed values.
// Uses cached parsed body from context if available.
func HasFieldValueIn(field string, allowedValues ...interface{}) Condition {
	return func(r *http.Request) (bool, string) {
		data, parseErr := GetParsedBody(r)
		if parseErr != "" {
			return false, parseErr
		}

		fieldPath := normalizeJSONPath(field)
		actualValue := getFieldValue(data, fieldPath)

		for _, allowed := range allowedValues {
			if actualValue == allowed {
				return true, ""
			}
		}
		return false, fmt.Sprintf("field '%s' must be one of %v, got %v", field, allowedValues, actualValue)
	}
}

// HasHeader creates a condition that checks if a request header has a specific value.
func HasHeader(header, expectedValue string) Condition {
	return func(r *http.Request) (bool, string) {
		if r == nil {
			return false, "request is nil"
		}
		if r.Header.Get(header) != expectedValue {
			return false, fmt.Sprintf("header '%s' must be '%s'", header, expectedValue)
		}
		return true, ""
	}
}

// And combines multiple conditions with AND logic.
// Returns the first failure reason encountered.
func And(conditions ...Condition) Condition {
	return func(r *http.Request) (bool, string) {
		for _, cond := range conditions {
			if ok, reason := cond(r); !ok {
				return false, reason
			}
		}
		return true, ""
	}
}

// Or combines multiple conditions with OR logic.
// Returns true if any condition passes, otherwise returns the last failure reason.
func Or(conditions ...Condition) Condition {
	return func(r *http.Request) (bool, string) {
		var lastReason string
		for _, cond := range conditions {
			if ok, reason := cond(r); ok {
				return true, ""
			} else {
				lastReason = reason
			}
		}
		return false, lastReason
	}
}

// Not negates a condition.
func Not(condition Condition) Condition {
	return func(r *http.Request) (bool, string) {
		ok, _ := condition(r)
		return !ok, ""
	}
}

// IsMethod checks if the request method matches.
func IsMethod(method string) Condition {
	return func(r *http.Request) (bool, string) {
		if r == nil {
			return false, "request is nil"
		}
		if r.Method != method {
			return false, fmt.Sprintf("method must be %s", method)
		}
		return true, ""
	}
}

// IfPresentThenValue validates a field value only if the field is present.
// If the field is not present in valid JSON, the condition passes.
// If the field is present, it must have one of the allowed values.
// If the body cannot be parsed (invalid JSON, empty, etc.), the condition fails with the parse error.
func IfPresentThenValue(field string, allowedValues ...interface{}) Condition {
	return func(r *http.Request) (bool, string) {
		// Check if field exists, distinguishing between "not present" and "parse error"
		exists, parseErr := checkFieldExists(r, field)
		if parseErr != "" {
			return false, parseErr // Propagate parse errors
		}
		if !exists {
			return true, "" // Field not present in valid JSON, condition passes
		}
		// Field exists, validate its value
		return HasFieldValueIn(field, allowedValues...)(r)
	}
}

// IfPresentThenEquals validates a field value only if the field is present.
// If the field is not present in valid JSON, the condition passes.
// If the field is present, it must equal the expected value.
// If the body cannot be parsed (invalid JSON, empty, etc.), the condition fails with the parse error.
func IfPresentThenEquals(field string, expectedValue interface{}) Condition {
	return func(r *http.Request) (bool, string) {
		// Check if field exists, distinguishing between "not present" and "parse error"
		exists, parseErr := checkFieldExists(r, field)
		if parseErr != "" {
			return false, parseErr // Propagate parse errors
		}
		if !exists {
			return true, "" // Field not present in valid JSON, condition passes
		}
		// Field exists, validate its value
		return HasFieldValue(field, expectedValue)(r)
	}
}

// checkFieldExists checks if a field exists in the request body.
// Returns:
//   - (true, "") if the field exists in valid JSON
//   - (false, "") if the field does not exist in valid JSON
//   - (false, "error") if there was a parse error (invalid JSON, empty body, etc.)
//
// Uses cached parsed body from context if available.
func checkFieldExists(r *http.Request, field string) (exists bool, parseErr string) {
	data, err := GetParsedBody(r)
	if err != "" {
		return false, err
	}

	fieldPath := normalizeJSONPath(field)
	if fieldExists(data, fieldPath) {
		return true, ""
	}
	return false, "" // Field not present, but JSON is valid
}

// Always returns a condition that always passes.
func Always() Condition {
	return func(r *http.Request) (bool, string) {
		return true, ""
	}
}

// Never returns a condition that always fails with the given reason.
func Never(reason string) Condition {
	return func(r *http.Request) (bool, string) {
		return false, reason
	}
}

// fieldExists checks if a field exists in a nested map using dot notation
func fieldExists(data map[string]interface{}, fieldPath string) bool {
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
