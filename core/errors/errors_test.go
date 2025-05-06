package errors

import (
	"os"
	"testing"
)

var validJSON = `{"InvalidInput":{"tracking_id":1001,"message":"Input is invalid.","retriable":false,"http_code":400},"NotFound":{"tracking_id":1002,"message":"The requested resource was not found.","retriable":false,"http_code":404},"InternalError":{"tracking_id":1003,"message":"An internal error occurred.","retriable":true,"http_code":500}}`

func TestLoadErrorMessages(t *testing.T) {
	// Create a temporary valid JSON file
	tmpFile, err := os.CreateTemp("", "config*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			return
		}
	}(tmpFile.Name())
	if _, err := tmpFile.Write([]byte(validJSON)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	err = tmpFile.Close()
	if err != nil {
		return
	}
	// Test with valid JSON
	handler := &ErrorHandler{}
	err = handler.loadErrorMessages(tmpFile.Name())
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	// Test with non-existent file
	err = handler.loadErrorMessages("nonexistent.json")
	if err == nil {
		t.Errorf("Expected error for non-existent file, got nil")
	}
	// Test with invalid JSON
	invalidJSON := `{"key":`
	tmpFile, err = os.CreateTemp("", "config*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			return
		}
	}(tmpFile.Name())
	if _, err := tmpFile.Write([]byte(invalidJSON)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	err = tmpFile.Close()
	if err != nil {
		return
	}
	err = handler.loadErrorMessages(tmpFile.Name())
	if err == nil {
		t.Errorf("Expected error for invalid JSON, got nil")
	}
}

func TestNewErrorHandler(t *testing.T) {
	// Create a temporary valid JSON file
	tmpFile, err := os.CreateTemp("", "config*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			return
		}
	}(tmpFile.Name())
	if _, err := tmpFile.Write([]byte(validJSON)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	err = tmpFile.Close()
	if err != nil {
		return
	}
	// Test with valid JSON
	handler, err := NewErrorHandler(tmpFile.Name())
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if handler == nil {
		t.Errorf("Expected handler, got nil")
	}
	// Test with non-existent file
	handler, err = NewErrorHandler("nonexistent.json")
	if err == nil {
		t.Errorf("Expected error for non-existent file, got nil")
	}
	if handler != nil {
		t.Errorf("Expected nil handler, got %v", handler)
	}
}

func TestCustomErrorMethods(t *testing.T) {
	originalErr := New("original error")
	httpCode := 500
	customErr := &CustomError{
		ErrorName:   "InternalError",
		TrackingID:  1003,
		Message:     "An internal error occurred.",
		Retriable:   true,
		HttpCode:    &httpCode,
		OriginalErr: originalErr,
	}
	// Test Error method
	expectedErrorMessage := "[1003] An internal error occurred."
	if customErr.Error() != expectedErrorMessage {
		t.Errorf("Expected %s, got %s", expectedErrorMessage, customErr.Error())
	}
	// Test Unwrap method
	if customErr.Unwrap() != originalErr {
		t.Errorf("Expected %v, got %v", originalErr, customErr.Unwrap())
	}
	// Test IsRetriable method
	if !customErr.IsRetriable() {
		t.Errorf("Expected retriable to be true, got false")
	}
	// Test IsError method
	if !customErr.IsError("InternalError") {
		t.Errorf("Expected IsError to return true for InternalError")
	}
	if !customErr.IsError("1003") {
		t.Errorf("Expected IsError to return true for TrackingID 1003")
	}
	// Test GetHttpCode method
	hasCode, code := customErr.GetHttpCode()
	if !hasCode || code != 500 {
		t.Errorf("Expected HTTP code 500, got %d", code)
	}
}

func TestNewVSAError(t *testing.T) {
	originalErr := New("original error")
	errorMap = map[string]ErrorMessage{
		"InternalError": {
			TrackingID: 1003,
			Message:    "An internal error occurred.",
			Retriable:  new(bool),
			HttpCode:   new(int),
		},
	}
	*errorMap["InternalError"].Retriable = true
	*errorMap["InternalError"].HttpCode = 500

	// Test with defined error
	err := NewVCPError("InternalError", originalErr)
	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("Expected CustomError, got %T", err)
	}
	if customErr.ErrorName != "InternalError" {
		t.Errorf("Expected ErrorName InternalError, got %s", customErr.ErrorName)
	}
	// Test with undefined error
	err = NewVCPError("UndefinedError", originalErr)
	customErr, ok = err.(*CustomError)
	if !ok {
		t.Fatalf("Expected CustomError, got %T", err)
	}
	if customErr.ErrorName != "NotDefined" {
		t.Errorf("Expected ErrorName NotDefined, got %s", customErr.ErrorName)
	}
}

func TestIsAndAs(t *testing.T) {
	originalErr := New("original error")
	customErr := &CustomError{
		ErrorName:   "InternalError",
		TrackingID:  1003,
		Message:     "An internal error occurred.",
		Retriable:   true,
		OriginalErr: originalErr,
	}
	// Test Is function
	if !Is(customErr, originalErr) {
		t.Errorf("Expected Is to return true for originalErr")
	}
	// Test As function
	var target *CustomError
	if !As(customErr, &target) {
		t.Errorf("Expected As to return true for CustomError")
	}
	if target != customErr {
		t.Errorf("Expected target to be %v, got %v", customErr, target)
	}
}
