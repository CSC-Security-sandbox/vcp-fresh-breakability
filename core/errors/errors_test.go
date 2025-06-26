package errors

import (
	"context"
	"os"
	"testing"

	"go.temporal.io/sdk/temporal"
)

var validJSON = `{"1001":{"message":"Input is invalid.","retriable":false,"http_code":400},"1002":{"message":"The requested resource was not found.","retriable":false,"http_code":404},"1003":{"message":"An internal error occurred.","retriable":true,"http_code":500}}`

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
	if !customErr.IsError(1003) {
		t.Errorf("Expected IsError to return true for InternalError")
	}
	// Test GetHttpCode method
	hasCode, code := customErr.GetHttpCode()
	if !hasCode || code != 500 {
		t.Errorf("Expected HTTP code 500, got %d", code)
	}
}

func TestNewVSAError(t *testing.T) {
	originalErr := New("original error")
	errorMap = map[int]ErrorMessage{
		1003: {
			Message:   "An internal error occurred.",
			Retriable: new(bool),
			HttpCode:  new(int),
		},
	}
	*errorMap[1003].Retriable = true
	*errorMap[1003].HttpCode = 500

	// Test with defined error
	err := NewVCPError(1003, originalErr)
	_, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("Expected CustomError, got %T", err)
	}
	// Test with undefined error
	err = NewVCPError(1005, originalErr)
	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("Expected CustomError, got %T", err)
	}
	if customErr.TrackingID != 0 {
		t.Errorf("Expected ErrorName NotDefined, got %d", customErr.TrackingID)
	}
}

func TestIsAndAs(t *testing.T) {
	originalErr := New("original error")
	customErr := &CustomError{
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

func TestGetErrorMessageByTrackingID(t *testing.T) {
	errorMap = map[int]ErrorMessage{
		1001: {
			Message:   "Input is invalid.",
			Retriable: new(bool),
			HttpCode:  new(int),
		},
	}
	*errorMap[1001].Retriable = false
	*errorMap[1001].HttpCode = 400

	// Test with existing TrackingID
	msg := GetErrorMessageByTrackingID(1001)
	if msg == nil {
		t.Fatalf("Expected non-nil ErrorMessage")
	}
	if msg.Message != "Input is invalid." {
		t.Errorf("Expected message 'Input is invalid.', got %s", msg.Message)
	}
	if msg.HttpCode == nil || *msg.HttpCode != 400 {
		t.Errorf("Expected HTTP code 400, got %v", msg.HttpCode)
	}

	// Test with non-existent TrackingID
	msg = GetErrorMessageByTrackingID(9999)
	if msg == nil {
		t.Fatalf("Expected non-nil ErrorMessage for undefined error")
	}
	if msg.Message != "undefined error" {
		t.Errorf("Expected message 'undefined error', got %s", msg.Message)
	}
	if msg.HttpCode == nil || *msg.HttpCode != 500 {
		t.Errorf("Expected HTTP code 500, got %v", msg.HttpCode)
	}
}

func TestWrapAsTemporalApplicationError(t *testing.T) {
	originalErr := New("original error")
	customErr := &CustomError{
		TrackingID:  1003,
		Message:     "An internal error occurred.",
		Retriable:   true,
		OriginalErr: originalErr,
	}

	// Test wrapping a CustomError
	wrapped := WrapAsTemporalApplicationError(customErr)
	appErr, ok := wrapped.(*temporal.ApplicationError)
	if !ok {
		t.Errorf("Expected temporal.ApplicationError, got %T", wrapped)
	}
	if appErr != nil && appErr.Message() != customErr.Error() {
		t.Errorf("Expected error message %q, got %q", customErr.Error(), appErr.Error())
	}

	// Test passing a non-CustomError
	plainErr := New("plain error")
	result := WrapAsTemporalApplicationError(plainErr)
	if result != plainErr {
		t.Errorf("Expected original error to be returned unchanged")
	}
}

func TestExtractUserFriendlyErrorMessage_TemporalApplicationError(t *testing.T) {
	// Setup errorMap for test
	ctx := context.TODO()
	errorMap = map[int]ErrorMessage{
		1001: {Message: "User error"},
	}

	// Create a temporal.ApplicationError with CustomErrorType and details
	appErr := temporal.NewApplicationError("wrapped", CustomErrorType, 1001, "details")
	msg := ExtractCustomerFacingErrorMessage(ctx, appErr)
	if msg != "User error" {
		t.Errorf("Expected 'User error', got %q", msg)
	}
}

func TestExtractUserFriendlyErrorMessage_TemporalApplicationError_DetailsError(t *testing.T) {
	// Create a temporal.ApplicationError with CustomErrorType but no details
	ctx := context.TODO()
	appErr := temporal.NewApplicationError("wrapped", CustomErrorType)
	msg := ExtractCustomerFacingErrorMessage(ctx, appErr)
	if msg != DefaultErrorMessage {
		t.Errorf("Expected default message, got %q", msg)
	}
}

func TestExtractUserFriendlyErrorMessage_NonTemporalError(t *testing.T) {
	ctx := context.TODO()
	err := New("plain error")
	msg := ExtractCustomerFacingErrorMessage(ctx, err)
	if msg != DefaultErrorMessage {
		t.Errorf("Expected default message, got %q", msg)
	}
}
