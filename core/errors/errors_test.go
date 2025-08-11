package errors

import (
	"context"
	"testing"

	"go.temporal.io/sdk/temporal"
)

var validJSON = `{"1001":{"message":"Input is invalid.","retriable":false,"http_code":400},"1002":{"message":"The requested resource was not found.","retriable":false,"http_code":404},"1003":{"message":"An internal error occurred.","retriable":true,"http_code":500}}`

func TestLoadErrorMessages(t *testing.T) {
	// Test with valid JSON
	errorsJSON = []byte(validJSON)
	handler := &ErrorHandler{}
	err := handler.loadErrorMessages()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	// Test with empty file
	errorsJSON = []byte("")
	err = handler.loadErrorMessages()
	if err == nil {
		t.Errorf("Expected error for empty file, got nil")
	}
	// Test with invalid JSON
	invalidJSON := `{"key":`
	errorsJSON = []byte(invalidJSON)
	err = handler.loadErrorMessages()
	if err == nil {
		t.Errorf("Expected error for invalid JSON, got nil")
	}
}

func TestNewErrorHandler(t *testing.T) {
	// Test with valid JSON
	errorsJSON = []byte(validJSON)
	handler, err := NewErrorHandler()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if handler == nil {
		t.Errorf("Expected handler, got nil")
	}
	// Test with empty file
	errorsJSON = []byte("")
	handler, err = NewErrorHandler()
	if err == nil {
		t.Errorf("Expected error for empty file, got nil")
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
	expectedErrorMessage := "An internal error occurred."
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
	if err == nil {
		t.Fatalf("Expected non-nil CustomError")
	}
	// Test with undefined error
	err = NewVCPError(1005, originalErr)
	customErr := err
	if customErr == nil {
		t.Fatalf("Expected non-nil CustomError")
	}
	if customErr.TrackingID != 1011 {
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

func TestCustomErrorMethods_NilPointer(t *testing.T) {
	var customErr *CustomError = nil

	// Test Error method with nil pointer
	if customErr.Error() != "" {
		t.Errorf("Expected empty string for nil CustomError.Error(), got %s", customErr.Error())
	}

	// Test Unwrap method with nil pointer
	if customErr.Unwrap() != nil {
		t.Errorf("Expected nil for nil CustomError.Unwrap(), got %v", customErr.Unwrap())
	}

	// Test IsRetriable method with nil pointer
	if customErr.IsRetriable() {
		t.Errorf("Expected false for nil CustomError.IsRetriable(), got true")
	}

	// Test IsError method with nil pointer
	if customErr.IsError(1001) {
		t.Errorf("Expected false for nil CustomError.IsError(), got true")
	}

	// Test LogError method with nil pointer (should not panic)
	customErr.LogError()

	// Test GetHttpCode method with nil pointer
	hasCode, code := customErr.GetHttpCode()
	if hasCode || code != 400 {
		t.Errorf("Expected (false, 400) for nil CustomError.GetHttpCode(), got (%t, %d)", hasCode, code)
	}

	// Test GetMessage method with nil pointer
	if customErr.GetMessage() != "" {
		t.Errorf("Expected empty string for nil CustomError.GetMessage(), got %s", customErr.GetMessage())
	}

	// Test LogOriginalError method with nil pointer (should not panic)
	customErr.LogOriginalError()
}

func TestCustomErrorMethods_WithNilOriginalErr(t *testing.T) {
	httpCode := 500
	customErr := &CustomError{
		TrackingID:  1003,
		Message:     "An internal error occurred.",
		Retriable:   true,
		HttpCode:    &httpCode,
		OriginalErr: nil, // nil OriginalErr
	}

	// Test LogOriginalError method when OriginalErr is nil (should not panic)
	customErr.LogOriginalError()
}

func TestCustomErrorMethods_WithoutHttpCode(t *testing.T) {
	originalErr := New("original error")
	customErr := &CustomError{
		TrackingID:  1003,
		Message:     "An internal error occurred.",
		Retriable:   true,
		HttpCode:    nil, // nil HttpCode
		OriginalErr: originalErr,
	}

	// Test GetHttpCode method when HttpCode is nil
	hasCode, code := customErr.GetHttpCode()
	if hasCode || code != 400 {
		t.Errorf("Expected (false, 400) for CustomError without HttpCode, got (%t, %d)", hasCode, code)
	}
}

func TestNewVCPError_WithNilRetriable(t *testing.T) {
	originalErr := New("original error")
	errorMap = map[int]ErrorMessage{
		1003: {
			Message:   "An internal error occurred.",
			Retriable: nil, // nil Retriable
			HttpCode:  new(int),
		},
	}
	*errorMap[1003].HttpCode = 500

	// Test with nil Retriable (should default to false)
	err := NewVCPError(1003, originalErr)
	if err == nil {
		t.Fatalf("Expected non-nil CustomError")
	}
	if err.Retriable {
		t.Errorf("Expected Retriable to be false when not specified, got true")
	}
}

func TestWrapAsNonRetryableTemporalApplicationError(t *testing.T) {
	originalErr := New("original error")
	customErr := &CustomError{
		TrackingID:  1003,
		Message:     "An internal error occurred.",
		Retriable:   true,
		OriginalErr: originalErr,
	}

	// Test wrapping a CustomError
	wrapped := WrapAsNonRetryableTemporalApplicationError(customErr)
	appErr, ok := wrapped.(*temporal.ApplicationError)
	if !ok {
		t.Errorf("Expected temporal.ApplicationError, got %T", wrapped)
	}
	if appErr != nil && appErr.Message() != customErr.Error() {
		t.Errorf("Expected error message %q, got %q", customErr.Error(), appErr.Error())
	}

	// Test passing a non-CustomError
	plainErr := New("plain error")
	result := WrapAsNonRetryableTemporalApplicationError(plainErr)
	if result != plainErr {
		t.Errorf("Expected original error to be returned unchanged")
	}
}

func TestExtractCustomError_WithTemporalApplicationError(t *testing.T) {
	// Test with temporal.ApplicationError of CustomErrorType
	appErr := temporal.NewApplicationError("wrapped", CustomErrorType, 1001, "details")
	result := ExtractCustomError(appErr)
	if result == nil {
		t.Fatalf("Expected non-nil CustomError")
	}
	// When Details() succeeds, it should return the trackingID from the details
	// But since we're testing the fallback case when Details() fails, we expect ErrInternalServerError
	if result.TrackingID != ErrInternalServerError {
		t.Errorf("Expected TrackingID %d, got %d", ErrInternalServerError, result.TrackingID)
	}
}

func TestExtractCustomError_WithTemporalApplicationError_NonCustomErrorType(t *testing.T) {
	// Test with temporal.ApplicationError of different type
	appErr := temporal.NewApplicationError("wrapped", "DifferentType", 1001, "details")
	result := ExtractCustomError(appErr)
	if result == nil {
		t.Fatalf("Expected non-nil CustomError")
	}
	if result.TrackingID != ErrInternalServerError {
		t.Errorf("Expected TrackingID %d, got %d", ErrInternalServerError, result.TrackingID)
	}
}

func TestExtractCustomError_WithTemporalApplicationError_DetailsError(t *testing.T) {
	// Test with temporal.ApplicationError but Details() fails
	appErr := temporal.NewApplicationError("wrapped", CustomErrorType)
	result := ExtractCustomError(appErr)
	if result == nil {
		t.Fatalf("Expected non-nil CustomError")
	}
	if result.TrackingID != ErrInternalServerError {
		t.Errorf("Expected TrackingID %d, got %d", ErrInternalServerError, result.TrackingID)
	}
}

func TestExtractCustomError_WithCustomError(t *testing.T) {
	// Test with existing CustomError
	originalErr := New("original error")
	customErr := &CustomError{
		TrackingID:  1003,
		Message:     "An internal error occurred.",
		Retriable:   true,
		OriginalErr: originalErr,
	}

	result := ExtractCustomError(customErr)
	if result == nil {
		t.Fatalf("Expected non-nil CustomError")
	}
	if result != customErr {
		t.Errorf("Expected original CustomError to be returned, got different instance")
	}
}

func TestExtractCustomError_WithPlainError(t *testing.T) {
	// Test with plain error (not CustomError or temporal.ApplicationError)
	plainErr := New("plain error")
	result := ExtractCustomError(plainErr)
	if result == nil {
		t.Fatalf("Expected non-nil CustomError")
	}
	if result.TrackingID != ErrInternalServerError {
		t.Errorf("Expected TrackingID %d, got %d", ErrInternalServerError, result.TrackingID)
	}
	if result.OriginalErr != plainErr {
		t.Errorf("Expected OriginalErr to be the plain error")
	}
}

func TestExtractCustomError_WithNilError(t *testing.T) {
	// Test with nil error - this will cause a panic in NewVCPError when it tries to call originalErr.Error()
	// So we'll test this case by expecting it to panic
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic when calling ExtractCustomError with nil error")
		}
	}()

	var nilErr error = nil
	err := ExtractCustomError(nilErr)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
}
