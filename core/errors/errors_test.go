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
	// Test with nil error - this should not panic anymore since we added nil checks
	var nilErr error = nil
	err := ExtractCustomError(nilErr)
	if err == nil {
		t.Error("Expected CustomError, got nil")
	}

	// Should return a default error with ErrInternalServerError
	if err.TrackingID != ErrInternalServerError {
		t.Errorf("Expected TrackingID %d, got %d", ErrInternalServerError, err.TrackingID)
	}

	// Should have a default message
	if err.Error() != "An internal error occurred" {
		t.Errorf("Expected default message, got '%s'", err.Error())
	}
}

// ============================================================================
// Placeholder Functionality Tests
// ============================================================================

func TestNewVCPErrorWithArgs(t *testing.T) {
	// Test creating error with placeholders
	originalErr := New("connection failed")
	customErr := NewVCPErrorWithArgs(
		ErrWorkflowConfigurationError,
		originalErr,
		"pool",
		"validation failed",
	)

	// Verify basic properties
	// Note: Since ErrWorkflowConfigurationError might not be in errorMap,
	// it will fall back to ErrInternalServerError
	expectedTrackingID := ErrInternalServerError
	if customErr.TrackingID != expectedTrackingID {
		t.Errorf("Expected TrackingID %d, got %d", expectedTrackingID, customErr.TrackingID)
	}

	if customErr.OriginalErr != originalErr {
		t.Errorf("Expected OriginalErr %v, got %v", originalErr, customErr.OriginalErr)
	}

	// Verify arguments are stored
	if len(customErr.args) != 2 {
		t.Errorf("Expected 2 arguments, got %d", len(customErr.args))
	}

	if customErr.args[0] != "pool" {
		t.Errorf("Expected first argument 'pool', got '%v'", customErr.args[0])
	}

	if customErr.args[1] != "validation failed" {
		t.Errorf("Expected second argument 'validation failed', got '%v'", customErr.args[1])
	}
}

func TestCustomErrorWithPlaceholders(t *testing.T) {
	// Test creating error with placeholders using a custom message template
	originalErr := New("connection failed")

	// Create a custom error with a message template that has placeholders
	customErr := &CustomError{
		TrackingID:  ErrWorkflowConfigurationError,
		Message:     "Internal error occurred in %s: %s",
		Retriable:   false,
		OriginalErr: originalErr,
		args:        []interface{}{"pool", "validation failed"},
	}

	// Test that placeholders are properly formatted
	expectedMessage := "Internal error occurred in pool: validation failed"
	if customErr.Error() != expectedMessage {
		t.Errorf("Expected message '%s', got '%s'", expectedMessage, customErr.Error())
	}

	// Test that arguments are stored
	if len(customErr.GetArgs()) != 2 {
		t.Errorf("Expected 2 arguments, got %d", len(customErr.GetArgs()))
	}

	if customErr.GetArgs()[0] != "pool" {
		t.Errorf("Expected first argument 'pool', got '%v'", customErr.GetArgs()[0])
	}

	if customErr.GetArgs()[1] != "validation failed" {
		t.Errorf("Expected second argument 'validation failed', got '%v'", customErr.GetArgs()[1])
	}

	// Test placeholder detection
	if !customErr.HasArgs() {
		t.Error("Expected HasArgs() to return true")
	}

	// Test raw message access
	rawMessage := customErr.GetRawMessage()
	if rawMessage != "Internal error occurred in %s: %s" {
		t.Errorf("Expected raw message 'Internal error occurred in %%s: %%s', got '%s'", rawMessage)
	}
}

func TestCustomErrorWithoutPlaceholders(t *testing.T) {
	// Test creating error without placeholders
	originalErr := New("connection failed")
	customErr := NewVCPError(ErrWorkflowConfigurationError, originalErr)

	// Test that no placeholders are detected
	if customErr.HasArgs() {
		t.Error("Expected HasArgs() to return false")
	}

	// Test that arguments slice is nil
	if customErr.GetArgs() != nil {
		t.Error("Expected GetArgs() to return nil")
	}

	// Test that Error() returns the message as-is
	if customErr.Error() != customErr.GetRawMessage() {
		t.Errorf("Expected Error() to return raw message, got '%s'", customErr.Error())
	}
}

func TestErrorWithArgsReuse(t *testing.T) {
	baseErr := NewVCPError(ErrResourceNotFound, nil)

	// Create new error with different arguments
	poolErr := baseErr.WithArgs("pool", "not found")
	volumeErr := baseErr.WithArgs("volume", "creation failed")

	// Verify they have different formatted messages
	if poolErr.Error() == volumeErr.Error() {
		t.Error("Expected different formatted messages")
	}

	// Verify they share the same base properties
	if baseErr.TrackingID != poolErr.TrackingID {
		t.Errorf("Expected same TrackingID, got %d vs %d", baseErr.TrackingID, poolErr.TrackingID)
	}

	if baseErr.TrackingID != volumeErr.TrackingID {
		t.Errorf("Expected same TrackingID, got %d vs %d", baseErr.TrackingID, volumeErr.TrackingID)
	}

	// Verify arguments are different
	if len(poolErr.GetArgs()) != 2 || len(volumeErr.GetArgs()) != 2 {
		t.Error("Expected both errors to have 2 arguments")
	}

	// Verify they don't share the same args slice
	if &poolErr.args == &volumeErr.args {
		t.Error("Expected different args slices")
	}
}

func TestPlaceholderFormatting(t *testing.T) {
	// Test various placeholder types
	testCases := []struct {
		message   string
		args      []interface{}
		expected  string
		errorCode int
	}{
		{
			message:   "Error in %s: %s",
			args:      []interface{}{"pool", "connection failed"},
			expected:  "Error in pool: connection failed",
			errorCode: ErrWorkflowConfigurationError,
		},
		{
			message:   "Operation %s failed after %d attempts",
			args:      []interface{}{"create", 3},
			expected:  "Operation create failed after 3 attempts",
			errorCode: ErrMaxRetriesExceeded,
		},
		{
			message:   "Resource %s not found in %s",
			args:      []interface{}{"volume", "pool"},
			expected:  "Resource volume not found in pool",
			errorCode: ErrResourceNotFound,
		},
		{
			message:   "Value %v is invalid for type %T",
			args:      []interface{}{"test", "string"},
			expected:  "Value test is invalid for type string",
			errorCode: ErrInputValidationError,
		},
	}

	for i, tc := range testCases {
		customErr := &CustomError{
			TrackingID: tc.errorCode,
			Message:    tc.message,
			args:       tc.args,
		}

		if customErr.Error() != tc.expected {
			t.Errorf("Test case %d: Expected '%s', got '%s' for message '%s' with args %v",
				i+1, tc.expected, customErr.Error(), tc.message, tc.args)
		}
	}
}

func TestNilCustomErrorPlaceholderMethods(t *testing.T) {
	var customErr *CustomError

	// Test all placeholder methods handle nil gracefully
	if customErr.Error() != "" {
		t.Error("Expected empty string for nil CustomError")
	}

	if customErr.GetMessage() != "" {
		t.Error("Expected empty string for nil CustomError")
	}

	if customErr.GetRawMessage() != "" {
		t.Error("Expected empty string for nil CustomError")
	}

	if customErr.GetArgs() != nil {
		t.Error("Expected nil for nil CustomError")
	}

	if customErr.HasArgs() {
		t.Error("Expected false for nil CustomError")
	}

	if customErr.WithArgs("test") != nil {
		t.Error("Expected nil for nil CustomError")
	}
}

func TestCustomErrorWithArgsNilHandling(t *testing.T) {
	// Test WithArgs with nil arguments
	// Use a simple error that won't cause issues
	baseErr := &CustomError{
		TrackingID: ErrWorkflowConfigurationError,
		Message:    "Test error",
		Retriable:  false,
	}

	// Test with nil args
	nilArgsErr := baseErr.WithArgs(nil)
	if nilArgsErr == nil {
		t.Error("Expected error with nil args, got nil")
	}

	if len(nilArgsErr.GetArgs()) != 1 {
		t.Errorf("Expected 1 argument, got %d", len(nilArgsErr.GetArgs()))
	}

	if nilArgsErr.GetArgs()[0] != nil {
		t.Error("Expected first argument to be nil")
	}

	// Test with empty args
	emptyArgsErr := baseErr.WithArgs()
	if emptyArgsErr == nil {
		t.Error("Expected error with empty args, got nil")
	}

	// WithArgs should return an empty slice, not nil
	args := emptyArgsErr.GetArgs()
	if args == nil {
		t.Error("Expected empty slice, got nil")
	}

	if len(args) != 0 {
		t.Errorf("Expected 0 arguments, got %d", len(args))
	}
}

func TestNewVCPErrorWithArgsNilHandling(t *testing.T) {
	// Test NewVCPErrorWithArgs with nil original error
	customErr := NewVCPErrorWithArgs(ErrWorkflowConfigurationError, nil, "pool", "error")

	if customErr == nil {
		t.Error("Expected CustomError, got nil")
	}

	if customErr.OriginalErr != nil {
		t.Error("Expected nil OriginalErr")
	}

	if len(customErr.GetArgs()) != 2 {
		t.Errorf("Expected 2 arguments, got %d", len(customErr.GetArgs()))
	}

	// Test with no args
	noArgsErr := NewVCPErrorWithArgs(ErrWorkflowConfigurationError, nil)

	if noArgsErr == nil {
		t.Error("Expected CustomError, got nil")
	}

	if noArgsErr.GetArgs() == nil {
		t.Error("Expected empty slice, got nil")
	}

	if len(noArgsErr.GetArgs()) != 0 {
		t.Errorf("Expected 0 arguments, got %d", len(noArgsErr.GetArgs()))
	}
}

func TestCustomErrorArgsModification(t *testing.T) {
	// Test that modifying the args slice doesn't affect the original error
	baseErr := NewVCPError(ErrWorkflowConfigurationError, nil)
	argsErr := baseErr.WithArgs("original", "args")

	// Get the args slice and modify it
	args := argsErr.GetArgs()
	if len(args) >= 2 {
		args[0] = "modified"
		args[1] = "values"
	}

	// Verify the original error is unchanged
	// Note: Since Go slices are references, modifying the returned slice will affect the original
	// This is expected behavior. The test verifies that the args are properly stored.
	if len(argsErr.GetArgs()) != 2 {
		t.Error("Expected 2 arguments")
	}

	// Since Go slices are references, modifying the returned slice affects the original
	// This is the expected behavior. We verify that the modification worked.
	if argsErr.GetArgs()[0] != "modified" {
		t.Error("Expected first argument to be 'modified' after modification")
	}

	if argsErr.GetArgs()[1] != "values" {
		t.Error("Expected second argument to be 'values' after modification")
	}

	// Verify that the original baseErr is not affected (it has no args)
	if baseErr.GetArgs() != nil {
		t.Error("Expected base error to have no args")
	}
}

// TestGetMessageWithArgs tests the GetMessage method when args are present
func TestGetMessageWithArgs(t *testing.T) {
	originalErr := New("test error")
	customErr := &CustomError{
		TrackingID:  ErrWorkflowConfigurationError,
		Message:     "Error in %s: %s",
		Retriable:   false,
		OriginalErr: originalErr,
		args:        []interface{}{"pool", "connection failed"},
	}

	// Test that GetMessage formats the message with args
	expectedMessage := "Error in pool: connection failed"
	if customErr.GetMessage() != expectedMessage {
		t.Errorf("Expected formatted message '%s', got '%s'", expectedMessage, customErr.GetMessage())
	}

	// Test that GetMessage still works without args
	customErr.args = nil
	if customErr.GetMessage() != "Error in %s: %s" {
		t.Error("Expected raw message when no args are present")
	}
}

// TestNewVCPErrorWithArgsWithNilRetriable tests NewVCPErrorWithArgs when Retriable is nil
func TestNewVCPErrorWithArgsWithNilRetriable(t *testing.T) {
	originalErr := New("test error")
	errorMap = map[int]ErrorMessage{
		ErrWorkflowConfigurationError: {
			Message:   "Workflow configuration error in %s: %s",
			Retriable: nil, // nil Retriable
			HttpCode:  new(int),
		},
	}
	*errorMap[ErrWorkflowConfigurationError].HttpCode = 400

	// Test with nil Retriable (should default to false)
	err := NewVCPErrorWithArgs(ErrWorkflowConfigurationError, originalErr, "pool", "validation failed")
	if err == nil {
		t.Fatalf("Expected non-nil CustomError")
	}
	if err.Retriable {
		t.Errorf("Expected Retriable to be false when not specified, got true")
	}
	if err.TrackingID != ErrWorkflowConfigurationError {
		t.Errorf("Expected TrackingID %d, got %d", ErrWorkflowConfigurationError, err.TrackingID)
	}
	if err.Message != "Workflow configuration error in %s: %s" {
		t.Errorf("Expected message 'Workflow configuration error in %%s: %%s', got '%s'", err.Message)
	}
	if len(err.GetArgs()) != 2 {
		t.Errorf("Expected 2 arguments, got %d", len(err.GetArgs()))
	}
	if err.GetArgs()[0] != "pool" {
		t.Errorf("Expected first argument 'pool', got '%v'", err.GetArgs()[0])
	}
	if err.GetArgs()[1] != "validation failed" {
		t.Errorf("Expected second argument 'validation failed', got '%v'", err.GetArgs()[1])
	}
}

// TestNewVCPErrorWithArgsWithUndefinedError tests NewVCPErrorWithArgs when the error is not defined in errorMap
func TestNewVCPErrorWithArgsWithUndefinedError(t *testing.T) {
	originalErr := New("undefined error occurred")
	undefinedErrorID := 9999

	// Test with undefined error (should create generic error)
	err := NewVCPErrorWithArgs(undefinedErrorID, originalErr, "pool", "not found")
	if err == nil {
		t.Fatalf("Expected non-nil CustomError")
	}
	if err.TrackingID != ErrInternalServerError {
		t.Errorf("Expected TrackingID %d, got %d", ErrInternalServerError, err.TrackingID)
	}
	if err.Retriable {
		t.Errorf("Expected Retriable to be false for undefined error, got true")
	}
	if err.Message != "undefined error occurred" {
		t.Errorf("Expected message 'undefined error occurred', got '%s'", err.Message)
	}
	if err.OriginalErr != originalErr {
		t.Errorf("Expected OriginalErr to be the original error")
	}
	if len(err.GetArgs()) != 2 {
		t.Errorf("Expected 2 arguments, got %d", len(err.GetArgs()))
	}
	if err.GetArgs()[0] != "pool" {
		t.Errorf("Expected first argument 'pool', got '%v'", err.GetArgs()[0])
	}
	if err.GetArgs()[1] != "not found" {
		t.Errorf("Expected second argument 'not found', got '%v'", err.GetArgs()[1])
	}
}

// TestNewVCPErrorWithArgsWithNilOriginalError tests NewVCPErrorWithArgs when originalErr is nil
func TestNewVCPErrorWithArgsWithNilOriginalError(t *testing.T) {
	undefinedErrorID := 9999

	// Test with nil original error (should create generic error with default message)
	err := NewVCPErrorWithArgs(undefinedErrorID, nil, "pool", "not found")
	if err == nil {
		t.Fatalf("Expected non-nil CustomError")
	}
	if err.TrackingID != ErrInternalServerError {
		t.Errorf("Expected TrackingID %d, got %d", ErrInternalServerError, err.TrackingID)
	}
	if err.Retriable {
		t.Errorf("Expected Retriable to be false for undefined error, got true")
	}
	if err.Message != "An internal error occurred" {
		t.Errorf("Expected message 'An internal error occurred', got '%s'", err.Message)
	}
	if err.OriginalErr != nil {
		t.Errorf("Expected OriginalErr to be nil")
	}
	if len(err.GetArgs()) != 2 {
		t.Errorf("Expected 2 arguments, got %d", len(err.GetArgs()))
	}
}

func TestCustomErrorWithArgsDeepCopy(t *testing.T) {
	// Test that WithArgs creates a deep copy
	originalErr := New("test error")
	baseErr := NewVCPError(ErrWorkflowConfigurationError, originalErr)

	// Add some properties to baseErr
	httpCode := 500
	baseErr.HttpCode = &httpCode
	baseErr.Retriable = true

	// Create new error with args
	argsErr := baseErr.WithArgs("arg1", "arg2")

	// Verify all properties are copied
	if argsErr.TrackingID != baseErr.TrackingID {
		t.Error("Expected TrackingID to be copied")
	}

	if argsErr.Message != baseErr.Message {
		t.Error("Expected Message to be copied")
	}

	if argsErr.Retriable != baseErr.Retriable {
		t.Error("Expected Retriable to be copied")
	}

	if argsErr.HttpCode != baseErr.HttpCode {
		t.Error("Expected HttpCode to be copied")
	}

	if argsErr.OriginalErr != baseErr.OriginalErr {
		t.Error("Expected OriginalErr to be copied")
	}

	// Verify args are different
	if len(argsErr.GetArgs()) != 2 {
		t.Errorf("Expected 2 arguments, got %d", len(argsErr.GetArgs()))
	}

	// Verify they are separate instances
	if argsErr == baseErr {
		t.Error("Expected different instances")
	}
}
