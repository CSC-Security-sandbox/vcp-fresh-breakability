package utils

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestGetRootError(t *testing.T) {
	root := errors.New("root error")
	wrapped := errors.New("wrapped: " + root.Error())
	doubleWrapped := errors.New("double wrapped: " + wrapped.Error())

	err := GetRootError(doubleWrapped)
	if err != nil {
		if err.Error() != doubleWrapped.Error() {
			t.Errorf("Expected root error to be '%v', got '%v'", doubleWrapped, err)
		}
	}
}

func TestIsTransientErr_PostgresTransient(t *testing.T) {
	err := &pgconn.PgError{Code: "40001"} // Serialization failure
	if !IsTransientErr(err) {
		t.Error("Expected true for transient PostgreSQL error")
	}
}

func TestIsTransientErr_PostgresNonTransient(t *testing.T) {
	err := &pgconn.PgError{Code: "23505"} // Unique violation
	if IsTransientErr(err) {
		t.Error("Expected false for non-transient PostgreSQL error")
	}
}

func TestIsTransientErr_GenericTransient(t *testing.T) {
	tests := []string{
		"dial error: connection refused",
		"invalid connection",
		"unexpected EOF",
		"error 40001: serialization failure",
	}

	for _, msg := range tests {
		err := errors.New(msg)
		if !IsTransientErr(err) {
			t.Errorf("Expected true for error message: %s", msg)
		}
	}
}

func TestIsTransientErr_UnexpectedEOFCaseInsensitive(t *testing.T) {
	tests := []struct {
		name     string
		errorMsg string
	}{
		{"lowercase", "unexpected eof"},
		{"uppercase", "UNEXPECTED EOF"},
		{"mixed case", "Unexpected Eof"},
		{"in sentence", "Connection failed: unexpected EOF encountered"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errorMsg)
			if !IsTransientErr(err) {
				t.Errorf("Expected true for error message: %s", tt.errorMsg)
			}
		})
	}
}

func TestIsTransientErr_ConnectionResetByPeer(t *testing.T) {
	tests := []struct {
		name     string
		errorMsg string
	}{
		{"lowercase", "connection reset by peer"},
		{"uppercase", "CONNECTION RESET BY PEER"},
		{"mixed case", "Connection Reset By Peer"},
		{"in sentence", "Failed to read: connection reset by peer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errorMsg)
			if !IsTransientErr(err) {
				t.Errorf("Expected true for error message: %s", tt.errorMsg)
			}
		})
	}
}

func TestIsTransientErr_ContextCanceled(t *testing.T) {
	tests := []struct {
		name     string
		errorMsg string
	}{
		{"lowercase", "context canceled"},
		{"uppercase", "CONTEXT CANCELED"},
		{"mixed case", "Context Canceled"},
		{"in sentence", "Operation failed: context canceled by user"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errorMsg)
			if !IsTransientErr(err) {
				t.Errorf("Expected true for error message: %s", tt.errorMsg)
			}
		})
	}
}

func TestIsTransientErr_NonTransient(t *testing.T) {
	err := errors.New("some permanent failure")
	if IsTransientErr(err) {
		t.Error("Expected false for non-transient error")
	}
}

func TestIsTransientErr_0A000_GenericFeatureNotSupported(t *testing.T) {
	// Generic 0A000 errors should NOT be transient (permanent errors)
	tests := []string{
		"unsupported SQL syntax",
		"feature not supported",
		"operation not supported",
		"function not supported",
	}

	for _, msg := range tests {
		err := &pgconn.PgError{Code: "0A000", Message: msg}
		if IsTransientErr(err) {
			t.Errorf("Expected false for generic 0A000 error: %s", msg)
		}
	}
}

func TestIsTransientErr_0A000_CachedPlanError(t *testing.T) {
	// Only the specific "cached plan must not change result type" error should be transient
	err := &pgconn.PgError{
		Code:    "0A000",
		Message: "cached plan must not change result type",
	}
	if !IsTransientErr(err) {
		t.Error("Expected true for cached plan must not change result type error")
	}

	// Test case-insensitive matching
	errCaseInsensitive := &pgconn.PgError{
		Code:    "0A000",
		Message: "CACHED PLAN MUST NOT CHANGE RESULT TYPE",
	}
	if !IsTransientErr(errCaseInsensitive) {
		t.Error("Expected true for cached plan error (case insensitive)")
	}

	// Test partial match should still work
	errPartial := &pgconn.PgError{
		Code:    "0A000",
		Message: "ERROR: cached plan must not change result type (SQLSTATE 0A000)",
	}
	if !IsTransientErr(errPartial) {
		t.Error("Expected true for cached plan error with additional context")
	}
}
