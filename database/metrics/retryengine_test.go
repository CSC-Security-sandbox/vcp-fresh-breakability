package database

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
)

func TestGetRootError(t *testing.T) {
	root := errors.New("root error")
	wrapped := errors.New("wrapped: " + root.Error())
	doubleWrapped := errors.New("double wrapped: " + wrapped.Error())

	err := utils.GetRootError(doubleWrapped)
	if err != nil {
		if err.Error() != doubleWrapped.Error() {
			t.Errorf("Expected root error to be '%v', got '%v'", doubleWrapped, err)
		}
	}
}

func TestIsTransientErr_PostgresTransient(t *testing.T) {
	err := &pgconn.PgError{Code: "40001"} // Serialization failure
	if !utils.IsTransientErr(err) {
		t.Error("Expected true for transient PostgreSQL error")
	}
}

func TestIsTransientErr_PostgresNonTransient(t *testing.T) {
	err := &pgconn.PgError{Code: "23505"} // Unique violation
	if utils.IsTransientErr(err) {
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
		if !utils.IsTransientErr(err) {
			t.Errorf("Expected true for error message: %s", msg)
		}
	}
}

func TestIsTransientErr_NonTransient(t *testing.T) {
	err := errors.New("some permanent failure")
	if utils.IsTransientErr(err) {
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
		if utils.IsTransientErr(err) {
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
	if !utils.IsTransientErr(err) {
		t.Error("Expected true for cached plan must not change result type error")
	}

	// Test case-insensitive matching
	errCaseInsensitive := &pgconn.PgError{
		Code:    "0A000",
		Message: "CACHED PLAN MUST NOT CHANGE RESULT TYPE",
	}
	if !utils.IsTransientErr(errCaseInsensitive) {
		t.Error("Expected true for cached plan error (case insensitive)")
	}

	// Test partial match should still work
	errPartial := &pgconn.PgError{
		Code:    "0A000",
		Message: "ERROR: cached plan must not change result type (SQLSTATE 0A000)",
	}
	if !utils.IsTransientErr(errPartial) {
		t.Error("Expected true for cached plan error with additional context")
	}
}
