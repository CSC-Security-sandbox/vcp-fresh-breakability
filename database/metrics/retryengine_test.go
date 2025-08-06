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
