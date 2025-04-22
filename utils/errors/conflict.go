package errors

import (
	"errors"
)

// ConflictErr defines an error for when there is a resource conflict
type ConflictErr struct {
	error
}

// NewConflictErr returns a ConflictErr
func NewConflictErr(reason string) error {
	return &ConflictErr{error: New(reason)}
}

// IsConflictErr checks whether the specified error is a ConflictErr
func IsConflictErr(err error) bool {
	var conflictErr *ConflictErr
	is := errors.As(err, &conflictErr)
	return is
}
