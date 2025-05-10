package errors

import (
	"errors"
)

// ConflictErr defines an error for when there is a resource conflict
type ConflictErr struct {
	error
	trackingID int // error code for ANF
}

// NewConflictErr returns a ConflictErr
func NewConflictErr(reason string) error {
	return &ConflictErr{error: New(reason)}
}

// NewConflictErrWithTrackingID returns a ConflictErr with specified trackingId
func NewConflictErrWithTrackingID(reason string, tID int) error {
	return &ConflictErr{error: New(reason), trackingID: tID}
}

// IsConflictErr checks whether the specified error is a ConflictErr
func IsConflictErr(err error) bool {
	var conflictErr *ConflictErr
	is := errors.As(err, &conflictErr)
	return is
}

// GetTrackingID returns the tracking id for the error
func (e *ConflictErr) GetTrackingID() int {
	return e.trackingID
}
