package errors

import (
	"errors"
	"fmt"
)

// StandardErr defines an error for when a standard error happens
type StandardErr struct {
	error
	trackingID int // error code for ANF
}

// New returns a standard error
func New(text string) error {
	return errors.New(text)
}

// NewWithTrackingID returns a standard error with tracking ID
func NewWithTrackingID(text string, code int) error {
	return &StandardErr{error: New(text), trackingID: code}
}

// Errorf formats according to a format specifier and returns the string
// as a value that satisfies error.
// Supports wrapping errors as described: https://go.dev/blog/go1.13-errors
func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}

// GetTrackingID returns the tracking ID for the error
func (e *StandardErr) GetTrackingID() int {
	return e.trackingID
}
