package errors

import "errors"

// GoneErr defines an error for when a resource is gone
type GoneErr struct {
	error
}

// NewGoneErr returns a GoneErr
func NewGoneErr(reason string) error {
	return &GoneErr{error: New(reason)}
}

// IsGoneErr checks whether the specified error is a GoneErr
func IsGoneErr(err error) bool {
	var goneErr *GoneErr
	is := errors.As(err, &goneErr)
	return is
}
