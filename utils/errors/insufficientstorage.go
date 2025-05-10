package errors

import "errors"

// InsufficientStorageErr defines an error for when there's a lack of available storage space.
type InsufficientStorageErr struct {
	error
	trackingID int
}

// NewInsufficientStorageErr returns a new InsufficientStorageErr error
func NewInsufficientStorageErr(reason string) error {
	return &InsufficientStorageErr{error: New(reason)}
}

// NewInsufficientStorageErrWithTrackingID returns a standard error with tracking ID
func NewInsufficientStorageErrWithTrackingID(reason string, tID int) error {
	return &InsufficientStorageErr{error: New(reason), trackingID: tID}
}

// IsInsufficientStorageErrErr checks whether the specified error is a InsufficientStorageErr error
func IsInsufficientStorageErrErr(err error) bool {
	var insufficientStorageErr *InsufficientStorageErr
	is := errors.As(err, &insufficientStorageErr)
	return is
}

// GetTrackingID returns the tracking id for the error
func (ise *InsufficientStorageErr) GetTrackingID() int {
	return ise.trackingID
}
