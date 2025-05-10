package errors

import "errors"

// NotReadyErr defines an error that indicates when a resource is not ready
type NotReadyErr struct {
	error
	trackingID int // error code for ANF
}

// NewNotReadyErr returns a new NotReadyErr error
func NewNotReadyErr(reason string) error {
	return &NotReadyErr{error: New(reason)}
}

// NewNotReadyErrWithTrackingID returns a standard error with tracking ID
func NewNotReadyErrWithTrackingID(reason string, tID int) error {
	return &NotReadyErr{error: New(reason), trackingID: tID}
}

// IsNotReadyErr checks whether the specified error is a NotReadyErr error
func IsNotReadyErr(err error) bool {
	var notReadyErr *NotReadyErr
	var is = errors.As(err, &notReadyErr)
	return is
}

// GetTrackingID returns the tracking ID for the error
func (e *NotReadyErr) GetTrackingID() int {
	return e.trackingID
}
