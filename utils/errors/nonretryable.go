package errors

import vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"

// NonRetryableErr defines an error which cannot be retried
type NonRetryableErr struct {
	error
	trackingID int
}

// NewNonRetryableErr returns a NonRetryableErr
func NewNonRetryableErr(reason string) error {
	return &NonRetryableErr{error: New(reason)}
}

// NewNonRetryableErrWithTrackingID returns a NonRetryableErr with specified trackingId
func NewNonRetryableErrWithTrackingID(reason string, tID int) error {
	return &NonRetryableErr{error: New(reason), trackingID: tID}
}

// IsNonRetryableErr checks whether the specified error is a NonRetryableErr
func IsNonRetryableErr(err error) bool {
	var nonRetryableError *NonRetryableErr
	is := vsaerrors.As(err, &nonRetryableError)
	return is
}

// GetTrackingID returns the tracking id for the error
func (uErr *NonRetryableErr) GetTrackingID() int {
	return uErr.trackingID
}
