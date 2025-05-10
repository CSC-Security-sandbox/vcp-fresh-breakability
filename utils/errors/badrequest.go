package errors

// BadRequestErr defines an error for when there is a bad request
type BadRequestErr struct {
	error
	trackingID int
}

// NewBadRequestErr returns a BadRequestErr
func NewBadRequestErr(reason string) error {
	return &BadRequestErr{error: New(reason)}
}

// NewBadRequestErrWithTrackingID returns a BadRequestErr with specified trackingId
func NewBadRequestErrWithTrackingID(reason string, tID int) error {
	return &BadRequestErr{error: New(reason), trackingID: tID}
}

// IsBadRequestErr checks whether the specified error is a BadRequestErr
func IsBadRequestErr(err error) bool {
	_, is := err.(*BadRequestErr)
	return is
}

// GetTrackingID returns the tracking id for the error
func (uErr *BadRequestErr) GetTrackingID() int {
	return uErr.trackingID
}
