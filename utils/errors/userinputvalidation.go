package errors

import vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"

// UserInputValidationErr defines an error for when the user input is invalid
type UserInputValidationErr struct {
	error
	trackingID int
}

// NewUserInputValidationErr returns a UserInputValidationErr
func NewUserInputValidationErr(reason string) error {
	return &UserInputValidationErr{error: New(reason)}
}

// NewUserInputValidationErrWithTrackingID returns a UserInputValidationErr with specified trackingId
func NewUserInputValidationErrWithTrackingID(reason string, tID int) error {
	return &UserInputValidationErr{error: New(reason), trackingID: tID}
}

// IsUserInputValidationErr checks whether the specified error is a UserInputValidationErr
func IsUserInputValidationErr(err error) bool {
	var userInputValidationErr *UserInputValidationErr
	is := vsaerrors.As(err, &userInputValidationErr)
	return is
}

// GetTrackingID returns the tracking id for the error
func (uErr *UserInputValidationErr) GetTrackingID() int {
	return uErr.trackingID
}
