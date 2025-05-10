package errors

import "errors"

// NotSupportedErr defines an error for when an operation/feature is not supported
type NotSupportedErr struct {
	message string
}

func (nse *NotSupportedErr) Error() string {
	return nse.message
}

// NewNotSupportedErr returns a NotSupportedErr
func NewNotSupportedErr() error {
	return &NotSupportedErr{message: "Not supported"}
}

// NewNotSupportedErrWithMessage returns a NotSupportedErr with the specified error message
func NewNotSupportedErrWithMessage(msg string) error {
	return &NotSupportedErr{message: msg}
}

// IsNotSupportedErr checks whether the specified error is a NotSupportedErr
func IsNotSupportedErr(err error) bool {
	var notSupportedErr *NotSupportedErr
	var is = errors.As(err, &notSupportedErr)
	return is
}
