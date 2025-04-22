package errors

import (
	"errors"
	"fmt"
	"strings"
)

// TimeoutErr defines an error for when there is a resource Timeout
type TimeoutErr struct {
	error
}

// NewTimeoutErr returns a TimeoutErr
func NewTimeoutErr(reason string) error {
	return &TimeoutErr{error: New(reason)}
}

// IsTimeoutErr checks whether the specified error is a TimeoutErr
func IsTimeoutErr(err error) bool {
	var timeoutErr *TimeoutErr
	is := errors.As(err, &timeoutErr)
	return is
}

// ConvertToTimeoutErrorIfIOtimeout converts error to timeout error if error is IOtimeout
func ConvertToTimeoutErrorIfIOtimeout(err error, module string) error {
	if err != nil && (strings.Contains(err.Error(), "i/o timeout") || strings.Contains(err.Error(), "context deadline exceeded")) {
		if module == "" {
			return NewTimeoutErr("i/o timeout")
		}
		return NewTimeoutErr(fmt.Sprintf("%s call timeout.", module))
	}
	return err
}
