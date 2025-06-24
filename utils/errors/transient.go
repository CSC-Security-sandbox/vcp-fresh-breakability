package errors

import (
	"fmt"
)

// TransientErr defines an error that is transient
type TransientErr struct {
	reason string
}

func (te *TransientErr) Error() string {
	return fmt.Sprintf("Transient error: %s", te.reason)
}

// NewTransientErr returns a TransientErr
func NewTransientErr(reason string) error {
	return &TransientErr{reason: reason}
}

// IsTransientErr checks whether the specified error is a TransientErr
func IsTransientErr(err error) bool {
	_, is := err.(*TransientErr)
	return is
}
