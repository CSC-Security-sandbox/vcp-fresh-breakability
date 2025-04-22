package errors

// NotReadyErr defines an error that indicates when a resource is not ready
type NotReadyErr struct {
	error
}

// NewNotReadyErr returns a new NotReadyErr error
func NewNotReadyErr(reason string) error {
	return &NotReadyErr{error: New(reason)}
}

// IsNotReadyErr checks whether the specified error is a NotReadyErr error
func IsNotReadyErr(err error) bool {
	_, is := err.(*NotReadyErr)
	return is
}
