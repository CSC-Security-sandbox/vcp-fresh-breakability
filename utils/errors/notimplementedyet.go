package errors

// NotImplementedYetErr defines an error for when an operation/feature is not implemented yet
type NotImplementedYetErr struct{}

func (nse *NotImplementedYetErr) Error() string {
	return "Not implemented yet"
}

// NewNotImplementedYetErr returns a NotImplementedYetErr
func NewNotImplementedYetErr() error {
	return &NotImplementedYetErr{}
}

// IsNotImplementedYetErr checks whether the specified error is a NotImplementedYetErr
func IsNotImplementedYetErr(err error) bool {
	_, is := err.(*NotImplementedYetErr)
	return is
}
