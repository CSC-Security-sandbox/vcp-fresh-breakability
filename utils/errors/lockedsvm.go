package errors

// SvmLockedError defines an error for when a svm is locked
type SvmLockedError struct {
	error
}

// NewSvmLockedError returns an SvmLockedError
func NewSvmLockedError() error {
	return &SvmLockedError{error: New("Unable to perform operation. Wait a few minutes, and then try the operation again. If the error persists, contact technical support for assistance")}
}

// NewSvmLockedErrorWithTrackingID returns an SvmLockedError with trackingID
func NewSvmLockedErrorWithTrackingID() error {
	return &SvmLockedError{
		error: New("Unable to perform operation. Wait a few minutes, and then try the operation again. If the error persists, contact technical support for assistance"),
	}
}

// IsSvmLockedError checks whether the specified error is an SvmLockedError
func IsSvmLockedError(err error) bool {
	_, is := err.(*SvmLockedError)
	return is
}

// SvmDegradedError defines an error for when a svm is degraded
// Use only for marking SVMs as degraded in the DB
type SvmDegradedError struct {
	error
}

// NewSvmDegradedError returns an SvmDegradedError
func NewSvmDegradedError() error {
	return &SvmDegradedError{error: New("Unable to perform operation. Wait a few minutes, and then try the operation again. If the error persists, contact technical support for assistance")}
}

// IsSvmDegradedError checks whether the specified error is an SvmDegradedError
func IsSvmDegradedError(err error) bool {
	_, is := err.(*SvmDegradedError)
	return is
}
