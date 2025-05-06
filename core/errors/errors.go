package errors

import (
	"errors"
	"fmt"
	"log/slog"

	"go.uber.org/multierr"
)

const (
	ErrInvalidInput  = "InvalidInput"
	ErrNotFound      = "NotFound"
	ErrInternalError = "InternalError"
)

var (
	Combine = multierr.Combine
	New     = errors.New
	Newf    = fmt.Errorf
)

type Error interface {
	error
}

// ErrorMessage struct represents the structure of each error message in the JSON file.
type ErrorMessage struct {
	TrackingID int    `json:"tracking_id"`
	Message    string `json:"message"`
	Retriable  *bool  `json:"retriable,omitempty"`
	HttpCode   *int   `json:"http_code,omitempty"`
}

// errorMap is a map of error names to their corresponding ErrorMessage.
var errorMap map[string]ErrorMessage

// CustomError is our custom error type that includes an error code and retriable flag.
type CustomError struct {
	ErrorName   string
	TrackingID  int
	Message     string
	Retriable   bool
	HttpCode    *int  // HttpCode is the HTTP code associated with the error
	OriginalErr error // OriginalErr holds the original error in case this is a wrapped error
}

// Error implements the error interface for CustomError.
func (e *CustomError) Error() string {
	return fmt.Sprintf("[%d] %s", e.TrackingID, e.Message)
}

// Unwrap returns the originalErr error if there is one.
func (e *CustomError) Unwrap() error {
	return e.OriginalErr
}

// IsRetriable returns true if the error is marked as retriable.
func (e *CustomError) IsRetriable() bool {
	return e.Retriable
}

// IsError returns true if the error name or TrackingID is same as queried name or TrackingID.
func (e *CustomError) IsError(errorNameOrTrackingID string) bool {
	return e.ErrorName == errorNameOrTrackingID || fmt.Sprintf("%d", e.TrackingID) == errorNameOrTrackingID
}

// LogError logs the error message along with its TrackingID.
func (e *CustomError) LogError() {
	slog.String("Error", e.Error())
}

// GetHttpCode returns the HTTP code associated with the error.
func (e *CustomError) GetHttpCode() (bool, int) {
	if e.HttpCode != nil {
		return true, *e.HttpCode
	}
	return false, 400 // Default HTTP code if not specified
}

// LogOriginalError logs the Original error message along with its code.
func (e *CustomError) LogOriginalError() {
	if e.OriginalErr != nil {
		slog.String("Original Error", e.OriginalErr.Error())
	}
}

// NewVCPError creates a new CustomError based on the given error name.
func NewVCPError(errorName string, originalErr error) Error {
	if errMsg, ok := errorMap[errorName]; ok {
		if errMsg.Retriable == nil {
			// Default to false if retriable is not specified in the JSON file.
			errMsg.Retriable = new(bool)
			*errMsg.Retriable = false
		}

		return &CustomError{
			ErrorName:   errorName,
			TrackingID:  errMsg.TrackingID,
			Message:     errMsg.Message,
			Retriable:   *errMsg.Retriable,
			HttpCode:    errMsg.HttpCode,
			OriginalErr: originalErr,
		}
	}
	// If the error name is not defined, create a generic non-retriable error with the original error.
	return &CustomError{
		ErrorName:   "NotDefined",
		TrackingID:  0,
		Message:     fmt.Sprintf("undefined error: %s", originalErr.Error()),
		Retriable:   false,
		OriginalErr: originalErr,
	}
}

// Is reports whether any error in err's tree matches target.
func Is(err error, target error) bool {
	return errors.Is(err, target)
}

// As finds the first error in err's tree that matches target, and if one is found, sets
// target to that error value and returns true. Otherwise, it returns false.
func As(err error, target any) bool {
	return errors.As(err, target)
}
