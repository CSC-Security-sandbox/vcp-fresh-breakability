package errors

import (
	"errors"
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/temporal"
	"go.uber.org/multierr"
)

var (
	Combine = multierr.Combine
	New     = errors.New
	Newf    = fmt.Errorf
)

const (
	ErrWorkflowConfigurationError = 1001
	ErrBadRequest                 = 1002
	ErrResourceNotFound           = 1003
	ErrFileReadError              = 1004
	ErrFileWriteError             = 1005
	ErrJSONParsingError           = 1006
	ErrMaxRetriesExceeded         = 1007
	ErrTimeLimitExceeded          = 1008

	ErrDatabaseConnectionClosed = 2001
	ErrDatabaseTransactionError = 2002
	ErrDatabaseDataInsertError  = 2003
	ErrDatabaseDataReadError    = 2004
	ErrDatabaseDataUpdateError  = 2005
	ErrDatabaseDataDeleteError  = 2006

	ErrGCPClientInitializationError  = 3001
	ErrPSAPeeringNotFoundError       = 3002
	ErrGCPResourceProvisionError     = 3003
	ErrGCPResourceFetchError         = 3004
	ErrGCPResourceDeprovisionError   = 3005
	ErrGCPResourceAlreadyExistsError = 3006

	ErrVSAClusterCreateError          = 4001
	ErrCouldNotFetchVSAClusterDetails = 4002
	ErrVSAClusterDeleteError          = 4003
	ErrIncorrectVSAClusterState       = 4004
	ErrVSAClusterNodeNotFound         = 4005

	ErrONTAPVersionFetchError = 5001
	ErrCreatingSVM            = 5002
	ErrDeletingSVM            = 5003
	ErrSVMNotFound            = 5004

	ErrIamClientNotFoundError      = 6020
	ErrFailedToParseProjectNumber  = 6021
	ErrFailedToMarshalPayload      = 6022
	ErrFailedToMarshalJson         = 6023
	ErrFailedToCreateHTTP          = 6024
	ErrFailedToExecuteHTTP         = 6025
	ErrFailedToReadResponse        = 6026
	ErrFailedToUnmarshalCCFE       = 6027
	ErrFailedToReadQuota           = 6028
	ErrFailedToCreateNewIamCred    = 6029
	ErrFailedToGenerateAccessToken = 6030
)

type Error interface {
	error
}

// ErrorMessage struct represents the structure of each error message in the JSON file.
type ErrorMessage struct {
	Description string `json:"description"`
	Message     string `json:"message"`
	Retriable   *bool  `json:"retriable,omitempty"`
	HttpCode    *int   `json:"http_code,omitempty"`
}

// errorMap is a map of error names to their corresponding ErrorMessage.
var errorMap map[int]ErrorMessage

// CustomError is our custom error type that includes an error code and retriable flag.
type CustomError struct {
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

// IsError returns true if the TrackingID is same as queried TrackingID.
func (e *CustomError) IsError(trackingID int) bool {
	return e.TrackingID == trackingID
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

func (e *CustomError) GetMessage() string {
	return e.Message
}

// LogOriginalError logs the Original error message along with its code.
func (e *CustomError) LogOriginalError() {
	if e.OriginalErr != nil {
		slog.String("Original Error", e.OriginalErr.Error())
	}
}

// NewVCPError creates a new CustomError based on the given error name.
func NewVCPError(trackingID int, originalErr error) Error {
	if errMsg, ok := errorMap[trackingID]; ok {
		if errMsg.Retriable == nil {
			// Default to false if retriable is not specified in the JSON file.
			errMsg.Retriable = new(bool)
			*errMsg.Retriable = false
		}

		return &CustomError{
			TrackingID:  trackingID,
			Message:     errMsg.Message,
			Retriable:   *errMsg.Retriable,
			HttpCode:    errMsg.HttpCode,
			OriginalErr: originalErr,
		}
	}
	// If the error name is not defined, create a generic non-retriable error with the original error.
	return &CustomError{
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

// GetErrorMessageByTrackingID returns the error details pertaining to the given TrackingID.
func GetErrorMessageByTrackingID(trackingID int) *ErrorMessage {
	if errMsg, ok := errorMap[trackingID]; ok {
		return &errMsg
	}

	httpCode := new(int)
	*httpCode = 500
	return &ErrorMessage{HttpCode: httpCode, Message: "undefined error"}
}

// WrapAsTemporalApplicationError wraps a given error as a Temporal application error if it is a CustomError.
// Otherwise, it returns the original error unchanged.
func WrapAsTemporalApplicationError(err error) error {
	var customError *CustomError
	if As(err, &customError) {
		return temporal.NewApplicationError(err.Error(), "CustomError", customError.TrackingID, customError.OriginalErr.Error())
	}

	return err
}
