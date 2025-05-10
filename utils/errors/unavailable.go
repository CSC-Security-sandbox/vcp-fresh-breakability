package errors

import (
	"net/http"
	"strings"
)

var (
	networkErrors = []string{"connect: connection refused", "context deadline exceeded", "i/o timeout", "service seems to be down", ": eof", "cannot publish to disconnected MQ"}
)

// IsNetworkError checks if the error returned from cbs to cvs is a connectivity error
func IsNetworkError(err error) bool {
	if GetStatusCodeFromError(err) == http.StatusGatewayTimeout {
		return true
	}
	for _, errorString := range networkErrors {
		if strings.Contains(strings.ToLower(err.Error()), errorString) {
			return true
		}
	}
	return false
}

// UnavailableErr defines an error for when a resource is unavailable
type UnavailableErr struct {
	error
	trackingID int // error code for ANF
}

// NewUnavailableErr returns an UnavailableErr
func NewUnavailableErr(msg string) error {
	return &UnavailableErr{error: New(msg)}
}

// NewUnavailableErrWithTrackingID returns a UnavailableErr with specified trackingId
func NewUnavailableErrWithTrackingID(reason string, tID int) error {
	return &UnavailableErr{error: New(reason), trackingID: tID}
}

// IsUnavailableErr checks whether the specified error is an UnavailableErr
func IsUnavailableErr(err error) bool {
	_, is := err.(*UnavailableErr)
	return is
}

// GetTrackingID returns the tracking id for the error
func (e *UnavailableErr) GetTrackingID() int {
	return e.trackingID
}

// ConvertToUnavailableErrorIfConnectionIssues converts error to unavailable error if
// error is "Connection Refused"/"Context Deadline Exceeded"/ "i/o timeout" / "service seems to be down error returned from CBS"
func ConvertToUnavailableErrorIfConnectionIssues(err error) error {
	if err != nil && IsNetworkError(err) {
		return NewUnavailableErr("service seems to be down or taking too long to respond, please try again after some time")
	}
	return err
}
