package active_directory_activities

import (
	"errors"
	"fmt"

	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
)

// operationError represents a failed long-running operation (e.g. from V1betaDescribeOperation poll).
// It carries code and message only; GetCvpErrorCodeAndMessage handles it alongside CVP API errors.
type operationError struct {
	code    int
	message string
}

func (e *operationError) Error() string {
	if e.message != "" {
		return e.message
	}
	return "operation failed"
}

// NewOperationError builds an error from a poll result (code + message). Pass to WrapCvpError so handling is by err type.
func NewOperationError(code int, message string) error {
	if message == "" {
		message = "operation failed"
	}
	return &operationError{code: code, message: message}
}

// CvpApiError is implemented by CVP API error response types (go-swagger). It provides access to the
// structured error body (HTTP status code and message) so callers can handle any CVP API error
// (AD, Volume, GetMultiple, etc.) without depending on concrete response types.
type CvpApiError interface {
	GetPayload() *cvpModels.Error
}

// GetCvpErrorCodeAndMessage extracts HTTP status code and message from err. It handles:
//   - operationError: internal poll-result errors (code and message fields).
//   - Any CVP API error: walks the error chain and returns the first structured payload (Code, Message).
//
// Wrapped errors are supported. Use for generic CVP error handling across APIs.
func GetCvpErrorCodeAndMessage(err error) (code int, message string, ok bool) {
	if err == nil {
		return 0, "", false
	}
	var opErr *operationError
	if errors.As(err, &opErr) {
		return opErr.code, opErr.message, true
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		apiErr, ok := e.(CvpApiError)
		if !ok {
			continue
		}
		payload := apiErr.GetPayload()
		if payload == nil {
			continue
		}
		msg := payload.Message
		if msg == "" {
			msg = "operation failed"
		}
		return int(payload.Code), msg, true
	}
	return 0, "", false
}

// wrapCvpErrorByHTTPCodeAndMessage maps an HTTP status code and message (from CVP or poll) to a VCP CustomError and wraps for Temporal.
// When forceNonRetryable is true, the error is always wrapped as non-retryable regardless of status code.
// Use forceNonRetryable=true for terminal operation results (Done=true) where retrying cannot change the outcome.
func wrapCvpErrorByHTTPCodeAndMessage(code int, message string, forceNonRetryable bool) error {
	if message == "" {
		message = "operation failed"
	}
	origErr := fmt.Errorf("%s", message)

	var trackingID int
	retryable := false
	switch code {
	case common.HTTPStatusBadRequest:
		trackingID = vsaerrors.ErrCVPBadRequest
	case common.HTTPStatusUnauthorized:
		trackingID = vsaerrors.ErrCVPUnauthorized
	case common.HTTPStatusForbidden:
		trackingID = vsaerrors.ErrCVPForbidden
	case common.HTTPStatusNotFound:
		trackingID = vsaerrors.ErrCVPNotFound
	case common.HTTPStatusConflict:
		trackingID = vsaerrors.ErrCVPConflict
	case common.HTTPStatusUnprocessableEntity:
		trackingID = vsaerrors.ErrCVPUnprocessableEntity
	case common.HTTPStatusTooManyRequests:
		trackingID = vsaerrors.ErrCVPTooManyRequests
		retryable = true
	case common.HTTPStatusInternalServerError:
		trackingID = vsaerrors.ErrCVPInternalServerError
		retryable = true
	default:
		trackingID = vsaerrors.ErrCVPInternalServerError
		retryable = true
	}

	if forceNonRetryable {
		retryable = false
	}

	ce := vsaerrors.NewVCPError(trackingID, origErr)
	if retryable {
		return vsaerrors.WrapAsTemporalApplicationError(ce)
	}
	return vsaerrors.WrapAsNonRetryableTemporalApplicationError(ce)
}

// WrapCvpError converts a CVP API error (or operationError) into a VCP CustomError and wraps for Temporal.
// Use after any direct CVP API call (e.g. AD Create/Update/Delete) where transient 500s should be retried.
func WrapCvpError(err error) error {
	code, message, ok := GetCvpErrorCodeAndMessage(err)
	if !ok {
		// Non-structured error (transport failure, timeout, etc.) — wrap as retryable
		// so Temporal retries transient CVP/network failures instead of permanently failing.
		return vsaerrors.WrapAsTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrCVPInternalServerError, err))
	}
	return wrapCvpErrorByHTTPCodeAndMessage(code, message, false)
}

// WrapCvpErrorNonRetryable wraps a terminal operation error (from a completed SDE poll) as non-retryable.
// Use when the SDE operation has completed (Done=true) with an error — retrying cannot change the outcome.
func WrapCvpErrorNonRetryable(code int, message string) error {
	return wrapCvpErrorByHTTPCodeAndMessage(code, message, true)
}
