package errors

import (
	"fmt"
	"strings"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// NotFoundErr defines an error for when an object is not found
type NotFoundErr struct {
	objectType       string
	objectIdentifier *string
	trackingID       int
}

func (nfe *NotFoundErr) Error() string {
	id := ""
	if nfe.objectIdentifier != nil {
		id = fmt.Sprintf(" '%s'", *nfe.objectIdentifier)
	}
	return fmt.Sprintf("%s%s not found", nfe.objectType, id)
}

// Is compares errors
func (nfe *NotFoundErr) Is(target error) bool {
	notFoundErr, is := target.(*NotFoundErr)
	if !is {
		return false
	}
	if nfe.objectType != notFoundErr.objectType {
		return false
	}
	if nillable.GetString(nfe.objectIdentifier, "") != nillable.GetString(notFoundErr.objectIdentifier, "") {
		return false
	}
	if nfe.trackingID != notFoundErr.trackingID {
		return false
	}
	return true
}

// NewNotFoundErr returns a NotFoundErr
func NewNotFoundErr(objectType string, objectIdentifier *string) error {
	return &NotFoundErr{objectType: objectType, objectIdentifier: objectIdentifier}
}

// NewNotFoundErrWithTrackingID returns a NotFoundErr with tracking ID
func NewNotFoundErrWithTrackingID(objectType string, objectIdentifier *string, tID int) error {
	return &NotFoundErr{objectType: objectType, objectIdentifier: objectIdentifier, trackingID: tID}
}

// ConvertToNotFoundErrIfContainsMessage converts the specified error into
// a NotFoundErr if specified message is contained within the error
func ConvertToNotFoundErrIfContainsMessage(err error, message, objectType string, objectIdentifier *string) error {
	if err != nil && strings.Contains(err.Error(), message) {
		return NewNotFoundErr(objectType, objectIdentifier)
	}
	return err
}

// IsNotFoundErr checks whether the specified error is a NotFoundErr
func IsNotFoundErr(err error) bool {
	var notFoundErr *NotFoundErr
	is := vsaerrors.As(err, &notFoundErr)
	return is
}

// IsNotFoundErrForObjectType checks whether the specified error is a NotFoundErr for the specified object type.
func IsNotFoundErrForObjectType(err error, objectType string) bool {
	if nfe, is := err.(*NotFoundErr); is {
		return strings.EqualFold(nfe.objectType, objectType)
	}
	return false
}

// IsNotFoundErrForObjectTypeInChain checks whether the error chain contains a NotFoundErr for the specified object type.
// It uses As to unwrap the error chain, so it works when the error is wrapped (e.g. by VCPError).
// Use this when the error may be wrapped; use IsNotFoundErrForObjectType for unwrapped errors.
func IsNotFoundErrForObjectTypeInChain(err error, objectType string) bool {
	var notFoundErr *NotFoundErr
	if vsaerrors.As(err, &notFoundErr) {
		return strings.EqualFold(notFoundErr.objectType, objectType)
	}
	return false
}

// GetTrackingID returns the tracking id for the error
func (nfe *NotFoundErr) GetTrackingID() int {
	return nfe.trackingID
}
