package errors

import (
	"fmt"
	"runtime/debug"
)

// NotFoundErr defines an error for when an object is not found
type NotFoundErr struct {
	objectType       string
	objectIdentifier *string
	stack            string
}

func (nfe *NotFoundErr) Error() string {
	id := ""
	if nfe.objectIdentifier != nil {
		id = fmt.Sprintf(" '%s'", *nfe.objectIdentifier)
	}
	return fmt.Sprintf("%s%s not found", nfe.objectType, id)
}

// NewNotFoundErr returns a NotFoundErr
func NewNotFoundErr(objectType string, objectIdentifier *string) error {
	return &NotFoundErr{objectType: objectType, objectIdentifier: objectIdentifier, stack: string(debug.Stack())}
}
