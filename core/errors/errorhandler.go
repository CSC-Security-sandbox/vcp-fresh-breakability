package errors

import (
	_ "embed"
	"encoding/json"
)

//go:embed errors.json
var errorsJSON []byte

// ErrorHandler contains settings of the error handler
type ErrorHandler struct {
}

func NewErrorHandler() (*ErrorHandler, error) {
	handler := &ErrorHandler{}
	err := handler.loadErrorMessages()
	if err != nil {
		return nil, err
	}
	return handler, nil
}

func (h *ErrorHandler) loadErrorMessages() error {
	err := json.Unmarshal(errorsJSON, &errorMap)
	if err != nil {
		return err
	}
	return nil
}
