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

// init loads error messages from the JSON file when the package is imported
func init() {
	err := json.Unmarshal(errorsJSON, &errorMap)
	if err != nil {
		panic("Failed to load error messages from JSON file: " + err.Error())
	}
}
