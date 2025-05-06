package errors

import (
	"encoding/json"
	"os"
)

// ErrorHandler contains settings of the error handler
type ErrorHandler struct {
}

func NewErrorHandler(configPath string) (*ErrorHandler, error) {
	handler := &ErrorHandler{}
	err := handler.loadErrorMessages(configPath)
	if err != nil {
		return nil, err
	}
	return handler, nil
}

func (h *ErrorHandler) loadErrorMessages(configPath string) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &errorMap)
	if err != nil {
		return err
	}
	return nil
}
