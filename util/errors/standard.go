package errors

import (
	"errors"
	"runtime/debug"
)

type defaultErr struct {
	error
	stack string
}

// New returns a standard error with a stack trace
func New(text string) error {
	// TODO: add a boolean to remove the stack? what for? performance reasons?
	return &defaultErr{error: errors.New(text), stack: string(debug.Stack())}
}
