package errors

import (
	"fmt"

	"github.com/mailgun/holster/stack"
)

// Implement this interface to pass along unstructured context to the logger
type HasContext interface {
	Context() map[string]interface{}
}

// True if the interface has the format method (from fmt package)
type HasFormat interface {
	Format(st fmt.State, verb rune)
}

// Creates errors that conform to the `HasContext` interface
type WithContext map[string]interface{}

func (c WithContext) Wrapf(err error, format string, args ...interface{}) error {
	return &withContext{
		stack:   stack.New(1),
		context: c,
		cause:   err,
		msg:     fmt.Sprintf(format, args...),
	}
}

func (c WithContext) Wrap(err error, msg string) error {
	return &withContext{
		stack:   stack.New(1),
		context: c,
		cause:   err,
		msg:     msg,
	}
}

func (c WithContext) Error(msg string) error {
	return &withContext{
		stack:   stack.New(1),
		context: c,
		cause:   fmt.Errorf(msg),
		msg:     "",
	}
}

func (c WithContext) Errorf(format string, args ...interface{}) error {
	return &withContext{
		stack:   stack.New(1),
		context: c,
		cause:   fmt.Errorf(format, args...),
		msg:     "",
	}
}
