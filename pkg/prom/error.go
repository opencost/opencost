package prom

import (
	"fmt"
	"strings"
)

// WrapError wraps the given error with the given message, usually for adding
// context, but persists the existing type of error.
func WrapError(err error, msg string) error {
	switch e := err.(type) {
	case CommError:
		return e.Wrap(msg)
	case NoDataError:
		return e.Wrap(msg)
	default:
		return fmt.Errorf("%s: %s", msg, err)
	}
}

// CommError describes an error communicating with Prometheus
type CommError struct {
	messages []string
}

// NewCommError creates a new CommError
func NewCommError(messages ...string) CommError {
	return CommError{messages: messages}
}

// IsCommError returns true if the given error is a CommError
func IsCommError(err error) bool {
	_, ok := err.(CommError)
	return ok
}

// Error prints the error as a string
func (pce CommError) Error() string {
	return fmt.Sprintf("Prometheus communication error: %s", strings.Join(pce.messages, ": "))
}

// Wrap wraps the error with the given message, but persists the error type.
func (pce CommError) Wrap(message string) CommError {
	pce.messages = append([]string{message}, pce.messages...)
	return pce
}

// NoDataError indicates that no data was returned by Prometheus. This should
// be treated like an EOF error, in that it may be expected.
type NoDataError struct {
	messages []string
}

// NewNoDataError creates a new NoDataError
func NewNoDataError(messages ...string) NoDataError {
	return NoDataError{messages: messages}
}

// IsNoDataError returns true if the given error is a NoDataError
func IsNoDataError(err error) bool {
	_, ok := err.(NoDataError)
	return ok
}

// Error prints the error as a string
func (nde NoDataError) Error() string {
	return fmt.Sprintf("No data error: %s", strings.Join(nde.messages, ": "))
}

// Wrap wraps the error with the given message, but persists the error type.
func (nde NoDataError) Wrap(message string) NoDataError {
	nde.messages = append([]string{message}, nde.messages...)
	return nde
}
