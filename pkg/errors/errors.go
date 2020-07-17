package errors

import (
	"errors"
	"sync"
)

func New(text string) error {
	return errors.New(text)
}

// Error collection helper
type ErrorCollector struct {
	m      sync.Mutex
	errors []error
}

// Reports an error to the collector. Ignores if the error is nil.
func (ec *ErrorCollector) Report(e error) {
	if e == nil {
		return
	}

	ec.m.Lock()
	defer ec.m.Unlock()

	ec.errors = append(ec.errors, e)
}

// Whether or not the collector caught errors
func (ec *ErrorCollector) IsError() bool {
	ec.m.Lock()
	defer ec.m.Unlock()

	return len(ec.errors) > 0
}

// Errors caught by the collector
func (ec *ErrorCollector) Errors() []error {
	ec.m.Lock()
	defer ec.m.Unlock()

	errs := make([]error, len(ec.errors))
	copy(errs, ec.errors)
	return errs
}
