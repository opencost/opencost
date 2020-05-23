package errors

import (
	"fmt"
	"runtime"
)

// Panic represents a panic that occurred, captured by a recovery.
type Panic struct {
	Error interface{}
	Stack string
}

// PanicHandler is a func that receives a Panic and returns a bool representing whether or not
// the panic should recover or not.
type PanicHandler = func(p Panic) bool

var (
	enabled    = false
	dispatcher = make(chan Panic)
)

// SetPanicHandler sets the handler that is executed when
func SetPanicHandler(handler PanicHandler) error {
	if enabled {
		return fmt.Errorf("Panic Handler has already been set")
	}

	enabled = true

	// Setup a go routine which receives via the panic channel, passes
	// resulting Panic to the handler passed.
	go func() {
		for {
			p := <-dispatcher

			// If we do not wish to recover, panic using same error
			if !handler(p) {
				panic(p.Error)
			}
		}
	}()

	return nil
}

// HandlePanic should be executed in a deferred method (or deferred directly). It will
// capture any panics that occur in the goroutine it exists, and report to the registered
// global panic handler.
func HandlePanic() {
	// Do not handle/recover if disabled
	if !enabled {
		return
	}

	if err := recover(); err != nil {
		dispatch(err)
	}
}

// generate stacktrace, dispatch the panic via channel
func dispatch(err interface{}) {
	stack := make([]byte, 1024*8)
	stack = stack[:runtime.Stack(stack, false)]

	dispatcher <- Panic{
		Error: err,
		Stack: string(stack),
	}
}
