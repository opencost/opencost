package errors

import (
	"fmt"
	"net/http"
	"runtime"
)

//--------------------------------------------------------------------------
//  PanicType
//--------------------------------------------------------------------------

// PanicType defines the context in which the panic occurred
type PanicType int

const (
	PanicTypeDefault PanicType = iota
	PanicTypeHTTP
)

// The string representation of PanicContext
func (pt PanicType) String() string {
	return []string{"PanicTypeDefault", "PanicTypeHTTP"}[pt]
}

//--------------------------------------------------------------------------
//  Panic
//--------------------------------------------------------------------------

// Panic represents a panic that occurred, captured by a recovery.
type Panic struct {
	Error interface{}
	Stack string
	Type  PanicType
}

// PanicHandler is a func that receives a Panic and returns a bool representing whether or not
// the panic should recover or not.
type PanicHandler = func(p Panic) bool

var (
	enabled    = false
	dispatcher = make(chan Panic)
)

// SetPanicHandler sets the handler that is executed when any panic is captured by
// HandlePanic(). Without setting a handler, the panic reporting is disabled.
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

// PanicHandlerMiddleware should wrap any of the http handlers to capture panics.
func PanicHandlerMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, rq *http.Request) {
		defer HandleHTTPPanic(rw, rq)

		handler.ServeHTTP(rw, rq)
	})
}

// HandlePanic should be executed in a deferred method (or deferred directly). It will
// capture any panics that occur in the goroutine it exists, and report to the registered
// global panic handler.
func HandlePanic() {
	// NOTE: For each "special" type of panic that is added, you must repeat this pattern. The recover()
	// NOTE: call cannot exist in a func outside of the deferred func.
	if !enabled {
		return
	}

	if err := recover(); err != nil {
		dispatch(err, PanicTypeDefault)
	}
}

// HandleHTTPPanic should be executed in a deferred method (or deferred directly) in http middleware.
// It will capture any panics that occur in the goroutine it exists, and report to the registered
// global panic handler. HTTP handler panics will have the errors.PanicTypeHTTP Type.
func HandleHTTPPanic(rw http.ResponseWriter, rq *http.Request) {
	// NOTE: For each "special" type of panic that is added, you must repeat this pattern. The recover()
	// NOTE: call cannot exist in a func outside of the deferred func.
	if !enabled {
		return
	}

	if err := recover(); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)

		dispatch(err, PanicTypeHTTP)
	}
}

// generate stacktrace, dispatch the panic via channel
func dispatch(err interface{}, panicType PanicType) {
	stack := make([]byte, 1024*8)
	stack = stack[:runtime.Stack(stack, false)]

	dispatcher <- Panic{
		Error: err,
		Stack: string(stack),
		Type:  panicType,
	}
}
