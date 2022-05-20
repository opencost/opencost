package metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/kubecost/events"
)

// ResponseMetricMiddleware dispatches metric events for handles request and responses.
func ResponseMetricMiddleware(handler http.Handler) http.Handler {
	dispatcher := events.GlobalDispatcherFor[HttpHandlerMetricEvent]()

	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// use a ResponseWriter implementation to record telemetry for the response
		respWriter := &responseWriterAdapter{w: rw}

		// record method and path of the request
		method := r.Method
		path := r.URL.Path

		// time and execute the handler
		start := time.Now()
		handler.ServeHTTP(respWriter, r)
		duration := time.Since(start)

		// record the response code and size
		code := respWriter.StatusCode()
		size := respWriter.TotalResponseSize()

		dispatcher.Dispatch(HttpHandlerMetricEvent{
			Handler:      path,
			Method:       method,
			Code:         code,
			ResponseTime: duration,
			ResponseSize: size,
		})

	})
}

// responseWriterAdapter implements http.ResponseWriter and extracts the statusCode.
type responseWriterAdapter struct {
	w          http.ResponseWriter
	written    bool
	statusCode int
	size       uint64
}

func (wd *responseWriterAdapter) Header() http.Header {
	return wd.w.Header()
}

func (wd *responseWriterAdapter) Write(bytes []byte) (int, error) {
	numBytes, err := wd.w.Write(bytes)
	wd.size += uint64(numBytes)
	return numBytes, err
}

func (wd *responseWriterAdapter) WriteHeader(statusCode int) {
	wd.written = true
	wd.statusCode = statusCode
	wd.w.WriteHeader(statusCode)
}

func (wd *responseWriterAdapter) StatusCode() int {
	if !wd.written {
		return http.StatusOK
	}
	return wd.statusCode
}

func (wd *responseWriterAdapter) Status() string {
	return fmt.Sprintf("%d", wd.StatusCode())
}

func (wd *responseWriterAdapter) TotalResponseSize() uint64 {
	return wd.size
}
