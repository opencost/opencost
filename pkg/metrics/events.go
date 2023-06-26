package metrics

import "time"

// HttpHandlerMetricEvent contains http handler response metrics.
type HttpHandlerMetricEvent struct {
	Handler      string
	Method       string
	Code         int
	ResponseTime time.Duration
	ResponseSize uint64
}
