package metrics

import "time"

// HttpHandlerMetricEvent contains http handler response metrics.
type HttpHandlerMetricEvent struct {
	Handler      string
	Code         int
	Method       string
	ResponseTime time.Duration
	ResponseSize uint64
}
