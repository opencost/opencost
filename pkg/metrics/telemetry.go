package metrics

import (
	"fmt"
	"sync"

	"github.com/kubecost/events"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	once       sync.Once
	dispatcher events.Dispatcher[HttpHandlerMetricEvent]
	// -- append new dispatchers here for new event types

	// prometheus metrics
	requestsCount *prometheus.CounterVec
	responseTime  *prometheus.HistogramVec
	responseSize  *prometheus.SummaryVec
)

// InitKubecostTelemetry registers kubecost application telemetry.
func InitKubecostTelemetry(config *MetricsConfig) {
	// TODO(bolt): Check MetricsConfig for disabled metrics

	once.Do(func() {
		// register prometheus metrics
		requestsCount = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kubecost_http_requests_total",
			Help: "kubecost_http_requests_total Total number of HTTP requests",
		}, []string{"handler", "method", "code"})

		var buckets = []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120, 240, 360, 720}
		responseTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kubecost_http_response_time_seconds",
			Help:    "kubecost_http_response_time_seconds Response time in seconds",
			Buckets: buckets,
		}, []string{"handler", "method", "code"})

		responseSize = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "kubecost_http_response_size_bytes",
			Help: "kubecost_http_response_size_bytes Response size in bytes",
		}, []string{"handler", "method", "code"})

		prometheus.MustRegister(requestsCount, responseTime, responseSize)

		// register event listeners
		dispatcher = events.GlobalDispatcherFor[HttpHandlerMetricEvent]()
		dispatcher.AddEventHandler(onHttpHandlerMetricEvent)
		// -- append new event handlers here
	})
}

// onHttpHandlerMetricEvent handles all incoming HttpHandlerMetricEvents
func onHttpHandlerMetricEvent(event HttpHandlerMetricEvent) {
	code := fmt.Sprintf("%d", event.Code)

	requestsCount.WithLabelValues(event.Handler, event.Method, code).Inc()
	responseSize.WithLabelValues(event.Handler, event.Method, code).Observe(float64(event.ResponseSize))
	responseTime.WithLabelValues(event.Handler, event.Method, code).Observe(event.ResponseTime.Seconds())
}
