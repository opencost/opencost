package prom

import (
	"fmt"
	"time"

	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/log"
	prometheus "github.com/prometheus/client_golang/api"
)

// QueuedPromRequest is a representation of a request waiting to be sent by the prometheus
// client.
type QueuedPromRequest struct {
	Context   string `json:"context"`
	Query     string `json:"query"`
	QueueTime int64  `json:"queueTime"`
}

// PrometheusQueueState contains diagnostic information concerning the state of the prometheus request
// queue
type PrometheusQueueState struct {
	QueuedRequests      []*QueuedPromRequest `json:"queuedRequests"`
	OutboundRequests    int                  `json:"outboundRequests"`
	TotalRequests       int                  `json:"totalRequests"`
	MaxQueryConcurrency int                  `json:"maxQueryConcurrency"`
}

// GetPrometheusQueueState is a diagnostic function that probes the prometheus request queue and gathers
// query, context, and queue statistics.
func GetPrometheusQueueState(client prometheus.Client) (*PrometheusQueueState, error) {
	rlpc, ok := client.(*RateLimitedPrometheusClient)
	if !ok {
		return nil, fmt.Errorf("Failed to get prometheus queue state for the provided client. Must be of type RateLimitedPrometheusClient.")
	}

	outbound := rlpc.TotalOutboundRequests()

	requests := []*QueuedPromRequest{}
	rlpc.queue.Each(func(_ int, entry interface{}) {
		if req, ok := entry.(*workRequest); ok {
			requests = append(requests, &QueuedPromRequest{
				Context:   req.contextName,
				Query:     req.query,
				QueueTime: time.Since(req.start).Milliseconds(),
			})
		}
	})

	return &PrometheusQueueState{
		QueuedRequests:      requests,
		OutboundRequests:    outbound,
		TotalRequests:       outbound + len(requests),
		MaxQueryConcurrency: env.GetMaxQueryConcurrency(),
	}, nil
}

// LogPrometheusClientState logs the current state, with respect to outbound requests, if that
// information is available.
func LogPrometheusClientState(client prometheus.Client) {
	if rc, ok := client.(requestCounter); ok {
		queued := rc.TotalQueuedRequests()
		outbound := rc.TotalOutboundRequests()
		total := queued + outbound

		log.Infof("Outbound Requests: %d, Queued Requests: %d, Total Requests: %d", outbound, queued, total)
	}
}
