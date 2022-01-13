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

// GetPrometheusMetrics returns a list of the state of Prometheus metric used by kubecost using the provided client
func GetPrometheusMetrics(client prometheus.Client, offset string) ([]*PrometheusDiagnostic, error) {
	docs := "https://github.com/kubecost/docs/blob/master/diagnostics.md"
	ctx := NewNamedContext(client, DiagnosticContextName)

	result := []*PrometheusDiagnostic{
		{
			ID:          "cadvisorMetric",
			Query:       fmt.Sprintf(`absent_over_time(container_cpu_usage_seconds_total[5m] %s)`, offset),
			Label:       "cAdvsior metrics available",
			Description: "Determine if cAdvisor metrics are available during last 5 minutes.",
			DocLink:     fmt.Sprintf("%s#cadvisor-metrics-available", docs),
		},
		{
			ID:          "ksmMetric",
			Query:       fmt.Sprintf(`absent_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte"}[5m]  %s)`, offset),
			Label:       "Kube-state-metrics available",
			Description: "Determine if metrics from kube-state-metrics are available during last 5 minutes.",
			DocLink:     fmt.Sprintf("%s#kube-state-metrics-metrics-available", docs),
		},
		{
			ID:          "kubecostMetric",
			Query:       fmt.Sprintf(`absent_over_time(node_cpu_hourly_cost[5m]  %s)`, offset),
			Label:       "Kubecost metrics available",
			Description: "Determine if metrics from Kubecost are available during last 5 minutes.",
		},
		{
			ID:          "neMetric",
			Query:       fmt.Sprintf(`absent_over_time(node_cpu_seconds_total[5m]  %s)`, offset),
			Label:       "Node-exporter metrics available",
			Description: "Determine if metrics from node-exporter are available during last 5 minutes.",
			DocLink:     fmt.Sprintf("%s#node-exporter-metrics-available", docs),
		},
		{
			ID:          "cadvisorLabel",
			Query:       fmt.Sprintf(`absent_over_time(container_cpu_usage_seconds_total{container!="",pod!=""}[5m]  %s)`, offset),
			Label:       "Expected cAdvsior labels available",
			Description: "Determine if expected cAdvisor labels are present during last 5 minutes.",
			DocLink:     fmt.Sprintf("%s#cadvisor-metrics-available", docs),
		},
		{
			ID:          "ksmVersion",
			Query:       fmt.Sprintf(`absent_over_time(kube_persistentvolume_capacity_bytes[5m]  %s)`, offset),
			Label:       "Expected kube-state-metrics version found",
			Description: "Determine if metric in required kube-state-metrics version are present during last 5 minutes.",
			DocLink:     fmt.Sprintf("%s#expected-kube-state-metrics-version-found", docs),
		},
		{
			ID:          "scrapeInterval",
			Query:       fmt.Sprintf(`absent_over_time(prometheus_target_interval_length_seconds[5m]  %s)`, offset),
			Label:       "Expected Prometheus self-scrape metrics available",
			Description: "Determine if prometheus has its own self-scraped metrics during the last 5 minutes.",
		},
		{
			ID: "cpuThrottling",
			Query: `avg(increase(container_cpu_cfs_throttled_periods_total{container="cost-model"}[10m])) by (container_name, pod_name, namespace)
		/ avg(increase(container_cpu_cfs_periods_total{container="cost-model"}[10m])) by (container_name, pod_name, namespace) > 0.2`,
			Label:       "Kubecost is not CPU throttled",
			Description: "Kubecost loading slowly? A kubecost component might be CPU throttled",
		},
	}

	for _, pd := range result {
		err := pd.executePrometheusDiagnosticQuery(ctx)
		if err != nil {
			log.Errorf(err.Error())
		}
	}

	return result, nil
}

// PrometheusDiagnostic holds information about a metric and the query to ensure it is functional
type PrometheusDiagnostic struct {
	ID          string         `json:"id"`
	Query       string         `json:"query"`
	Label       string         `json:"label"`
	Description string         `json:"description"`
	DocLink     string         `json:"docLink"`
	Result      []*QueryResult `json:"result"`
	Passed      bool           `json:"passed"`
}

// executePrometheusDiagnosticQuery executes a PrometheusDiagnostic query using the given context
func (pd *PrometheusDiagnostic) executePrometheusDiagnosticQuery(ctx *Context) error {
	resultCh := ctx.Query(pd.Query)
	result, err := resultCh.Await()
	if err != nil {
		return fmt.Errorf("prometheus diagnostic %s failed with error: %s", pd.ID, err)
	}
	if result == nil {
		result = []*QueryResult{}
	}
	pd.Result = result
	pd.Passed = len(result) == 0
	return nil
}
