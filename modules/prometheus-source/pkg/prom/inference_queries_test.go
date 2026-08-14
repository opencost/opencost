package prom

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
)

// fakePromClient implements the prometheus api.Client interface, dispatching
// canned vector responses keyed by substrings of the PromQL query.
type promRule struct {
	substr string
	res    string
}

type fakePromClient struct {
	// responses maps a query substring to the JSON "result" array content.
	responses map[string]string
	// ordered is checked first, in order, for cases where several substrings
	// match one query and the choice must be deterministic.
	ordered []promRule
	queries []string
}

func (f *fakePromClient) URL(ep string, args map[string]string) *url.URL {
	u, _ := url.Parse("http://fake-prometheus:9090" + ep)
	return u
}

func (f *fakePromClient) Do(_ context.Context, req *http.Request) (*http.Response, []byte, error) {
	query := req.URL.Query().Get("query")
	f.queries = append(f.queries, query)

	result := "[]"
	// Ordered rules take precedence over the map, because more than one
	// substring can match a single query: the counter-delta path pins both its
	// end query and its window-max query to the same timestamp, so a map
	// keyed on "@ <unix>" alone would dispatch nondeterministically by Go map
	// iteration order.
	for _, rule := range f.ordered {
		if strings.Contains(query, rule.substr) {
			body := fmt.Sprintf(`{"status":"success","data":{"resultType":"vector","result":%s}}`, rule.res)
			return &http.Response{StatusCode: http.StatusOK}, []byte(body), nil
		}
	}
	for substr, res := range f.responses {
		if strings.Contains(query, substr) {
			result = res
			break
		}
	}

	body := fmt.Sprintf(`{"status":"success","data":{"resultType":"vector","result":%s}}`, result)
	return &http.Response{StatusCode: http.StatusOK}, []byte(body), nil
}

func vectorSample(modelName, namespaceUID, podUID string, value float64) string {
	return fmt.Sprintf(`{"metric":{"model_name":"%s","namespace_uid":"%s","pod_uid":"%s"},"value":[1712000000,"%g"]}`,
		modelName, namespaceUID, podUID, value)
}

func newFakeQuerier(responses map[string]string) (*PrometheusMetricsQuerier, *fakePromClient) {
	client := &fakePromClient{responses: responses}
	config := &OpenCostPrometheusConfig{ClusterLabel: "cluster_id"}
	return &PrometheusMetricsQuerier{
		promConfig:   config,
		promClient:   client,
		promContexts: NewContextFactory(client, config),
	}, client
}

func requireSingleResult(t *testing.T, results []*source.InferenceEngineMetricResult, wantValue float64) {
	t.Helper()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	got := results[0]
	if got.ModelName != "Qwen3-32B" || got.NamespaceUID != "ns-uid" || got.PodUID != "pod-uid-0" {
		t.Errorf("unexpected result identity: %+v", got)
	}
	if got.Value != wantValue {
		t.Errorf("unexpected value: got %v, want %v", got.Value, wantValue)
	}
}

func TestQueryInferenceGauges(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	tests := []struct {
		name          string
		query         func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceEngineMetricResult]
		wantSubstring string
	}{
		{
			name: "kv cache usage avg",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceEngineMetricResult] {
				return q.QueryInferenceKVCacheUsageAvg(start, end)
			},
			wantSubstring: `avg by (model_name, pod_uid, namespace_uid, engine, cluster_id) (((avg_over_time(vllm:kv_cache_usage_perc[`,
		},
		{
			name: "kv cache usage max",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceEngineMetricResult] {
				return q.QueryInferenceKVCacheUsageMax(start, end)
			},
			wantSubstring: `max by (model_name, pod_uid, namespace_uid, engine, cluster_id) (((max_over_time(vllm:kv_cache_usage_perc[`,
		},
		{
			name: "queue depth avg",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceEngineMetricResult] {
				return q.QueryInferenceQueueDepthAvg(start, end)
			},
			wantSubstring: `avg by (model_name, pod_uid, namespace_uid, engine, cluster_id) (((avg_over_time(vllm:num_requests_waiting[`,
		},
		{
			name: "queue depth max",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceEngineMetricResult] {
				return q.QueryInferenceQueueDepthMax(start, end)
			},
			wantSubstring: `max by (model_name, pod_uid, namespace_uid, engine, cluster_id) (((max_over_time(vllm:num_requests_waiting[`,
		},
		{
			name: "running requests avg",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceEngineMetricResult] {
				return q.QueryInferenceRunningRequestsAvg(start, end)
			},
			wantSubstring: `avg by (model_name, pod_uid, namespace_uid, engine, cluster_id) (((avg_over_time(vllm:num_requests_running[`,
		},
		{
			name: "running requests max",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceEngineMetricResult] {
				return q.QueryInferenceRunningRequestsMax(start, end)
			},
			wantSubstring: `max by (model_name, pod_uid, namespace_uid, engine, cluster_id) (((max_over_time(vllm:num_requests_running[`,
		},
		{
			name: "kv cache usage p95",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceEngineMetricResult] {
				return q.QueryInferenceKVCacheUsageP95(start, end)
			},
			wantSubstring: `max by (model_name, pod_uid, namespace_uid, engine, cluster_id) (((quantile_over_time(0.95, vllm:kv_cache_usage_perc[`,
		},
		{
			name: "queue depth p95",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceEngineMetricResult] {
				return q.QueryInferenceQueueDepthP95(start, end)
			},
			wantSubstring: `max by (model_name, pod_uid, namespace_uid, engine, cluster_id) (((quantile_over_time(0.95, vllm:num_requests_waiting[`,
		},
		{
			name: "running requests p95",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceEngineMetricResult] {
				return q.QueryInferenceRunningRequestsP95(start, end)
			},
			wantSubstring: `max by (model_name, pod_uid, namespace_uid, engine, cluster_id) (((quantile_over_time(0.95, vllm:num_requests_running[`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, client := newFakeQuerier(map[string]string{
				"vllm:": "[" + vectorSample("Qwen3-32B", "ns-uid", "pod-uid-0", 0.42) + "]",
			})

			results, err := tt.query(q).Await()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(client.queries) != 1 {
				t.Fatalf("expected 1 query, got %d", len(client.queries))
			}
			if !strings.Contains(client.queries[0], tt.wantSubstring) {
				t.Errorf("query %q does not contain %q", client.queries[0], tt.wantSubstring)
			}

			requireSingleResult(t, results, 0.42)
		})
	}
}

// TestInferenceQueriesJoinIsNonFatalAndClusterScoped pins the two properties
// of the namespace_uid join that are invisible in a single-cluster test
// deployment and expensive in a real one.
//
// A bare `*` join is an inner join, so any deployment that does not scrape
// OpenCost's own /metrics, or that runs with namespace_info disabled, would
// lose every model-server series rather than lose one label. And matching on
// the namespace name alone makes two clusters that each have a namespace
// called "prod" a many-to-one duplicate match, which fails the entire query in
// a federated Prometheus instead of degrading.
func TestInferenceQueriesJoinIsNonFatalAndClusterScoped(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	q, client := newFakeQuerier(map[string]string{
		"vllm:": "[" + vectorSample("Qwen3-32B", "ns-uid", "pod-uid-0", 0.42) + "]",
	})

	if _, err := q.QueryInferenceKVCacheUsageAvg(start, end).Await(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.queries) != 1 {
		t.Fatalf("expected 1 query, got %d", len(client.queries))
	}
	query := client.queries[0]

	// The join must carry a fallback branch for series whose namespace has no
	// namespace_info match. `unless` rather than a bare `or`: the joined series
	// carries an extra label, so `or` would emit both copies and split one pod
	// across two rows.
	if !strings.Contains(query, "unless on (namespace, cluster_id)") {
		t.Errorf("join has no non-fatal fallback branch: %q", query)
	}

	// Both sides of the match must include the cluster label.
	if !strings.Contains(query, "on (namespace, cluster_id) group_left(namespace_uid)") {
		t.Errorf("join is not cluster-scoped: %q", query)
	}
	if !strings.Contains(query, "max by (namespace, namespace_uid, cluster_id)") {
		t.Errorf("namespace_info side is not cluster-scoped: %q", query)
	}
}

// TestInferenceQueriesApplyClusterFilter pins that the configured cluster
// filter reaches the metric selector, the way every DCGM query scopes its own.
func TestInferenceQueriesApplyClusterFilter(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	client := &fakePromClient{responses: map[string]string{
		"vllm:": "[" + vectorSample("Qwen3-32B", "ns-uid", "pod-uid-0", 0.42) + "]",
	}}
	config := &OpenCostPrometheusConfig{
		ClusterLabel:  "cluster_id",
		ClusterFilter: `cluster_id="cluster-one"`,
	}
	q := &PrometheusMetricsQuerier{
		promConfig:   config,
		promClient:   client,
		promContexts: NewContextFactory(client, config),
	}

	if _, err := q.QueryInferenceKVCacheUsageAvg(start, end).Await(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(client.queries[0], `vllm:kv_cache_usage_perc{cluster_id="cluster-one"}[`) {
		t.Errorf("cluster filter not applied to the metric selector: %q", client.queries[0])
	}
}

func TestQueryInferencePreemptions(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	t.Run("delta is end minus start", func(t *testing.T) {
		q, client := newFakeQuerier(map[string]string{
			fmt.Sprintf("@ %d", end.Unix()):   "[" + vectorSample("Qwen3-32B", "ns-uid", "pod-uid-0", 40) + "]",
			fmt.Sprintf("@ %d", start.Unix()): "[" + vectorSample("Qwen3-32B", "ns-uid", "pod-uid-0", 15) + "]",
		})

		results, err := q.QueryInferencePreemptions(start, end).Await()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// End, start, and the window maximum used to correct a reset.
		if len(client.queries) != 3 {
			t.Fatalf("expected 3 queries, got %d", len(client.queries))
		}
		if !strings.Contains(client.queries[0], "sum by (model_name, pod_uid, namespace_uid, engine, cluster_id) (((last_over_time(vllm:num_preemptions_total[") {
			t.Errorf("unexpected end-of-window query: %q", client.queries[0])
		}
		var sawMax bool
		for _, q := range client.queries {
			if strings.Contains(q, "max_over_time(vllm:num_preemptions_total[") {
				sawMax = true
			}
		}
		if !sawMax {
			t.Errorf("no window-maximum query issued; reset correction cannot recover pre-reset progress: %q", client.queries)
		}
		requireSingleResult(t, results, 25)
	})

	// A counter that restarts mid-window has made progress the two endpoints
	// cannot see. The window maximum recovers it: the counter climbed from the
	// start value to its peak, then restarted and climbed to the end value.
	//
	// Worked example, matching Prometheus' own increase() on the same samples:
	// 100, 110, restart, 5, 15, 25. The pre-reset progress is 110 - 100 = 10,
	// the post-reset progress is 25, and the reported increase is 35. Falling
	// back to the end value alone reports 25 and silently drops everything
	// before the restart.
	t.Run("counter reset recovers pre-reset progress from the window maximum", func(t *testing.T) {
		client := &fakePromClient{ordered: []promRule{
			{substr: "max_over_time(", res: "[" + vectorSample("Qwen3-32B", "ns-uid", "pod-uid-0", 110) + "]"},
			{substr: fmt.Sprintf("@ %d", start.Unix()), res: "[" + vectorSample("Qwen3-32B", "ns-uid", "pod-uid-0", 100) + "]"},
			{substr: "last_over_time(", res: "[" + vectorSample("Qwen3-32B", "ns-uid", "pod-uid-0", 25) + "]"},
		}}
		config := &OpenCostPrometheusConfig{ClusterLabel: "cluster_id"}
		q := &PrometheusMetricsQuerier{
			promConfig:   config,
			promClient:   client,
			promContexts: NewContextFactory(client, config),
		}

		results, err := q.QueryInferencePreemptions(start, end).Await()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// (110 - 100) + 25
		requireSingleResult(t, results, 35)
	})

	// If the window maximum is not available, the end value remains the lower
	// bound rather than the delta going negative.
	t.Run("counter reset without a window maximum falls back to the end value", func(t *testing.T) {
		client := &fakePromClient{ordered: []promRule{
			{substr: "max_over_time(", res: "[]"},
			{substr: fmt.Sprintf("@ %d", start.Unix()), res: "[" + vectorSample("Qwen3-32B", "ns-uid", "pod-uid-0", 100) + "]"},
			{substr: "last_over_time(", res: "[" + vectorSample("Qwen3-32B", "ns-uid", "pod-uid-0", 4) + "]"},
		}}
		config := &OpenCostPrometheusConfig{ClusterLabel: "cluster_id"}
		q := &PrometheusMetricsQuerier{
			promConfig:   config,
			promClient:   client,
			promContexts: NewContextFactory(client, config),
		}

		results, err := q.QueryInferencePreemptions(start, end).Await()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		requireSingleResult(t, results, 4)
	})

	t.Run("replica absent at window start counts from zero", func(t *testing.T) {
		q, _ := newFakeQuerier(map[string]string{
			fmt.Sprintf("@ %d", end.Unix()): "[" + vectorSample("Qwen3-32B", "ns-uid", "pod-uid-0", 9) + "]",
		})

		results, err := q.QueryInferencePreemptions(start, end).Await()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		requireSingleResult(t, results, 9)
	})
}

func TestQueryInferenceGaugeEmptyResults(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	q, _ := newFakeQuerier(map[string]string{})
	results, err := q.QueryInferenceKVCacheUsageAvg(start, end).Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}
}
