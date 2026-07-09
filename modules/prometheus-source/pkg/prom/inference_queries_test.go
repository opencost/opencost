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
type fakePromClient struct {
	// responses maps a query substring to the JSON "result" array content.
	responses map[string]string
	queries   []string
}

func (f *fakePromClient) URL(ep string, args map[string]string) *url.URL {
	u, _ := url.Parse("http://fake-prometheus:9090" + ep)
	return u
}

func (f *fakePromClient) Do(_ context.Context, req *http.Request) (*http.Response, []byte, error) {
	query := req.URL.Query().Get("query")
	f.queries = append(f.queries, query)

	result := "[]"
	for substr, res := range f.responses {
		if strings.Contains(query, substr) {
			result = res
			break
		}
	}

	body := fmt.Sprintf(`{"status":"success","data":{"resultType":"vector","result":%s}}`, result)
	return &http.Response{StatusCode: http.StatusOK}, []byte(body), nil
}

func vectorSample(modelName, namespace, pod string, value float64) string {
	return fmt.Sprintf(`{"metric":{"model_name":"%s","namespace":"%s","pod":"%s"},"value":[1712000000,"%g"]}`,
		modelName, namespace, pod, value)
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

func requireSingleResult(t *testing.T, results []*source.InferenceServerMetricResult, wantValue float64) {
	t.Helper()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	got := results[0]
	if got.ModelName != "Qwen3-32B" || got.Namespace != "llm-d" || got.Pod != "vllm-0" {
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
		query         func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceServerMetricResult]
		wantSubstring string
	}{
		{
			name: "kv cache usage avg",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceServerMetricResult] {
				return q.QueryInferenceKVCacheUsageAvg(start, end)
			},
			wantSubstring: `avg by (model_name, namespace, pod) (avg_over_time(vllm:kv_cache_usage_perc[`,
		},
		{
			name: "kv cache usage max",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceServerMetricResult] {
				return q.QueryInferenceKVCacheUsageMax(start, end)
			},
			wantSubstring: `max by (model_name, namespace, pod) (max_over_time(vllm:kv_cache_usage_perc[`,
		},
		{
			name: "queue depth avg",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceServerMetricResult] {
				return q.QueryInferenceQueueDepthAvg(start, end)
			},
			wantSubstring: `avg by (model_name, namespace, pod) (avg_over_time(vllm:num_requests_waiting[`,
		},
		{
			name: "queue depth max",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceServerMetricResult] {
				return q.QueryInferenceQueueDepthMax(start, end)
			},
			wantSubstring: `max by (model_name, namespace, pod) (max_over_time(vllm:num_requests_waiting[`,
		},
		{
			name: "running requests avg",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceServerMetricResult] {
				return q.QueryInferenceRunningRequestsAvg(start, end)
			},
			wantSubstring: `avg by (model_name, namespace, pod) (avg_over_time(vllm:num_requests_running[`,
		},
		{
			name: "kv cache usage p95",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceServerMetricResult] {
				return q.QueryInferenceKVCacheUsageP95(start, end)
			},
			wantSubstring: `max by (model_name, namespace, pod) (quantile_over_time(0.95, vllm:kv_cache_usage_perc[`,
		},
		{
			name: "queue depth p95",
			query: func(q *PrometheusMetricsQuerier) *source.Future[source.InferenceServerMetricResult] {
				return q.QueryInferenceQueueDepthP95(start, end)
			},
			wantSubstring: `max by (model_name, namespace, pod) (quantile_over_time(0.95, vllm:num_requests_waiting[`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, client := newFakeQuerier(map[string]string{
				"vllm:": "[" + vectorSample("Qwen3-32B", "llm-d", "vllm-0", 0.42) + "]",
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

func TestQueryInferencePreemptions(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	t.Run("delta is end minus start", func(t *testing.T) {
		q, client := newFakeQuerier(map[string]string{
			fmt.Sprintf("@ %d", end.Unix()):   "[" + vectorSample("Qwen3-32B", "llm-d", "vllm-0", 40) + "]",
			fmt.Sprintf("@ %d", start.Unix()): "[" + vectorSample("Qwen3-32B", "llm-d", "vllm-0", 15) + "]",
		})

		results, err := q.QueryInferencePreemptions(start, end).Await()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(client.queries) != 2 {
			t.Fatalf("expected 2 queries, got %d", len(client.queries))
		}
		if !strings.Contains(client.queries[0], "sum by (model_name, namespace, pod) (last_over_time(vllm:num_preemptions_total[") {
			t.Errorf("unexpected end-of-window query: %q", client.queries[0])
		}
		requireSingleResult(t, results, 25)
	})

	t.Run("counter reset falls back to the end value", func(t *testing.T) {
		q, _ := newFakeQuerier(map[string]string{
			fmt.Sprintf("@ %d", end.Unix()):   "[" + vectorSample("Qwen3-32B", "llm-d", "vllm-0", 4) + "]",
			fmt.Sprintf("@ %d", start.Unix()): "[" + vectorSample("Qwen3-32B", "llm-d", "vllm-0", 100) + "]",
		})

		results, err := q.QueryInferencePreemptions(start, end).Await()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		requireSingleResult(t, results, 4)
	})

	t.Run("replica absent at window start counts from zero", func(t *testing.T) {
		q, _ := newFakeQuerier(map[string]string{
			fmt.Sprintf("@ %d", end.Unix()): "[" + vectorSample("Qwen3-32B", "llm-d", "vllm-0", 9) + "]",
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
