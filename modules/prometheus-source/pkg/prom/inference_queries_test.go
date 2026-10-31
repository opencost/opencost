package prom

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingPromClient captures the PromQL of every query it is asked to run and
// returns an empty-but-valid Prometheus response, so query construction can be
// asserted without a Prometheus server.
type recordingPromClient struct {
	mu      sync.Mutex
	queries []string
}

func (c *recordingPromClient) URL(ep string, args map[string]string) *url.URL {
	return &url.URL{Scheme: "http", Host: "localhost:9090", Path: ep}
}

func (c *recordingPromClient) Do(_ context.Context, req *http.Request) (*http.Response, []byte, error) {
	if req != nil && req.URL != nil {
		if q := req.URL.Query().Get("query"); q != "" {
			c.mu.Lock()
			c.queries = append(c.queries, q)
			c.mu.Unlock()
		}
	}

	body := []byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`)
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}, body, nil
}

func (c *recordingPromClient) recorded() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.queries...)
}

func newRecordingQuerier() (*PrometheusMetricsQuerier, *recordingPromClient) {
	client := &recordingPromClient{}
	cfg := &OpenCostPrometheusConfig{ClusterLabel: "cluster_id"}
	return newPrometheusMetricsQuerier(cfg, client, NewContextFactory(client, cfg)), client
}

// TestQueryInferenceProcessingTime_MetricSelection pins the vLLM metric each
// processing-time query reads.
//
// Regression test for #4146: the output query used
// vllm:request_time_per_output_token_seconds_sum, whose _sum is a sum of
// per-request mean seconds-per-token rather than a DECODE duration, which
// inverted the compute-time cost split. The calculator tests take
// InputProcessingTime/OutputProcessingTime as given, so only an assertion at the
// query layer catches a reverted metric name.
func TestQueryInferenceProcessingTime_MetricSelection(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	tests := []struct {
		name     string
		run      func(*PrometheusMetricsQuerier) error
		want     string
		rejected string
	}{
		{
			name: "input uses prefill time",
			run: func(q *PrometheusMetricsQuerier) error {
				_, err := q.QueryInferenceInputProcessingTime(start, end).Await()
				return err
			},
			want: "vllm:request_prefill_time_seconds_sum",
		},
		{
			name: "output uses decode time, not time-per-output-token",
			run: func(q *PrometheusMetricsQuerier) error {
				_, err := q.QueryInferenceOutputProcessingTime(start, end).Await()
				return err
			},
			want:     "vllm:request_decode_time_seconds_sum",
			rejected: "vllm:request_time_per_output_token_seconds_sum",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			querier, client := newRecordingQuerier()

			if err := tc.run(querier); err != nil {
				t.Fatalf("query returned error: %v", err)
			}

			queries := client.recorded()

			// queryCounterDelta issues one query pinned to the start of the window and
			// one pinned to the end; both must read the same metric.
			if len(queries) != 2 {
				t.Fatalf("expected 2 boundary queries, got %d: %v", len(queries), queries)
			}

			for i, q := range queries {
				if !strings.Contains(q, tc.want) {
					t.Errorf("boundary query %d does not read %s: %s", i, tc.want, q)
				}
				if tc.rejected != "" && strings.Contains(q, tc.rejected) {
					t.Errorf("boundary query %d reads %s, which is seconds-per-token rather than a duration: %s",
						i, tc.rejected, q)
				}
			}
		})
	}
}

// valuePromClient answers each query with one series for model "m" in namespace
// "ns". End-of-window queries return the value configured for the metric they
// read; start-of-window queries (2m lookback) return 0. Metrics with no
// configured value return an empty vector, like an engine that is not deployed.
type valuePromClient struct {
	recordingPromClient
	endValues map[string]string
}

func (c *valuePromClient) Do(ctx context.Context, req *http.Request) (*http.Response, []byte, error) {
	// Record the query; the recording client's empty response is discarded.
	_, _, _ = c.recordingPromClient.Do(ctx, req)

	result := "[]"
	q := req.URL.Query().Get("query")
	for metric, value := range c.endValues {
		if !strings.Contains(q, metric+"[") {
			continue
		}
		if strings.Contains(q, "[2m] @") {
			value = "0"
		}
		result = `[{"metric":{"model_name":"m","namespace":"ns"},"value":[1700000000,"` + value + `"]}]`
	}

	body := []byte(`{"status":"success","data":{"resultType":"vector","result":` + result + `}}`)
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}, body, nil
}

func newValueQuerier(endValues map[string]string) *PrometheusMetricsQuerier {
	client := &valuePromClient{endValues: endValues}
	cfg := &OpenCostPrometheusConfig{ClusterLabel: "cluster_id"}
	return newPrometheusMetricsQuerier(cfg, client, NewContextFactory(client, cfg))
}

// TestQueryInferenceTokens_ReadsVLLMAndSGLang pins the token counters each token
// query reads: one per supported model server. Dropping one would silently zero
// the cost data for every model served by that engine (#4148).
func TestQueryInferenceTokens_ReadsVLLMAndSGLang(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	tests := []struct {
		name string
		run  func(*PrometheusMetricsQuerier) error
		want []string
	}{
		{
			name: "prompt tokens",
			run: func(q *PrometheusMetricsQuerier) error {
				_, err := q.QueryInferencePromptTokens(start, end).Await()
				return err
			},
			want: []string{"vllm:prompt_tokens_total", "sglang:prompt_tokens_total"},
		},
		{
			name: "generation tokens",
			run: func(q *PrometheusMetricsQuerier) error {
				_, err := q.QueryInferenceGenerationTokens(start, end).Await()
				return err
			},
			want: []string{"vllm:generation_tokens_total", "sglang:generation_tokens_total"},
		},
		{
			name: "cached tokens",
			run: func(q *PrometheusMetricsQuerier) error {
				_, err := q.QueryInferenceCachedTokens(start, end).Await()
				return err
			},
			want: []string{"vllm:prefix_cache_hits_total", "sglang:cached_tokens_total"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			querier, client := newRecordingQuerier()

			if err := tc.run(querier); err != nil {
				t.Fatalf("query returned error: %v", err)
			}

			// Two boundary queries (start and end of window) per metric.
			queries := client.recorded()
			if len(queries) != 2*len(tc.want) {
				t.Fatalf("expected %d boundary queries, got %d: %v", 2*len(tc.want), len(queries), queries)
			}

			for _, metric := range tc.want {
				n := 0
				for _, q := range queries {
					if !strings.Contains(q, metric+"[") {
						continue
					}
					n++
					if !strings.Contains(q, "sum by (model_name, namespace)") {
						t.Errorf("query for %s is not grouped by (model_name, namespace): %s", metric, q)
					}
				}
				if n != 2 {
					t.Errorf("expected 2 boundary queries reading %s, got %d", metric, n)
				}
			}
		})
	}
}

// TestQueryInferenceTokens_SumsAcrossEngines asserts the per-engine deltas are
// added per (model_name, namespace), and that an engine with no series (not
// deployed) contributes nothing.
func TestQueryInferenceTokens_SumsAcrossEngines(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()
	key := modelNamespaceKey("m", "ns")

	tests := []struct {
		name      string
		endValues map[string]string
		want      float64
	}{
		{
			name:      "vLLM only",
			endValues: map[string]string{"vllm:prompt_tokens_total": "1000"},
			want:      1000,
		},
		{
			name:      "SGLang only",
			endValues: map[string]string{"sglang:prompt_tokens_total": "250"},
			want:      250,
		},
		{
			name: "both engines serve the model",
			endValues: map[string]string{
				"vllm:prompt_tokens_total":   "1000",
				"sglang:prompt_tokens_total": "250",
			},
			want: 1250,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := newValueQuerier(tc.endValues).QueryInferencePromptTokens(start, end).Await()
			if err != nil {
				t.Fatalf("query returned error: %v", err)
			}

			got := 0.0
			for _, r := range res {
				got += r.Values[key]
			}
			if got != tc.want {
				t.Errorf("prompt tokens for %s = %v, want %v", key, got, tc.want)
			}
		})
	}
}

// TestQueryInferenceProcessingTime_GroupsByModelAndNamespace asserts both
// processing-time queries aggregate by the labels the collector keys on. Dropping
// either label would silently collapse or discard per-model series.
func TestQueryInferenceProcessingTime_GroupsByModelAndNamespace(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	querier, client := newRecordingQuerier()

	if _, err := querier.QueryInferenceInputProcessingTime(start, end).Await(); err != nil {
		t.Fatalf("input query returned error: %v", err)
	}
	if _, err := querier.QueryInferenceOutputProcessingTime(start, end).Await(); err != nil {
		t.Fatalf("output query returned error: %v", err)
	}

	queries := client.recorded()
	if len(queries) != 4 {
		t.Fatalf("expected 4 boundary queries across both metrics, got %d", len(queries))
	}

	for i, q := range queries {
		if !strings.Contains(q, "sum by (model_name, namespace)") {
			t.Errorf("query %d is not grouped by (model_name, namespace): %s", i, q)
		}
	}
}
