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
