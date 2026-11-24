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

func TestQueryInferenceDimensionTokens_Disabled(t *testing.T) {
	t.Setenv("INFERENCE_ATTRIBUTION_ENABLED", "false")
	querier, client := newRecordingQuerier()

	start := time.Now().Add(-time.Hour)
	end := time.Now()

	res, err := querier.QueryInferenceDimensionTokens(start, end).Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) == 0 || len(res[0].Values) != 0 {
		t.Fatalf("expected empty values when disabled, got %+v", res)
	}
	if len(client.recorded()) != 0 {
		t.Errorf("expected no queries when attribution is disabled, got %v", client.recorded())
	}
}

func TestQueryInferenceDimensionTokens_PromQLGeneration(t *testing.T) {
	t.Setenv("INFERENCE_ATTRIBUTION_ENABLED", "true")
	t.Setenv("INFERENCE_ATTRIBUTION_CONFIG_PATH", "")
	t.Setenv("INFERENCE_ATTRIBUTION_INPUT_TOKENS_SERIES", "custom_input_tokens_total")
	t.Setenv("INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_SERIES", "custom_output_tokens_total")
	t.Setenv("INFERENCE_ATTRIBUTION_INPUT_TOKENS_ESTIMATED_SERIES", "custom_est_input_tokens_total")
	t.Setenv("INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_ESTIMATED_SERIES", "custom_est_output_tokens_total")
	t.Setenv("INFERENCE_ATTRIBUTION_TENANT_LABEL", "team")
	t.Setenv("INFERENCE_ATTRIBUTION_WORKLOAD_LABEL", "app")
	t.Setenv("INFERENCE_ATTRIBUTION_USER_LABEL", "caller")
	t.Setenv("INFERENCE_ATTRIBUTION_MODEL_LABEL", "model_id")
	t.Setenv("INFERENCE_ATTRIBUTION_NAMESPACE_LABEL", "k8s_namespace")

	querier, client := newRecordingQuerier()

	start := time.Now().Add(-60 * time.Second)
	end := time.Now()

	_, err := querier.QueryInferenceDimensionTokens(start, end).Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Each of the 4 series (input, output, est input, est output) needs a
	// raw-sample window query and a start-of-window query.
	queries := client.recorded()
	if len(queries) != 8 {
		t.Fatalf("expected 8 queries (end + start for each of 4 series), got %d: %v", len(queries), queries)
	}

	for _, metric := range []string{"custom_input_tokens_total", "custom_output_tokens_total", "custom_est_input_tokens_total", "custom_est_output_tokens_total"} {
		var ends, starts int
		for _, q := range queries {
			if !strings.Contains(q, metric+"[") {
				continue
			}
			switch {
			case strings.Contains(q, "[2m] @"):
				starts++
			default:
				ends++
			}
		}
		if ends != 1 || starts != 1 {
			t.Errorf("metric %s: expected 1 window and 1 start query, got %d window and %d start: %v", metric, ends, starts, queries)
		}
	}

	for _, q := range queries {
		if strings.Contains(q, "increase(") || strings.Contains(q, "rate(") {
			t.Errorf("query must not extrapolate with increase/rate: %s", q)
		}
		isStart := strings.HasPrefix(q, "last_over_time(") && strings.Contains(q, "[2m] @")
		isWindow := !strings.Contains(q, "(") && strings.Contains(q, "s] @")
		if !isStart && !isWindow {
			t.Errorf("query is neither an exact start sample nor a raw window selector: %s", q)
		}
		// The delta is taken per series, so the query must not aggregate.
		if strings.Contains(q, "sum by") {
			t.Errorf("query must not aggregate before the delta is taken: %s", q)
		}
	}
}

// seriesPromClient answers queries from fixed samples, keyed by whether the
// query is the raw window selector or the start-of-window last_over_time one.
type seriesPromClient struct {
	end   string // JSON "result" array for window queries (instant "value" or matrix "values")
	start string // JSON "result" array for start-of-window queries
}

func (c *seriesPromClient) URL(ep string, args map[string]string) *url.URL {
	return &url.URL{Scheme: "http", Host: "localhost:9090", Path: ep}
}

func (c *seriesPromClient) Do(_ context.Context, req *http.Request) (*http.Response, []byte, error) {
	q := req.URL.Query().Get("query")
	result := c.end
	if strings.Contains(q, "[2m] @") {
		result = c.start
	}
	body := []byte(`{"status":"success","data":{"resultType":"vector","result":` + result + `}}`)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))}, body, nil
}

func queryDimensionTokensWith(t *testing.T, end, start string) map[string]float64 {
	t.Helper()
	t.Setenv("INFERENCE_ATTRIBUTION_ENABLED", "true")
	t.Setenv("INFERENCE_ATTRIBUTION_CONFIG_PATH", "")
	t.Setenv("INFERENCE_ATTRIBUTION_INPUT_TOKENS_SERIES", "litellm_input_tokens_metric_total")
	t.Setenv("INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_SERIES", "")
	t.Setenv("INFERENCE_ATTRIBUTION_INPUT_TOKENS_ESTIMATED_SERIES", "")
	t.Setenv("INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_ESTIMATED_SERIES", "")
	t.Setenv("INFERENCE_ATTRIBUTION_TENANT_LABEL", "team_alias")
	t.Setenv("INFERENCE_ATTRIBUTION_WORKLOAD_LABEL", "api_key_alias")
	t.Setenv("INFERENCE_ATTRIBUTION_USER_LABEL", "end_user")
	t.Setenv("INFERENCE_ATTRIBUTION_MODEL_LABEL", "model")
	t.Setenv("INFERENCE_ATTRIBUTION_NAMESPACE_LABEL", "deployment_namespace")

	client := &seriesPromClient{end: end, start: start}
	cfg := &OpenCostPrometheusConfig{ClusterLabel: "cluster_id"}
	querier := newPrometheusMetricsQuerier(cfg, client, NewContextFactory(client, cfg))

	res, err := querier.QueryInferenceDimensionTokens(time.Now().Add(-60*time.Second), time.Now()).Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := map[string]float64{}
	for _, r := range res {
		for k, v := range r.Values {
			out[k.UserID+"/"+k.TenantID+"/"+k.WorkloadID+"/"+k.ServingModel] += v.PromptTokens
		}
	}
	return out
}

// Regression test: with a 60s window and a 30s scrape, increase() reported 544
// for a 272-token step (2x). The exact delta must report 272.
func TestQueryInferenceDimensionTokens_ExactDeltaNotExtrapolated(t *testing.T) {
	series := func(v string) string {
		return `[{"metric":{"end_user":"alice","team_alias":"acme","api_key_alias":"rag","model":"gemma","instance":"a:4000"},"value":[1,"` + v + `"]}]`
	}
	got := queryDimensionTokensWith(t, series("286"), series("14"))

	if want := 272.0; got["alice/acme/rag/gemma"] != want {
		t.Errorf("expected exact delta %v, got %v (%v)", want, got["alice/acme/rag/gemma"], got)
	}
}

// A series with no sample at the start of the window is new: its start is 0.
func TestQueryInferenceDimensionTokens_NewSeriesCountsFromZero(t *testing.T) {
	end := `[{"metric":{"end_user":"bob","team_alias":"acme","api_key_alias":"rag","model":"gemma","instance":"a:4000"},"value":[1,"50"]}]`
	got := queryDimensionTokensWith(t, end, `[]`)

	if want := 50.0; got["bob/acme/rag/gemma"] != want {
		t.Errorf("expected %v for a new series, got %v", want, got)
	}
}

// A negative delta is a counter reset; the post-reset value is the activity.
func TestQueryInferenceDimensionTokens_CounterReset(t *testing.T) {
	end := `[{"metric":{"end_user":"alice","team_alias":"acme","api_key_alias":"rag","model":"gemma","instance":"a:4000"},"value":[1,"30"]}]`
	start := `[{"metric":{"end_user":"alice","team_alias":"acme","api_key_alias":"rag","model":"gemma","instance":"a:4000"},"value":[1,"1000"]}]`
	got := queryDimensionTokensWith(t, end, start)

	if want := 30.0; got["alice/acme/rag/gemma"] != want {
		t.Errorf("expected reset to use the end value %v, got %v", want, got)
	}
}

// The delta is taken per series. One replica resetting must not corrupt the
// other, and the replicas' deltas are summed for the same dimension key.
func TestQueryInferenceDimensionTokens_PerReplicaDeltaThenSum(t *testing.T) {
	mk := func(inst, v string) string {
		return `{"metric":{"end_user":"alice","team_alias":"acme","api_key_alias":"rag","model":"gemma","instance":"` + inst + `"},"value":[1,"` + v + `"]}`
	}
	end := `[` + mk("a:4000", "130") + `,` + mk("b:4000", "20") + `]`    // a: +30, b: reset
	start := `[` + mk("a:4000", "100") + `,` + mk("b:4000", "900") + `]` // b dropped 900 -> 20
	got := queryDimensionTokensWith(t, end, start)

	// a contributes 30, b contributes its post-reset 20. A sum taken before the
	// delta would give (130+20)-(100+900) < 0 and fall back to 150.
	if want := 50.0; got["alice/acme/rag/gemma"] != want {
		t.Errorf("expected per-replica deltas to sum to %v, got %v", want, got)
	}
}

func TestQueryInferenceDimensionTokens_UserAndWorkloadDisabled(t *testing.T) {
	t.Setenv("INFERENCE_ATTRIBUTION_ENABLED", "true")
	t.Setenv("INFERENCE_ATTRIBUTION_CONFIG_PATH", "")
	t.Setenv("INFERENCE_ATTRIBUTION_INPUT_TOKENS_SERIES", "litellm_input_tokens")
	t.Setenv("INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_SERIES", "litellm_output_tokens")
	t.Setenv("INFERENCE_ATTRIBUTION_INPUT_TOKENS_ESTIMATED_SERIES", "")
	t.Setenv("INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_ESTIMATED_SERIES", "")
	t.Setenv("INFERENCE_ATTRIBUTION_TENANT_LABEL", "team_alias")
	t.Setenv("INFERENCE_ATTRIBUTION_WORKLOAD_LABEL", "")
	t.Setenv("INFERENCE_ATTRIBUTION_USER_LABEL", "")
	t.Setenv("INFERENCE_ATTRIBUTION_MODEL_LABEL", "model")
	t.Setenv("INFERENCE_ATTRIBUTION_NAMESPACE_LABEL", "deployment_namespace")

	querier, client := newRecordingQuerier()

	start := time.Now().Add(-120 * time.Second)
	end := time.Now()

	_, err := querier.QueryInferenceDimensionTokens(start, end).Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 2 series (input, output), each with an end and a start query.
	queries := client.recorded()
	if len(queries) != 4 {
		t.Fatalf("expected 4 queries, got %d: %v", len(queries), queries)
	}

	// Labels are no longer in the query (the delta is per series), so the
	// disabled user/workload labels are checked on the result instead.
	for _, q := range queries {
		if strings.Contains(q, "sum by") {
			t.Errorf("query must not aggregate before the delta is taken: %s", q)
		}
	}
}

// With user and workload labels disabled, the key must carry neither, even when
// the series has such labels.
func TestQueryInferenceDimensionTokens_UserAndWorkloadDisabledKey(t *testing.T) {
	t.Setenv("INFERENCE_ATTRIBUTION_ENABLED", "true")
	t.Setenv("INFERENCE_ATTRIBUTION_CONFIG_PATH", "")
	t.Setenv("INFERENCE_ATTRIBUTION_INPUT_TOKENS_SERIES", "litellm_input_tokens")
	t.Setenv("INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_SERIES", "")
	t.Setenv("INFERENCE_ATTRIBUTION_INPUT_TOKENS_ESTIMATED_SERIES", "")
	t.Setenv("INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_ESTIMATED_SERIES", "")
	t.Setenv("INFERENCE_ATTRIBUTION_TENANT_LABEL", "team_alias")
	t.Setenv("INFERENCE_ATTRIBUTION_WORKLOAD_LABEL", "")
	t.Setenv("INFERENCE_ATTRIBUTION_USER_LABEL", "")
	t.Setenv("INFERENCE_ATTRIBUTION_MODEL_LABEL", "model")
	t.Setenv("INFERENCE_ATTRIBUTION_NAMESPACE_LABEL", "deployment_namespace")

	// Two series differing only in user and workload labels collapse to one key.
	mk := func(user, wl, v string) string {
		return `{"metric":{"end_user":"` + user + `","api_key_alias":"` + wl + `","team_alias":"acme","model":"gemma"},"value":[1,"` + v + `"]}`
	}
	client := &seriesPromClient{end: `[` + mk("alice", "rag", "10") + `,` + mk("bob", "batch", "5") + `]`, start: `[]`}
	cfg := &OpenCostPrometheusConfig{ClusterLabel: "cluster_id"}
	querier := newPrometheusMetricsQuerier(cfg, client, NewContextFactory(client, cfg))

	res, err := querier.QueryInferenceDimensionTokens(time.Now().Add(-120*time.Second), time.Now()).Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var keys, total = 0, 0.0
	for _, r := range res {
		for k, v := range r.Values {
			keys++
			total += v.PromptTokens
			if k.UserID != "" || k.WorkloadID != "" {
				t.Errorf("key carries a disabled label: %+v", k)
			}
		}
	}
	if keys != 1 || total != 15 {
		t.Errorf("expected 1 key totalling 15, got %d keys totalling %v", keys, total)
	}
}

// A reset between two samples inside the window must not be lost: 100 -> 180 ->
// 0 -> 300 is 80 + 0 + 300 = 380 new tokens, not 300 - 100 = 200.
func TestQueryInferenceDimensionTokens_MidWindowCounterReset(t *testing.T) {
	end := `[{"metric":{"end_user":"alice","team_alias":"acme","api_key_alias":"rag","model":"gemma","instance":"a:4000"},"values":[[1,"180"],[2,"0"],[3,"300"]]}]`
	start := `[{"metric":{"end_user":"alice","team_alias":"acme","api_key_alias":"rag","model":"gemma","instance":"a:4000"},"value":[1,"100"]}]`
	got := queryDimensionTokensWith(t, end, start)

	if want := 380.0; got["alice/acme/rag/gemma"] != want {
		t.Errorf("expected reset-aware delta %v, got %v", want, got)
	}
}

// Label values containing the join separators must not make two different
// series share an identity (and therefore a start value).
func TestSeriesIdentity_NoCollisionOnSeparators(t *testing.T) {
	a := map[string]interface{}{"tenant_id": "a,user_id=b", "user_id": "c"}
	b := map[string]interface{}{"tenant_id": "a", "user_id": "b,user_id=c"}
	if seriesIdentity(a) == seriesIdentity(b) {
		t.Errorf("distinct label sets share identity %q", seriesIdentity(a))
	}
}

// Estimated tokens come from a separate series, so they must count in the
// totals as well as in the estimated subtotals.
func TestQueryInferenceDimensionTokens_EstimatedCountsInTotals(t *testing.T) {
	t.Setenv("INFERENCE_ATTRIBUTION_ENABLED", "true")
	t.Setenv("INFERENCE_ATTRIBUTION_CONFIG_PATH", "")
	t.Setenv("INFERENCE_ATTRIBUTION_INPUT_TOKENS_SERIES", "")
	t.Setenv("INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_SERIES", "out_total")
	t.Setenv("INFERENCE_ATTRIBUTION_INPUT_TOKENS_ESTIMATED_SERIES", "est_in_total")
	t.Setenv("INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_ESTIMATED_SERIES", "")
	t.Setenv("INFERENCE_ATTRIBUTION_TENANT_LABEL", "team_alias")
	t.Setenv("INFERENCE_ATTRIBUTION_WORKLOAD_LABEL", "")
	t.Setenv("INFERENCE_ATTRIBUTION_USER_LABEL", "")
	t.Setenv("INFERENCE_ATTRIBUTION_MODEL_LABEL", "model")
	t.Setenv("INFERENCE_ATTRIBUTION_NAMESPACE_LABEL", "deployment_namespace")

	client := &seriesPromClient{
		end:   `[{"metric":{"team_alias":"acme","model":"gemma"},"value":[1,"40"]}]`,
		start: `[]`,
	}
	cfg := &OpenCostPrometheusConfig{ClusterLabel: "cluster_id"}
	querier := newPrometheusMetricsQuerier(cfg, client, NewContextFactory(client, cfg))

	res, err := querier.QueryInferenceDimensionTokens(time.Now().Add(-60*time.Second), time.Now()).Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var found bool
	for _, r := range res {
		for _, v := range r.Values {
			found = true
			if v.PromptTokens != 40 || v.EstimatedPromptTokens != 40 {
				t.Errorf("estimated input must be in totals and subtotals, got %+v", v)
			}
			if v.CompletionTokens != 40 || v.EstimatedCompletionTokens != 0 {
				t.Errorf("exact output must not be estimated, got %+v", v)
			}
		}
	}
	if !found {
		t.Fatal("no dimension result returned")
	}
}
