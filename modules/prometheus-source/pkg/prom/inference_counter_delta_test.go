package prom

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// counterDeltaClient dispatches canned vector responses keyed by a substring of
// the PromQL query. The end-of-window query carries the full window as its
// lookback and the start-of-window query a fixed 2m one, so "[2m] @" selects
// the start endpoint and "[60m] @" the end endpoint.
type counterDeltaClient struct {
	responses map[string]string
	queries   []string
}

func (c *counterDeltaClient) URL(ep string, args map[string]string) *url.URL {
	u, _ := url.Parse("http://fake-prometheus:9090" + ep)
	return u
}

func (c *counterDeltaClient) Do(_ context.Context, req *http.Request) (*http.Response, []byte, error) {
	query := req.URL.Query().Get("query")
	c.queries = append(c.queries, query)

	result := "[]"
	for substr, res := range c.responses {
		if strings.Contains(query, substr) {
			result = res
			break
		}
	}
	body := fmt.Sprintf(`{"status":"success","data":{"resultType":"vector","result":%s}}`, result)
	return &http.Response{StatusCode: http.StatusOK}, []byte(body), nil
}

func podSample(modelName, namespace, pod string, value float64) string {
	return fmt.Sprintf(`{"metric":{"model_name":"%s","namespace":"%s","pod":"%s"},"value":[1712000000,"%g"]}`,
		modelName, namespace, pod, value)
}

func podlessSample(modelName, namespace string, value float64) string {
	return fmt.Sprintf(`{"metric":{"model_name":"%s","namespace":"%s"},"value":[1712000000,"%g"]}`,
		modelName, namespace, value)
}

func newCounterDeltaCtx(responses map[string]string) (*Context, *counterDeltaClient) {
	client := &counterDeltaClient{responses: responses}
	config := &OpenCostPrometheusConfig{ClusterLabel: "cluster_id"}
	factory := NewContextFactory(client, config)
	return factory.NewNamedContext(ClusterContextName), client
}

// TestQueryCounterDelta_ReplicaResetIsNotMaskedByAnotherReplica is the
// regression test for the undercount reported by @dev404ai on #3915.
//
// One model, one namespace, two replicas over the same window:
//
//	replica A: 900 -> 100 after a reset, so its reset-aware lower bound is 100
//	replica B: 100 -> 1000, so its delta is 900
//
// The reset-aware model total is 1000. Summing the replicas at each endpoint
// before subtracting reads 1000 -> 1100 and returns 100, and the reset guard
// never fires because the aggregate delta is positive.
func TestQueryCounterDelta_ReplicaResetIsNotMaskedByAnotherReplica(t *testing.T) {
	end := time.Now()
	start := end.Add(-60 * time.Minute)

	ctx, _ := newCounterDeltaCtx(map[string]string{
		"[2m] @": "[" +
			podSample("Qwen3-32B", "llm-d", "vllm-a", 900) + "," +
			podSample("Qwen3-32B", "llm-d", "vllm-b", 100) + "]",
		"[60m] @": "[" +
			podSample("Qwen3-32B", "llm-d", "vllm-a", 100) + "," +
			podSample("Qwen3-32B", "llm-d", "vllm-b", 1000) + "]",
	})

	got, err := queryCounterDelta(ctx, "vllm:prompt_tokens_total", start, end)
	if err != nil {
		t.Fatalf("queryCounterDelta: %v", err)
	}

	const want = 1000.0
	if got["Qwen3-32B:llm-d"] != want {
		t.Errorf("delta = %v, want %v (100 post-reset from A plus 900 from B); "+
			"any other value means replica identity was not preserved through the subtraction",
			got["Qwen3-32B:llm-d"], want)
	}
}

// TestQueryCounterDelta_ReplicaResetIsNotOvercounted covers the other
// direction of the same erasure, which the original report did not need to hit.
//
//	replica A: 900 -> 100 (reset), lower bound 100
//	replica B: 100 -> 150, delta 50
//
// True total 150. Aggregated, the endpoints read 1000 -> 250, the delta is
// negative, so the reset guard fires and returns the aggregate end value of
// 250. Applying a per-series heuristic to an aggregate is unbounded in sign.
func TestQueryCounterDelta_ReplicaResetIsNotOvercounted(t *testing.T) {
	end := time.Now()
	start := end.Add(-60 * time.Minute)

	ctx, _ := newCounterDeltaCtx(map[string]string{
		"[2m] @": "[" +
			podSample("Qwen3-32B", "llm-d", "vllm-a", 900) + "," +
			podSample("Qwen3-32B", "llm-d", "vllm-b", 100) + "]",
		"[60m] @": "[" +
			podSample("Qwen3-32B", "llm-d", "vllm-a", 100) + "," +
			podSample("Qwen3-32B", "llm-d", "vllm-b", 150) + "]",
	})

	got, err := queryCounterDelta(ctx, "vllm:prompt_tokens_total", start, end)
	if err != nil {
		t.Fatalf("queryCounterDelta: %v", err)
	}

	const want = 150.0
	if got["Qwen3-32B:llm-d"] != want {
		t.Errorf("delta = %v, want %v (100 post-reset from A plus 50 from B); "+
			"any other value means the reset guard was applied to an aggregate rather than per replica",
			got["Qwen3-32B:llm-d"], want)
	}
}

// TestQueryCounterDelta_SeriesWithoutPodLabelStillCount pins the degradation
// path. `pod` is never self-reported by the serving engine; it comes from
// Kubernetes service discovery relabeling, so it is legitimately absent under
// non-Kubernetes SD, under relabel rules that strip it, and under federation of
// pre-aggregated recording rules. Those deployments must keep reporting at the
// previous accuracy rather than losing their token totals.
// The endpoint values here are the aggregates the two-replica scenario above
// induces (900+100 at start, 100+1000 at end), which is exactly what Prometheus
// returned for every deployment before this change. The resulting 100 against a
// true 1000 is the reported undercount, preserved here as the documented
// behaviour of the degraded path rather than as a correct answer.
func TestQueryCounterDelta_SeriesWithoutPodLabelStillCount(t *testing.T) {
	end := time.Now()
	start := end.Add(-60 * time.Minute)

	ctx, _ := newCounterDeltaCtx(map[string]string{
		"[2m] @":  "[" + podlessSample("Qwen3-32B", "llm-d", 1000) + "]",
		"[60m] @": "[" + podlessSample("Qwen3-32B", "llm-d", 1100) + "]",
	})

	got, err := queryCounterDelta(ctx, "vllm:prompt_tokens_total", start, end)
	if err != nil {
		t.Fatalf("queryCounterDelta: %v", err)
	}

	const want = 100.0
	if got["Qwen3-32B:llm-d"] != want {
		t.Errorf("delta = %v, want %v; series without a pod label must still be counted, "+
			"at the previous accuracy rather than dropped", got["Qwen3-32B:llm-d"], want)
	}
}

// TestQueryCounterDelta_GroupsByPod pins the query shape, since the per-replica
// correction is only possible if both endpoints carry pod in the by-clause.
func TestQueryCounterDelta_GroupsByPod(t *testing.T) {
	end := time.Now()
	start := end.Add(-60 * time.Minute)

	ctx, client := newCounterDeltaCtx(map[string]string{
		"[2m] @":  "[" + podSample("Qwen3-32B", "llm-d", "vllm-a", 1) + "]",
		"[60m] @": "[" + podSample("Qwen3-32B", "llm-d", "vllm-a", 2) + "]",
	})

	if _, err := queryCounterDelta(ctx, "vllm:prompt_tokens_total", start, end); err != nil {
		t.Fatalf("queryCounterDelta: %v", err)
	}
	if len(client.queries) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(client.queries))
	}
	for _, q := range client.queries {
		if !strings.Contains(q, "sum by (model_name, namespace, pod)") {
			t.Errorf("endpoint query does not group by pod, so resets cannot be corrected per replica: %q", q)
		}
	}
}
