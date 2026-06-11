package inferencecost

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// --- Fake Prometheus server ---

// promSample is one model/namespace series value for a fake Prometheus response.
type promSample struct {
	modelName string
	namespace string
	value     float64
}

// promInstantJSON returns a minimal Prometheus API v1 instant-query JSON
// payload containing the given vector samples.
func promInstantJSON(samples []promSample) string {
	type result struct {
		Metric map[string]string `json:"metric"`
		Value  [2]interface{}    `json:"value"`
	}
	results := make([]result, 0, len(samples))
	for _, s := range samples {
		results = append(results, result{
			Metric: map[string]string{
				"model_name": s.modelName,
				"namespace":  s.namespace,
			},
			Value: [2]interface{}{1.0, fmt.Sprintf("%f", s.value)},
		})
	}
	data := map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result":     results,
		},
	}
	b, _ := json.Marshal(data)
	return string(b)
}

// newFakePromServer returns a *httptest.Server that responds to any
// /api/v1/query request with the provided samples.
func newFakePromServer(t *testing.T, samples []promSample) *httptest.Server {
	t.Helper()
	body := promInstantJSON(samples)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body)) //nolint:errcheck
	}))
}

// --- HTTP handler tests (do not require real Prometheus / allocation data) ---

func TestQueryService_NilCollector_Returns501(t *testing.T) {
	qs := &QueryService{collector: nil, calculator: nil}
	handler := qs.GetInferenceCostTotalHandler()

	req, _ := http.NewRequest(http.MethodGet, "/inferenceCost/total?window=2024-01-01T00:00:00Z,2024-01-02T00:00:00Z", nil)
	rr := httptest.NewRecorder()
	handler(rr, req, nil)

	if rr.Code != http.StatusNotImplemented {
		t.Errorf("total handler: status = %d, want %d (501)", rr.Code, http.StatusNotImplemented)
	}

	handler2 := qs.GetInferenceCostTimeseriesHandler()
	rr2 := httptest.NewRecorder()
	handler2(rr2, req, nil)
	if rr2.Code != http.StatusNotImplemented {
		t.Errorf("timeseries handler: status = %d, want %d (501)", rr2.Code, http.StatusNotImplemented)
	}
}

func TestQueryService_MissingWindow_Returns400(t *testing.T) {
	// Use a real but unavailable Prometheus so CollectMetrics never runs.
	srv := newFakePromServer(t, nil)
	defer srv.Close()
	cfg := baseConfig()
	cfg.PrometheusURL = srv.URL
	querier := &mockQuerier{set: opencost.NewAllocationSet(time.Now().Add(-time.Hour), time.Now())}
	collector, err := NewCollector(cfg, querier)
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	qs := NewQueryService(collector, NewCalculator(cfg))

	handler := qs.GetInferenceCostTotalHandler()
	req, _ := http.NewRequest(http.MethodGet, "/inferenceCost/total", nil) // no window
	rr := httptest.NewRecorder()
	handler(rr, req, nil)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestQueryService_InvalidCostBasis_Returns400(t *testing.T) {
	srv := newFakePromServer(t, nil)
	defer srv.Close()
	cfg := baseConfig()
	cfg.PrometheusURL = srv.URL
	querier := &mockQuerier{set: opencost.NewAllocationSet(time.Now().Add(-time.Hour), time.Now())}
	collector, _ := NewCollector(cfg, querier)
	qs := NewQueryService(collector, NewCalculator(cfg))

	handler := qs.GetInferenceCostTotalHandler()
	req, _ := http.NewRequest(http.MethodGet, "/inferenceCost/total?window=2024-01-01T00:00:00Z,2024-01-02T00:00:00Z&costBasis=bogus", nil)
	rr := httptest.NewRecorder()
	handler(rr, req, nil)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for invalid costBasis", rr.Code)
	}
}

func TestQueryService_TimeseriesMissingAccumulate_Returns400(t *testing.T) {
	srv := newFakePromServer(t, nil)
	defer srv.Close()
	cfg := baseConfig()
	cfg.PrometheusURL = srv.URL
	querier := &mockQuerier{set: opencost.NewAllocationSet(time.Now().Add(-time.Hour), time.Now())}
	collector, _ := NewCollector(cfg, querier)
	qs := NewQueryService(collector, NewCalculator(cfg))

	handler := qs.GetInferenceCostTimeseriesHandler()
	req, _ := http.NewRequest(http.MethodGet, "/inferenceCost/timeseries?window=2024-01-01T00:00:00Z,2024-01-02T00:00:00Z", nil)
	rr := httptest.NewRecorder()
	handler(rr, req, nil)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for missing accumulate", rr.Code)
	}
}

// --- query() unit tests using pre-built InferenceCost structs ---
//
// computeStep calls CollectMetrics → CalculateCosts → project/filter/aggregate.
// We can test the projection and aggregation layers directly by using
// combineMetrics with pre-built allocationResult and token maps (bypassing the
// AggregateBy path in the real collector which requires a live K8s label setup).

func TestQuery_SingleStep_ReturnsOneSet(t *testing.T) {
	qs, srv := newQueryServiceWithDirectMetrics(t, "llama-3", "llm-prod", 10.0, 1_000_000)
	defer srv.Close()

	now := time.Now().UTC()
	req := &QueryRequest{
		Start:     now.Add(-time.Hour),
		End:       now,
		CostBasis: CostBasisAllocation,
		Step:      time.Hour,
	}

	setRange, err := qs.query(context.Background(), req)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(setRange.InferenceCostSets) != 1 {
		t.Errorf("expected 1 set for a single step, got %d", len(setRange.InferenceCostSets))
	}
}

func TestQuery_TwoSteps_ReturnsTwoSets(t *testing.T) {
	qs, srv := newQueryServiceWithDirectMetrics(t, "llama-3", "llm-prod", 5.0, 500_000)
	defer srv.Close()

	now := time.Now().UTC().Truncate(time.Hour)
	req := &QueryRequest{
		Start:      now.Add(-2 * time.Hour),
		End:        now,
		CostBasis:  CostBasisAllocation,
		Accumulate: opencost.AccumulateOptionHour,
		Step:       time.Hour,
	}

	setRange, err := qs.query(context.Background(), req)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(setRange.InferenceCostSets) != 2 {
		t.Errorf("expected 2 sets for 2-hour range at 1-hour step, got %d", len(setRange.InferenceCostSets))
	}
}

func TestQuery_Filter_ExcludesNonMatchingNamespace(t *testing.T) {
	qs, srv := newQueryServiceWithDirectMetrics(t, "llama-3", "llm-prod", 10.0, 1_000_000)
	defer srv.Close()

	filterSpecs, _ := parseFilter(`namespace:"other-ns"`)
	now := time.Now().UTC()
	req := &QueryRequest{
		Start:     now.Add(-time.Hour),
		End:       now,
		CostBasis: CostBasisAllocation,
		Step:      time.Hour,
		Filter:    filterSpecs,
	}

	setRange, err := qs.query(context.Background(), req)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	total := accumulate(setRange.InferenceCostSets)
	if len(total.InferenceCosts) != 0 {
		t.Errorf("expected 0 entries after filtering to non-matching namespace, got %d", len(total.InferenceCosts))
	}
}

func TestQuery_Filter_MatchingNamespace(t *testing.T) {
	qs, srv := newQueryServiceWithDirectMetrics(t, "llama-3", "llm-prod", 10.0, 1_000_000)
	defer srv.Close()

	filterSpecs, _ := parseFilter(`namespace:"llm-prod"`)
	now := time.Now().UTC()
	req := &QueryRequest{
		Start:     now.Add(-time.Hour),
		End:       now,
		CostBasis: CostBasisAllocation,
		Step:      time.Hour,
		Filter:    filterSpecs,
	}

	setRange, err := qs.query(context.Background(), req)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	total := accumulate(setRange.InferenceCostSets)
	if len(total.InferenceCosts) == 0 {
		t.Error("expected at least one entry after filtering to matching namespace, got 0")
	}
}

func TestQuery_UsageBasis_LowerThanAllocation(t *testing.T) {
	// Usage costs should be <= allocation costs (no idle sharing).
	// The fake allocation querier returns fixed costs; the multiplier calculator
	// sets both basis costs from the allocationResult directly.
	qs, srv := newQueryServiceWithDirectMetrics(t, "llama-3", "llm-prod", 10.0, 1_000_000)
	defer srv.Close()

	now := time.Now().UTC()

	queryBasis := func(basis CostBasis) float64 {
		req := &QueryRequest{
			Start:     now.Add(-time.Hour),
			End:       now,
			CostBasis: basis,
			Step:      time.Hour,
		}
		setRange, err := qs.query(context.Background(), req)
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		total := accumulate(setRange.InferenceCostSets)
		var tc float64
		for _, ic := range total.InferenceCosts {
			tc += ic.TotalCost
		}
		return tc
	}

	allocCost := queryBasis(CostBasisAllocation)
	usageCost := queryBasis(CostBasisUsage)

	// With ShareNone for usage (default config), usage cost ≤ allocation cost.
	if usageCost > allocCost {
		t.Errorf("usageCost (%.4f) > allocCost (%.4f): unexpected", usageCost, allocCost)
	}
}

// newQueryServiceWithDirectMetrics builds a QueryService backed by a fake
// Prometheus server and an allocation querier that returns a pre-built
// AllocationSet with the model label set in its sanitized form.
//
// OpenCost's AggregateBy("label:llm-d.ai/model") sanitizes the label key
// via promutil.SanitizeLabelName before lookup, converting non-alphanumeric
// characters to underscores. So we must store the label under the sanitized
// key "llm_d_ai_model" for the allocation to be found and named correctly
// after aggregation.
func newQueryServiceWithDirectMetrics(t *testing.T, modelName, namespace string, allocCost, tokenCount float64) (*QueryService, *httptest.Server) {
	t.Helper()

	samples := []promSample{
		{modelName: modelName, namespace: namespace, value: tokenCount},
	}
	srv := newFakePromServer(t, samples)

	cfg := baseConfig()
	cfg.PrometheusURL = srv.URL
	cfg.AllocationMode = AllocationModeMultiplier
	cfg.OutputTokenCostMultiplier = 2.5
	// cfg.ModelLabel = "llm-d.ai/model" (default from baseConfig)

	// The sanitized form of "llm-d.ai/model" that AggregateBy uses for lookup.
	sanitizedLabelKey := "llm_d_ai_model"

	querier := &mockQuerier{
		set: func() *opencost.AllocationSet {
			now := time.Now()
			as := opencost.NewAllocationSet(now.Add(-time.Hour), now)
			a := &opencost.Allocation{
				Name:    modelName,
				GPUCost: allocCost,
				Properties: &opencost.AllocationProperties{
					Namespace: namespace,
					Labels: opencost.AllocationLabels{
						sanitizedLabelKey: modelName,
					},
				},
			}
			as.Set(a)
			return as
		}(),
	}

	collector, err := NewCollector(cfg, querier)
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	calculator := NewCalculator(cfg)
	return NewQueryService(collector, calculator), srv
}
