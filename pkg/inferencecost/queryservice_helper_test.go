package inferencecost

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

// makeQP is a small helper that converts a plain map into an httputil.QueryParams
// so tests can call ParseInferenceCostRequest without spinning up HTTP.
func makeQP(pairs map[string]string) httputil.QueryParams {
	raw := make(map[string][]string, len(pairs))
	for k, v := range pairs {
		raw[k] = []string{v}
	}
	return httputil.NewQueryParams(raw)
}

// validWindowStr returns a well-formed RFC3339 window string covering [yesterday, now].
func validWindowStr() string {
	now := time.Now().UTC().Truncate(time.Hour)
	start := now.Add(-24 * time.Hour)
	return start.Format(time.RFC3339) + "," + now.Format(time.RFC3339)
}

// --- ParseInferenceCostRequest ---

func TestParseInferenceCostRequest_MissingWindow(t *testing.T) {
	_, err := ParseInferenceCostRequest(makeQP(map[string]string{}))
	if err == nil {
		t.Fatal("expected error for missing window, got nil")
	}
}

func TestParseInferenceCostRequest_InvalidWindow(t *testing.T) {
	_, err := ParseInferenceCostRequest(makeQP(map[string]string{
		"window": "not-a-window",
	}))
	if err == nil {
		t.Fatal("expected error for invalid window, got nil")
	}
}

func TestParseInferenceCostRequest_DefaultBasis(t *testing.T) {
	req, err := ParseInferenceCostRequest(makeQP(map[string]string{
		"window": validWindowStr(),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.CostBasis != CostBasisAllocation {
		t.Errorf("default CostBasis = %q, want %q", req.CostBasis, CostBasisAllocation)
	}
}

func TestParseInferenceCostRequest_UsageBasis(t *testing.T) {
	req, err := ParseInferenceCostRequest(makeQP(map[string]string{
		"window":    validWindowStr(),
		"costBasis": "usage",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.CostBasis != CostBasisUsage {
		t.Errorf("CostBasis = %q, want %q", req.CostBasis, CostBasisUsage)
	}
}

func TestParseInferenceCostRequest_InvalidCostBasis(t *testing.T) {
	_, err := ParseInferenceCostRequest(makeQP(map[string]string{
		"window":    validWindowStr(),
		"costBasis": "bogus",
	}))
	if err == nil {
		t.Fatal("expected error for invalid costBasis, got nil")
	}
}

func TestParseInferenceCostRequest_ValidAggregation(t *testing.T) {
	req, err := ParseInferenceCostRequest(makeQP(map[string]string{
		"window":    validWindowStr(),
		"aggregate": "model_name,namespace",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(req.AggregateBy) != 2 {
		t.Errorf("AggregateBy len = %d, want 2", len(req.AggregateBy))
	}
}

func TestParseInferenceCostRequest_UnsupportedAggregateDimension(t *testing.T) {
	_, err := ParseInferenceCostRequest(makeQP(map[string]string{
		"window":    validWindowStr(),
		"aggregate": "product",
	}))
	if err == nil {
		t.Fatal("expected error for unsupported aggregate dimension, got nil")
	}
}

func TestParseInferenceCostRequest_ValidFilter(t *testing.T) {
	req, err := ParseInferenceCostRequest(makeQP(map[string]string{
		"window": validWindowStr(),
		"filter": `namespace:"llm-prod"`,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(req.Filter) != 1 {
		t.Errorf("Filter len = %d, want 1", len(req.Filter))
	}
	if req.Filter[0].property != "namespace" || req.Filter[0].value != "llm-prod" {
		t.Errorf("Filter[0] = %+v, want {namespace, llm-prod}", req.Filter[0])
	}
}

func TestParseInferenceCostRequest_InvalidFilterProperty(t *testing.T) {
	_, err := ParseInferenceCostRequest(makeQP(map[string]string{
		"window": validWindowStr(),
		"filter": `product:"team-a"`,
	}))
	if err == nil {
		t.Fatal("expected error for unsupported filter property, got nil")
	}
}

func TestParseInferenceCostRequest_AccumulateDay(t *testing.T) {
	req, err := ParseInferenceCostRequest(makeQP(map[string]string{
		"window":     validWindowStr(),
		"accumulate": "day",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Accumulate != opencost.AccumulateOptionDay {
		t.Errorf("Accumulate = %q, want %q", req.Accumulate, opencost.AccumulateOptionDay)
	}
	if req.Step != 24*time.Hour {
		t.Errorf("Step = %s, want 24h", req.Step)
	}
}

// --- ParseInferenceCostTimeseriesRequest ---

func TestParseInferenceCostTimeseriesRequest_MissingAccumulate(t *testing.T) {
	_, err := ParseInferenceCostTimeseriesRequest(makeQP(map[string]string{
		"window": validWindowStr(),
	}))
	if err == nil {
		t.Fatal("expected error when accumulate is missing for timeseries, got nil")
	}
}

func TestParseInferenceCostTimeseriesRequest_Valid(t *testing.T) {
	req, err := ParseInferenceCostTimeseriesRequest(makeQP(map[string]string{
		"window":     validWindowStr(),
		"accumulate": "hour",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Accumulate != opencost.AccumulateOptionHour {
		t.Errorf("Accumulate = %q, want %q", req.Accumulate, opencost.AccumulateOptionHour)
	}
	if req.Step != time.Hour {
		t.Errorf("Step = %s, want 1h", req.Step)
	}
}
