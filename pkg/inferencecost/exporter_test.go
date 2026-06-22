package inferencecost

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// newTestExporter creates a fresh Exporter registered on an isolated Prometheus
// registry so tests don't conflict with each other or the default registry.
func newTestExporter(t *testing.T) (*Exporter, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	e := NewExporter()
	for _, c := range []prometheus.Collector{
		e.totalCost,
		e.costPerMillionTokens,
		e.cacheSavingsFraction,
	} {
		if err := reg.Register(c); err != nil {
			t.Fatalf("failed to register collector: %v", err)
		}
	}
	return e, reg
}

func sampleMetric(method AllocationMethod) *InferenceCost {
	return &InferenceCost{
		Properties: InferenceCostProperties{
			ModelName:    "meta-llama/Llama-3.1-8B",
			ModelVersion: "v1",
			Namespace:    "llm-prod",
		},
		AllocationTotalCost: 4.0,
		UsageTotalCost:      1.0,
		TotalTokens:         1_000_000,
		EffectiveInputTokens: 800_000,
		GenerationTokens:    200_000,
		AllocationMethod:    method,
		CostPerMillionTokens: map[CostBasis]float64{
			CostBasisAllocation: 4.0,
			CostBasisUsage:      1.0,
		},
		InputCostPerMillionTokens: map[CostBasis]float64{
			CostBasisAllocation: 3.5,
			CostBasisUsage:      0.875,
		},
		OutputCostPerMillionTokens: map[CostBasis]float64{
			CostBasisAllocation: 7.0,
			CostBasisUsage:      1.75,
		},
		Timestamp: time.Now(),
	}
}

// TestExporter_MetricNames verifies that exported metric names are llm_* not opencost_inference_*.
func TestExporter_MetricNames(t *testing.T) {
	e, reg := newTestExporter(t)
	e.Export([]*InferenceCost{sampleMetric(AllocationMethodComputeTime)})

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}

	required := []string{
		"llm_total_cost",
		"llm_cost_per_million_tokens",
		"llm_cache_savings_fraction",
	}
	for _, want := range required {
		found := false
		for _, got := range names {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("metric %q not found; registered names: %v", want, names)
		}
	}

	for _, name := range names {
		if strings.HasPrefix(name, "opencost_inference") {
			t.Errorf("found deprecated metric name %q — should be llm_*", name)
		}
	}
}

// TestExporter_TwoCostBasisSeriesPerModel verifies that llm_total_cost produces
// two series (usage + allocation) and llm_cost_per_million_tokens produces
// six series (2 cost bases × 3 phase values: blended/"", prompt, generation).
func TestExporter_TwoCostBasisSeriesPerModel(t *testing.T) {
	e, reg := newTestExporter(t)
	e.Export([]*InferenceCost{sampleMetric(AllocationMethodComputeTime)})

	// llm_total_cost should have 2 series (usage + allocation)
	count := testutil.CollectAndCount(e.totalCost)
	if count != 2 {
		t.Errorf("llm_total_cost: expected 2 series (usage+allocation), got %d", count)
	}

	// llm_cost_per_million_tokens should have 6 series:
	// 2 cost bases × 3 phases (blended/"", prompt, generation)
	count = testutil.CollectAndCount(e.costPerMillionTokens)
	if count != 6 {
		t.Errorf("llm_cost_per_million_tokens: expected 6 series (2 bases × 3 phases), got %d", count)
	}

	// Verify both cost_basis values are present for llm_total_cost.
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() != "llm_total_cost" {
			continue
		}
		bases := make(map[string]bool)
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "cost_basis" {
					bases[lp.GetValue()] = true
				}
			}
		}
		if !bases["usage"] {
			t.Error("llm_total_cost missing cost_basis=usage series")
		}
		if !bases["allocation"] {
			t.Error("llm_total_cost missing cost_basis=allocation series")
		}
	}
}

// TestExporter_PhaseLabelsAndAllocationMethod verifies that
// llm_cost_per_million_tokens has the correct phase labels and allocation_method.
func TestExporter_PhaseLabelsAndAllocationMethod(t *testing.T) {
	for _, method := range []AllocationMethod{
		AllocationMethodComputeTime,
		AllocationMethodPrefixCachingOff,
		AllocationMethodMultiplier,
	} {
		e, reg := newTestExporter(t)
		e.Export([]*InferenceCost{sampleMetric(method)})

		mfs, _ := reg.Gather()
		for _, mf := range mfs {
			name := mf.GetName()
			if name != "llm_cost_per_million_tokens" {
				continue
			}
			bases := make(map[string]bool)
			phases := make(map[string]bool)
			methods := make(map[string]bool)
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					switch lp.GetName() {
					case "cost_basis":
						bases[lp.GetValue()] = true
					case "phase":
						phases[lp.GetValue()] = true
					case "allocation_method":
						methods[lp.GetValue()] = true
					}
				}
			}
			if !bases["usage"] || !bases["allocation"] {
				t.Errorf("%s method=%s: missing cost_basis label values, got %v", name, method, bases)
			}
			// Should have 3 phase values: "" (blended), "prompt", "generation"
			if !phases[""] || !phases["prompt"] || !phases["generation"] {
				t.Errorf("%s method=%s: expected phases [\"\", \"prompt\", \"generation\"], got %v", name, method, phases)
			}
			// allocation_method should be present (for phase=prompt and phase=generation)
			// and empty (for blended phase="")
			if !methods[string(method)] || !methods[""] {
				t.Errorf("%s: expected allocation_method values [%s, \"\"], got %v", name, method, methods)
			}
		}
	}
}

// TestExporter_HelpStringsContainReconciliationNote verifies that usage-basis
// metrics document that they do not reconcile to the bill.
func TestExporter_HelpStringsContainReconciliationNote(t *testing.T) {
	e, reg := newTestExporter(t)
	e.Export([]*InferenceCost{sampleMetric(AllocationMethodComputeTime)})

	mfs, _ := reg.Gather()
	reconciliationKeyword := "does NOT reconcile"

	for _, mf := range mfs {
		name := mf.GetName()
		if name != "llm_total_cost" && name != "llm_cost_per_million_tokens" {
			continue
		}
		help := mf.GetHelp()
		if !strings.Contains(help, reconciliationKeyword) {
			t.Errorf("%s Help string should mention reconciliation, got: %q", name, help)
		}
	}
}

// TestExporter_CacheSavingsFraction verifies that llm_cache_savings_fraction is exported correctly.
func TestExporter_CacheSavingsFraction(t *testing.T) {
	e, reg := newTestExporter(t)
	ic := sampleMetric(AllocationMethodComputeTime)
	ic.CacheSavingsFraction = 0.4
	e.Export([]*InferenceCost{ic})

	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() != "llm_cache_savings_fraction" {
			continue
		}
		if len(mf.GetMetric()) != 1 {
			t.Fatalf("expected 1 series for llm_cache_savings_fraction, got %d", len(mf.GetMetric()))
		}
		val := mf.GetMetric()[0].GetGauge().GetValue()
		if !floatEq(val, 0.4) {
			t.Errorf("llm_cache_savings_fraction want 0.4 got %f", val)
		}
		return
	}
	t.Error("llm_cache_savings_fraction metric not found")
}

// TestExporter_Values verifies that exported gauge values match InferenceCost fields.
func TestExporter_Values(t *testing.T) {
	e, reg := newTestExporter(t)
	ic := sampleMetric(AllocationMethodComputeTime)
	e.Export([]*InferenceCost{ic})

	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() != "llm_total_cost" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var basis string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "cost_basis" {
					basis = lp.GetValue()
				}
			}
			val := m.GetGauge().GetValue()
			switch basis {
			case "allocation":
				if !floatEq(val, ic.AllocationTotalCost) {
					t.Errorf("llm_total_cost allocation want %f got %f", ic.AllocationTotalCost, val)
				}
			case "usage":
				if !floatEq(val, ic.UsageTotalCost) {
					t.Errorf("llm_total_cost usage want %f got %f", ic.UsageTotalCost, val)
				}
			}
		}
	}
}
