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
		e.dimensionCost,
		e.unmatchedKeys,
		e.tokenCoverage,
		e.identityCoverage,
		e.estimatedTokenShare,
	} {
		if err := reg.Register(c); err != nil {
			t.Fatalf("failed to register collector: %v", err)
		}
	}
	return e, reg
}

func sampleMetric(method AllocationMethod) *InferenceCost {
	now := time.Now()
	ic := &InferenceCost{
		Properties: InferenceCostProperties{
			ModelName:    "meta-llama/Llama-3.1-8B",
			ModelVersion: "v1",
			Namespace:    "llm-prod",
		},
		AllocationTotalCost:  4.0,
		UsageTotalCost:       1.0,
		TotalTokens:          1_000_000,
		EffectiveInputTokens: 800_000,
		GenerationTokens:     200_000,
		AllocationMethod:     method,
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
		Timestamp: now,
	}
	// Set a 1-hour window so costs are already normalized (4.0 for 1 hour = 4.0/hour)
	ic.Window.Start = now.Add(-1 * time.Hour)
	ic.Window.End = now
	return ic
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
		"llm_total_hourly_cost",
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

// TestExporter_TwoCostBasisSeriesPerModel verifies that llm_total_hourly_cost produces
// two series (usage + allocation) and llm_cost_per_million_tokens produces
// six series (2 cost bases × 3 phase values: blended/"", prompt, generation).
func TestExporter_TwoCostBasisSeriesPerModel(t *testing.T) {
	e, reg := newTestExporter(t)
	e.Export([]*InferenceCost{sampleMetric(AllocationMethodComputeTime)})

	// llm_total_hourly_cost should have 2 series (usage + allocation)
	count := testutil.CollectAndCount(e.totalCost)
	if count != 2 {
		t.Errorf("llm_total_hourly_cost: expected 2 series (usage+allocation), got %d", count)
	}

	// llm_cost_per_million_tokens should have 6 series:
	// 2 cost bases × 3 phases (blended/"", prompt, generation)
	count = testutil.CollectAndCount(e.costPerMillionTokens)
	if count != 6 {
		t.Errorf("llm_cost_per_million_tokens: expected 6 series (2 bases × 3 phases), got %d", count)
	}

	// Verify both cost_basis values are present for llm_total_hourly_cost.
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() != "llm_total_hourly_cost" {
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
			t.Error("llm_total_hourly_cost missing cost_basis=usage series")
		}
		if !bases["allocation"] {
			t.Error("llm_total_hourly_cost missing cost_basis=allocation series")
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
		if name != "llm_total_hourly_cost" && name != "llm_cost_per_million_tokens" {
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
		if mf.GetName() != "llm_total_hourly_cost" {
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
					t.Errorf("llm_total_hourly_cost allocation want %f got %f", ic.AllocationTotalCost, val)
				}
			case "usage":
				if !floatEq(val, ic.UsageTotalCost) {
					t.Errorf("llm_total_hourly_cost usage want %f got %f", ic.UsageTotalCost, val)
				}
			}
		}
	}
}

func TestExporter_DimensionCost(t *testing.T) {
	e, reg := newTestExporter(t)
	now := time.Now()
	mc := sampleMetric(AllocationMethodComputeTime)

	dimA := &InferenceCost{
		Properties: InferenceCostProperties{
			ModelName:  "meta-llama/Llama-3.1-8B",
			Namespace:  "llm-prod",
			TenantID:   "tenant-a",
			WorkloadID: "w1",
			UserID:     "", // Empty user_id
		},
		AllocationTotalCost: 2.5,
		UsageTotalCost:      0.6,
		PromptTokens:        500000,
		GenerationTokens:    100000,
		TotalTokens:         600000,
	}
	dimA.Window.Start = now.Add(-1 * time.Hour)
	dimA.Window.End = now

	dimRem := &InferenceCost{
		Properties: InferenceCostProperties{
			ModelName:  "meta-llama/Llama-3.1-8B",
			Namespace:  "llm-prod",
			TenantID:   "__unmetered__",
			WorkloadID: "",
			UserID:     "",
		},
		AllocationTotalCost: 1.5,
		UsageTotalCost:      0.4,
		PromptTokens:        300000,
		GenerationTokens:    100000,
		TotalTokens:         400000,
	}
	dimRem.Window.Start = now.Add(-1 * time.Hour)
	dimRem.Window.End = now

	e.Export([]*InferenceCost{mc}, []*InferenceCost{dimA, dimRem})

	mfs, _ := reg.Gather()
	foundTenantA := false
	foundRem := false
	for _, mf := range mfs {
		if mf.GetName() != "llm_dimension_hourly_cost" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := make(map[string]string)
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["tenant_id"] == "tenant-a" && labels["cost_basis"] == "allocation" {
				foundTenantA = true
				if labels["user_id"] != "" {
					t.Errorf("expected empty user_id label, got %q", labels["user_id"])
				}
				if !floatEq(m.GetGauge().GetValue(), 2.5) {
					t.Errorf("tenant-a hourly cost want 2.5 got %f", m.GetGauge().GetValue())
				}
			}
			if labels["tenant_id"] == "__unmetered__" && labels["cost_basis"] == "allocation" {
				foundRem = true
				if !floatEq(m.GetGauge().GetValue(), 1.5) {
					t.Errorf("remainder hourly cost want 1.5 got %f", m.GetGauge().GetValue())
				}
			}
		}
	}

	if !foundTenantA {
		t.Errorf("llm_dimension_hourly_cost for tenant-a not found")
	}
	if !foundRem {
		t.Errorf("llm_dimension_hourly_cost for __unmetered__ not found")
	}
}

func TestExporter_UnmatchedKeys(t *testing.T) {
	e, reg := newTestExporter(t)
	e.ExportUnmatchedKeys(map[string]float64{
		"litellm-gateway": 5.0,
	})

	mfs, _ := reg.Gather()
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "llm_dimension_unmatched_keys" {
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "producer" && lp.GetValue() == "litellm-gateway" {
						found = true
						if !floatEq(m.GetGauge().GetValue(), 5.0) {
							t.Errorf("unmatched keys count want 5.0 got %f", m.GetGauge().GetValue())
						}
					}
				}
			}
		}
	}
	if !found {
		t.Errorf("llm_dimension_unmatched_keys for litellm-gateway not found")
	}
}

func TestExporter_CoverageAndEstimatedShare(t *testing.T) {
	e, reg := newTestExporter(t)
	now := time.Now()
	mc := sampleMetric(AllocationMethodComputeTime)
	mc.PromptTokens = 1000000
	mc.GenerationTokens = 200000

	dimA := &InferenceCost{
		Properties: InferenceCostProperties{
			ModelName:  "meta-llama/Llama-3.1-8B",
			Namespace:  "llm-prod",
			TenantID:   "tenant-a",
			WorkloadID: "w1",
		},
		PromptTokens:              800000,
		GenerationTokens:          160000,
		TotalTokens:               960000,
		EstimatedPromptTokens:     80000,
		EstimatedCompletionTokens: 16000,
	}
	dimA.Window.Start = now.Add(-1 * time.Hour)
	dimA.Window.End = now

	e.Export([]*InferenceCost{mc}, []*InferenceCost{dimA})

	mfs, _ := reg.Gather()
	var foundInputCov, foundOutputCov, foundIdentCov, foundEstShare bool
	for _, mf := range mfs {
		switch mf.GetName() {
		case "llm_dimension_token_coverage":
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "direction" && lp.GetValue() == "input" {
						foundInputCov = true
						if !floatEq(m.GetGauge().GetValue(), 0.8) {
							t.Errorf("input coverage want 0.8 got %f", m.GetGauge().GetValue())
						}
					} else if lp.GetName() == "direction" && lp.GetValue() == "output" {
						foundOutputCov = true
						if !floatEq(m.GetGauge().GetValue(), 0.8) {
							t.Errorf("output coverage want 0.8 got %f", m.GetGauge().GetValue())
						}
					}
				}
			}
		case "llm_dimension_identity_coverage":
			foundIdentCov = true
			val := mf.GetMetric()[0].GetGauge().GetValue()
			if !floatEq(val, 1.0) {
				t.Errorf("identity coverage want 1.0 got %f", val)
			}
		case "llm_dimension_estimated_token_share":
			foundEstShare = true
			val := mf.GetMetric()[0].GetGauge().GetValue()
			// (80k + 16k) / 960k = 96k / 960k = 0.1
			if !floatEq(val, 0.1) {
				t.Errorf("estimated token share want 0.1 got %f", val)
			}
		}
	}

	if !foundInputCov || !foundOutputCov {
		t.Errorf("missing token coverage metrics: input=%v output=%v", foundInputCov, foundOutputCov)
	}
	if !foundIdentCov {
		t.Errorf("missing identity coverage metric")
	}
	if !foundEstShare {
		t.Errorf("missing estimated token share metric")
	}
}
