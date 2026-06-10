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
	cfg := &Config{
		UsageCostShareSplit: UsageCostShareSplitNone, // Default for tests
	}
	e := NewExporter(cfg)
	for _, c := range []prometheus.Collector{
		e.totalCost,
		e.costPerMillionTokens,
		e.inputCostPerMillionTokens,
		e.outputCostPerMillionTokens,
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
		"llm_input_cost_per_million_tokens",
		"llm_output_cost_per_million_tokens",
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

// TestExporter_TwoCostBasisSeriesPerModel verifies that llm_total_cost and
// llm_cost_per_million_tokens each produce two series (usage + allocation).
func TestExporter_TwoCostBasisSeriesPerModel(t *testing.T) {
	e, reg := newTestExporter(t)
	e.Export([]*InferenceCost{sampleMetric(AllocationMethodComputeTime)})

	for _, metricName := range []string{"llm_total_cost", "llm_cost_per_million_tokens"} {
		count := testutil.CollectAndCount(e.totalCost)
		if metricName == "llm_cost_per_million_tokens" {
			count = testutil.CollectAndCount(e.costPerMillionTokens)
		}
		if count != 2 {
			t.Errorf("%s: expected 2 series (usage+allocation), got %d", metricName, count)
		}
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

// TestExporter_InputOutputHaveCostBasisAndAllocationMethod verifies that
// input/output metrics carry both cost_basis and allocation_method labels.
func TestExporter_InputOutputHaveCostBasisAndAllocationMethod(t *testing.T) {
	for _, method := range []AllocationMethod{
		AllocationMethodComputeTime,
		AllocationMethodComputeTimeUncorrected,
		AllocationMethodMultiplier,
	} {
		e, reg := newTestExporter(t)
		e.Export([]*InferenceCost{sampleMetric(method)})

		mfs, _ := reg.Gather()
		for _, mf := range mfs {
			name := mf.GetName()
			if name != "llm_input_cost_per_million_tokens" && name != "llm_output_cost_per_million_tokens" {
				continue
			}
			bases := make(map[string]bool)
			methods := make(map[string]bool)
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					switch lp.GetName() {
					case "cost_basis":
						bases[lp.GetValue()] = true
					case "allocation_method":
						methods[lp.GetValue()] = true
					}
				}
			}
			if !bases["usage"] || !bases["allocation"] {
				t.Errorf("%s method=%s: missing cost_basis label values, got %v", name, method, bases)
			}
			if !methods[string(method)] {
				t.Errorf("%s: expected allocation_method=%s, got %v", name, method, methods)
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

	// llm_cost_per_million_tokens
	_ = reg
}
