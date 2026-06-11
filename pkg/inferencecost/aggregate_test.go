package inferencecost

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// makeWindow is a test helper.
func makeWindow(start, end time.Time) opencost.Window {
	return opencost.NewClosedWindow(start, end)
}

// makeICR builds a minimal InferenceCostResponse for aggregation tests.
func makeICR(modelName, ns string, totalCost, promptTokens, genTokens, inputCost, outputCost float64) *InferenceCostResponse {
	totalTokens := promptTokens + genTokens
	var cpmt, icpmt, ocpmt float64
	if totalTokens > 0 {
		cpmt = totalCost / totalTokens * 1_000_000
	}
	if promptTokens > 0 {
		icpmt = inputCost / promptTokens * 1_000_000
	}
	if genTokens > 0 {
		ocpmt = outputCost / genTokens * 1_000_000
	}
	return &InferenceCostResponse{
		Properties: InferenceCostAPIProperties{
			ModelName: modelName,
			Namespace: ns,
		},
		CostBasis:                  CostBasisAllocation,
		TotalCost:                  totalCost,
		PromptTokens:               promptTokens,
		GenerationTokens:           genTokens,
		TotalTokens:                totalTokens,
		CostPerMillionTokens:       cpmt,
		InputCost:                  inputCost,
		OutputCost:                 outputCost,
		InputCostPerMillionTokens:  icpmt,
		OutputCostPerMillionTokens: ocpmt,
	}
}

// --- aggKey ---

func TestAggKey_NoAggregation(t *testing.T) {
	props := InferenceCostAPIProperties{ModelName: "llama", Namespace: "prod"}
	key, err := aggKey(props, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "llama:prod" {
		t.Errorf("key = %q, want %q", key, "llama:prod")
	}
}

func TestAggKey_NoAggregation_SlashInModelName(t *testing.T) {
	// Model names like "org/model" must not produce an ambiguous key.
	// ":" is the separator because it cannot appear in K8s label values or namespaces.
	props := InferenceCostAPIProperties{ModelName: "Qwen/Qwen3-VL-2B-Instruct", Namespace: "test-epd-ec"}
	key, err := aggKey(props, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "Qwen/Qwen3-VL-2B-Instruct:test-epd-ec" {
		t.Errorf("key = %q, want %q", key, "Qwen/Qwen3-VL-2B-Instruct:test-epd-ec")
	}
}

func TestAggKey_ByModelName(t *testing.T) {
	props := InferenceCostAPIProperties{ModelName: "llama", Namespace: "prod"}
	key, err := aggKey(props, []string{"model_name"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "llama" {
		t.Errorf("key = %q, want %q", key, "llama")
	}
}

func TestAggKey_MultiDim(t *testing.T) {
	props := InferenceCostAPIProperties{ModelName: "llama", Namespace: "prod"}
	key, err := aggKey(props, []string{"model_name", "namespace"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "llama/prod" {
		t.Errorf("key = %q, want %q", key, "llama/prod")
	}
}

func TestAggKey_UnsupportedDimension(t *testing.T) {
	props := InferenceCostAPIProperties{ModelName: "llama", Namespace: "prod"}
	_, err := aggKey(props, []string{"product"})
	if err == nil {
		t.Fatal("expected error for unsupported dimension, got nil")
	}
}

func TestAggKey_EmptyValueFallsBackToUnallocated(t *testing.T) {
	props := InferenceCostAPIProperties{ModelName: "llama"} // Namespace empty
	key, err := aggKey(props, []string{"namespace"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != opencost.UnallocatedSuffix {
		t.Errorf("key = %q, want %q (unallocated suffix)", key, opencost.UnallocatedSuffix)
	}
}

// --- aggregate ---

func TestAggregate_ByModelName_SumsTokensAndRecomputesRates(t *testing.T) {
	now := time.Now().UTC()
	win := makeWindow(now.Add(-time.Hour), now)

	set := newInferenceCostSet(win)
	// Two entries for the same model, different namespaces.
	set.InferenceCosts["llama/ns1"] = makeICR("llama", "ns1", 100, 1_000_000, 500_000, 70, 30)
	set.InferenceCosts["llama/ns2"] = makeICR("llama", "ns2", 200, 2_000_000, 1_000_000, 140, 60)

	if err := set.aggregate([]string{"model_name"}); err != nil {
		t.Fatalf("aggregate error: %v", err)
	}

	if len(set.InferenceCosts) != 1 {
		t.Fatalf("expected 1 entry after aggregation, got %d", len(set.InferenceCosts))
	}

	agg := set.InferenceCosts["llama"]
	if agg == nil {
		t.Fatal("expected key 'llama', not found")
	}
	if agg.TotalCost != 300 {
		t.Errorf("TotalCost = %.2f, want 300", agg.TotalCost)
	}
	if agg.PromptTokens != 3_000_000 {
		t.Errorf("PromptTokens = %.0f, want 3_000_000", agg.PromptTokens)
	}
	if agg.TotalTokens != 4_500_000 {
		t.Errorf("TotalTokens = %.0f, want 4_500_000", agg.TotalTokens)
	}

	// Rate must be recomputed from sums, not averaged.
	wantCPMT := 300.0 / 4_500_000 * 1_000_000
	if !floatEq(agg.CostPerMillionTokens, wantCPMT) {
		t.Errorf("CostPerMillionTokens = %.4f, want %.4f", agg.CostPerMillionTokens, wantCPMT)
	}
}

func TestAggregate_NoAggregation_DoesNothing(t *testing.T) {
	now := time.Now().UTC()
	win := makeWindow(now.Add(-time.Hour), now)

	set := newInferenceCostSet(win)
	set.InferenceCosts["llama/prod"] = makeICR("llama", "prod", 100, 1_000_000, 500_000, 70, 30)
	set.InferenceCosts["mistral/prod"] = makeICR("mistral", "prod", 50, 500_000, 250_000, 35, 15)

	if err := set.aggregate(nil); err != nil {
		t.Fatalf("aggregate error: %v", err)
	}
	if len(set.InferenceCosts) != 2 {
		t.Errorf("expected 2 entries after no-op aggregation, got %d", len(set.InferenceCosts))
	}
}

// --- accumulate ---

func TestAccumulate_EmptySlice(t *testing.T) {
	out := accumulate(nil)
	if out == nil {
		t.Fatal("expected non-nil result for empty slice")
	}
	if len(out.InferenceCosts) != 0 {
		t.Errorf("expected 0 entries, got %d", len(out.InferenceCosts))
	}
}

func TestAccumulate_SumsAcrossWindows(t *testing.T) {
	now := time.Now().UTC()
	win1 := makeWindow(now.Add(-2*time.Hour), now.Add(-1*time.Hour))
	win2 := makeWindow(now.Add(-1*time.Hour), now)

	set1 := newInferenceCostSet(win1)
	set1.InferenceCosts["llama/prod"] = makeICR("llama", "prod", 100, 1_000_000, 500_000, 70, 30)

	set2 := newInferenceCostSet(win2)
	set2.InferenceCosts["llama/prod"] = makeICR("llama", "prod", 120, 1_200_000, 600_000, 84, 36)

	out := accumulate([]*InferenceCostSet{set1, set2})

	if len(out.InferenceCosts) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(out.InferenceCosts))
	}
	acc := out.InferenceCosts["llama/prod"]
	if acc.TotalCost != 220 {
		t.Errorf("TotalCost = %.2f, want 220", acc.TotalCost)
	}
	if acc.PromptTokens != 2_200_000 {
		t.Errorf("PromptTokens = %.0f, want 2_200_000", acc.PromptTokens)
	}
	// Combined window should span from win1.Start to win2.End.
	if out.Window.Start() == nil || out.Window.End() == nil {
		t.Fatal("expected combined window to be non-nil")
	}
}

func TestAccumulate_MultipleModels(t *testing.T) {
	now := time.Now().UTC()
	win1 := makeWindow(now.Add(-2*time.Hour), now.Add(-1*time.Hour))
	win2 := makeWindow(now.Add(-1*time.Hour), now)

	set1 := newInferenceCostSet(win1)
	set1.InferenceCosts["llama/prod"] = makeICR("llama", "prod", 100, 1_000_000, 500_000, 70, 30)
	set1.InferenceCosts["mistral/prod"] = makeICR("mistral", "prod", 50, 500_000, 250_000, 35, 15)

	set2 := newInferenceCostSet(win2)
	set2.InferenceCosts["llama/prod"] = makeICR("llama", "prod", 120, 1_200_000, 600_000, 84, 36)
	// mistral absent in set2

	out := accumulate([]*InferenceCostSet{set1, set2})

	if len(out.InferenceCosts) != 2 {
		t.Errorf("expected 2 entries, got %d", len(out.InferenceCosts))
	}
	llama := out.InferenceCosts["llama/prod"]
	if llama == nil || llama.TotalCost != 220 {
		t.Errorf("llama TotalCost = %.2f, want 220", llama.TotalCost)
	}
	mistral := out.InferenceCosts["mistral/prod"]
	if mistral == nil || mistral.TotalCost != 50 {
		t.Errorf("mistral TotalCost = %.2f, want 50", mistral.TotalCost)
	}
}

// --- parseFilter ---

func TestParseFilter_Empty(t *testing.T) {
	specs, err := parseFilter("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(specs) != 0 {
		t.Errorf("expected 0 specs, got %d", len(specs))
	}
}

func TestParseFilter_SingleTerm(t *testing.T) {
	specs, err := parseFilter(`namespace:"llm-prod"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}
	if specs[0].property != "namespace" || specs[0].value != "llm-prod" {
		t.Errorf("spec = %+v, want {namespace, llm-prod}", specs[0])
	}
}

func TestParseFilter_MultiTermAnd(t *testing.T) {
	specs, err := parseFilter(`namespace:"llm-prod"+model_name:"llama"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(specs))
	}
}

func TestParseFilter_UnsupportedProperty(t *testing.T) {
	_, err := parseFilter(`product:"team-a"`)
	if err == nil {
		t.Fatal("expected error for unsupported property, got nil")
	}
}

func TestParseFilter_MissingColon(t *testing.T) {
	_, err := parseFilter(`namespacellm-prod`)
	if err == nil {
		t.Fatal("expected error for missing colon, got nil")
	}
}

// --- matchesFilter ---

func TestMatchesFilter_NoSpecs(t *testing.T) {
	ic := &InferenceCostResponse{Properties: InferenceCostAPIProperties{Namespace: "prod"}}
	if !matchesFilter(ic, nil) {
		t.Error("expected matchesFilter to return true for empty specs")
	}
}

func TestMatchesFilter_Match(t *testing.T) {
	ic := &InferenceCostResponse{Properties: InferenceCostAPIProperties{Namespace: "prod"}}
	specs := []filterSpec{{property: "namespace", value: "prod"}}
	if !matchesFilter(ic, specs) {
		t.Error("expected matchesFilter to return true for matching spec")
	}
}

func TestMatchesFilter_NoMatch(t *testing.T) {
	ic := &InferenceCostResponse{Properties: InferenceCostAPIProperties{Namespace: "prod"}}
	specs := []filterSpec{{property: "namespace", value: "staging"}}
	if matchesFilter(ic, specs) {
		t.Error("expected matchesFilter to return false for non-matching spec")
	}
}
