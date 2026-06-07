package aws

import (
	"strconv"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
)

// ---------------------------------------------------------------------------
// instanceIDFromProviderID
// ---------------------------------------------------------------------------

func TestInstanceIDFromProviderID(t *testing.T) {
	tests := []struct {
		name       string
		providerID string
		wantID     string
	}{
		{
			name:       "standard aws provider id",
			providerID: "aws:///us-east-2a/i-0fea4fd46592d050b",
			wantID:     "i-0fea4fd46592d050b",
		},
		{
			name:       "bare instance id",
			providerID: "i-0abc123def456",
			wantID:     "i-0abc123def456",
		},
		{
			name:       "empty string",
			providerID: "",
			wantID:     "",
		},
		{
			name:       "non-aws provider id",
			providerID: "gce://project/zone/instance",
			wantID:     "",
		},
		{
			name:       "aws provider id different region",
			providerID: "aws:///eu-west-1a/i-0deadbeef1234567",
			wantID:     "i-0deadbeef1234567",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := instanceIDFromProviderID(tc.providerID)
			if got != tc.wantID {
				t.Errorf("instanceIDFromProviderID(%q) = %q, want %q", tc.providerID, got, tc.wantID)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// applyEffectiveRate — node mutation math
// ---------------------------------------------------------------------------

func mustParseFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

func TestApplyEffectiveRate_RatioPreserved(t *testing.T) {
	// CPU:RAM = 0.40:0.10 => 80%:20% ratio
	node := &models.Node{
		VCPUCost: "0.40",
		RAMCost:  "0.10",
		GPUCost:  "0",
		GPU:      "0",
		Cost:     "0.50",
	}
	effectiveRate := 0.30 // e.g. SP coverage cut the cost

	if err := applyEffectiveRate(node, effectiveRate); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotCPU := mustParseFloat(node.VCPUCost)
	gotRAM := mustParseFloat(node.RAMCost)
	gotTotal := mustParseFloat(node.Cost)

	// Total must equal effectiveRate
	if diff := gotCPU + gotRAM - gotTotal; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("cpu(%f) + ram(%f) != cost(%f)", gotCPU, gotRAM, gotTotal)
	}
	if diff := gotTotal - effectiveRate; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("cost = %f, want %f", gotTotal, effectiveRate)
	}

	// Ratio: cpu should be ~80% of remainder (no GPU)
	wantCPU := effectiveRate * 0.8
	wantRAM := effectiveRate * 0.2
	if diff := gotCPU - wantCPU; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("VCPUCost = %f, want %f", gotCPU, wantCPU)
	}
	if diff := gotRAM - wantRAM; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("RAMCost = %f, want %f", gotRAM, wantRAM)
	}
}

func TestApplyEffectiveRate_GPUFixedCostPreserved(t *testing.T) {
	// 1 GPU @ $1/hr, CPU $0.40, RAM $0.10 => total was $1.50
	// New effective rate: $1.20. GPU stays at $1.00; remainder ($0.20) split 80/20
	node := &models.Node{
		VCPUCost: "0.40",
		RAMCost:  "0.10",
		GPUCost:  "1.00",
		GPU:      "1",
		Cost:     "1.50",
	}
	effectiveRate := 1.20

	if err := applyEffectiveRate(node, effectiveRate); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotCPU := mustParseFloat(node.VCPUCost)
	gotRAM := mustParseFloat(node.RAMCost)
	gotTotal := mustParseFloat(node.Cost)

	if diff := gotTotal - effectiveRate; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("cost = %f, want %f", gotTotal, effectiveRate)
	}

	// GPU cost (1.0) is removed from remainder => remainder = 0.20
	remainder := effectiveRate - 1.00
	wantCPU := remainder * 0.8
	wantRAM := remainder * 0.2
	if diff := gotCPU - wantCPU; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("VCPUCost = %f, want %f", gotCPU, wantCPU)
	}
	if diff := gotRAM - wantRAM; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("RAMCost = %f, want %f", gotRAM, wantRAM)
	}
}

func TestApplyEffectiveRate_ZeroExistingCosts(t *testing.T) {
	// When existing costs are 0, all goes to VCPUCost
	node := &models.Node{
		VCPUCost: "0",
		RAMCost:  "0",
		GPUCost:  "0",
		GPU:      "0",
		Cost:     "0",
	}
	effectiveRate := 0.25

	if err := applyEffectiveRate(node, effectiveRate); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotCPU := mustParseFloat(node.VCPUCost)
	gotRAM := mustParseFloat(node.RAMCost)
	gotTotal := mustParseFloat(node.Cost)

	if diff := gotTotal - effectiveRate; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("cost = %f, want %f", gotTotal, effectiveRate)
	}
	if diff := gotCPU - effectiveRate; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("VCPUCost = %f, want %f", gotCPU, effectiveRate)
	}
	if gotRAM != 0 {
		t.Errorf("RAMCost should be 0, got %f", gotRAM)
	}
}

func TestApplyEffectiveRate_UnparsableCosts(t *testing.T) {
	// When costs cannot be parsed, all goes to VCPUCost
	node := &models.Node{
		VCPUCost: "N/A",
		RAMCost:  "",
		GPUCost:  "0",
		GPU:      "0",
		Cost:     "N/A",
	}
	effectiveRate := 0.50

	if err := applyEffectiveRate(node, effectiveRate); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotTotal := mustParseFloat(node.Cost)
	if diff := gotTotal - effectiveRate; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("cost = %f, want %f", gotTotal, effectiveRate)
	}
}

// ---------------------------------------------------------------------------
// ApplyReservedInstancePricing — disabled flag no-op
// ---------------------------------------------------------------------------

func TestApplyReservedInstancePricing_DisabledNoOp(t *testing.T) {
	// CUR_NODE_PRICING_ENABLED is not set in test env => defaults to false.
	// The method must return without modifying nodes.
	a := &AWS{}
	original := "0.50"
	node := &models.Node{
		Cost:       original,
		VCPUCost:   "0.40",
		RAMCost:    "0.10",
		ProviderID: "aws:///us-east-2a/i-0fea4fd46592d050b",
	}
	nodes := map[string]*models.Node{"node1": node}

	a.ApplyReservedInstancePricing(nodes)

	if node.Cost != original {
		t.Errorf("Cost was modified when feature is disabled: got %s, want %s", node.Cost, original)
	}
}
