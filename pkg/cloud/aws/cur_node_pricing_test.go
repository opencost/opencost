package aws

import (
	"strconv"
	"strings"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
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
//
// VCPUCost is $/vCPU/hr and RAMCost is $/GiB/hr (per-unit, NOT node totals).
// The reconstructed node total VCPUCost*vCPUs + RAMCost*GiB + GPUCost*GPUs must
// equal the effective rate.
// ---------------------------------------------------------------------------

func parseFloat(t *testing.T, s string) float64 {
	t.Helper()
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("could not parse float %q: %v", s, err)
	}
	return f
}

const gib int64 = 1 << 30

func nodeTotal(t *testing.T, node *models.Node) float64 {
	t.Helper()
	vcpus := parseFloat(t, node.VCPU)
	ramGiB := parseFloat(t, node.RAMBytes) / float64(gib)
	gpus := parseFloat(t, node.GPU)
	return parseFloat(t, node.VCPUCost)*vcpus +
		parseFloat(t, node.RAMCost)*ramGiB +
		parseFloat(t, node.GPUCost)*gpus
}

func TestApplyEffectiveRate_PerUnitAndRatioPreserved(t *testing.T) {
	// 4 vCPU, 16 GiB. Per-unit: $0.10/vCPU/hr, $0.00625/GiB/hr
	// => node totals: CPU $0.40, RAM $0.10 => 80%:20% ratio.
	node := &models.Node{
		VCPU:     "4",
		RAMBytes: strconv.FormatInt(16*gib, 10),
		VCPUCost: "0.10",
		RAMCost:  "0.00625",
		GPUCost:  "0",
		GPU:      "0",
		Cost:     "0.50",
	}
	effectiveRate := 0.30 // SP coverage cut the cost

	if err := applyEffectiveRate(node, effectiveRate); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Reconstructed node total must equal the effective rate.
	if diff := nodeTotal(t, node) - effectiveRate; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("reconstructed total = %f, want %f", nodeTotal(t, node), effectiveRate)
	}
	if diff := parseFloat(t, node.Cost) - effectiveRate; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("Cost = %s, want %f", node.Cost, effectiveRate)
	}

	// Ratio preserved: CPU carries 80% of the rate => per-unit = 0.30*0.8/4
	wantCPUUnit := effectiveRate * 0.8 / 4
	wantRAMUnit := effectiveRate * 0.2 / 16
	if diff := parseFloat(t, node.VCPUCost) - wantCPUUnit; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("VCPUCost = %s, want %f", node.VCPUCost, wantCPUUnit)
	}
	if diff := parseFloat(t, node.RAMCost) - wantRAMUnit; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("RAMCost = %s, want %f", node.RAMCost, wantRAMUnit)
	}
}

func TestApplyEffectiveRate_GPUFixedCostPreserved(t *testing.T) {
	// 1 GPU @ $1/hr fixed. CPU/RAM remainder = $0.20, split 80/20.
	node := &models.Node{
		VCPU:     "4",
		RAMBytes: strconv.FormatInt(16*gib, 10),
		VCPUCost: "0.10",
		RAMCost:  "0.00625",
		GPUCost:  "1.00",
		GPU:      "1",
		Cost:     "1.50",
	}
	effectiveRate := 1.20

	if err := applyEffectiveRate(node, effectiveRate); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if diff := nodeTotal(t, node) - effectiveRate; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("reconstructed total = %f, want %f", nodeTotal(t, node), effectiveRate)
	}

	remainder := effectiveRate - 1.00
	wantCPUUnit := remainder * 0.8 / 4
	wantRAMUnit := remainder * 0.2 / 16
	if diff := parseFloat(t, node.VCPUCost) - wantCPUUnit; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("VCPUCost = %s, want %f", node.VCPUCost, wantCPUUnit)
	}
	if diff := parseFloat(t, node.RAMCost) - wantRAMUnit; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("RAMCost = %s, want %f", node.RAMCost, wantRAMUnit)
	}
}

func TestApplyEffectiveRate_ZeroExistingCosts(t *testing.T) {
	// No existing per-unit prices => 50/50 split across CPU and RAM.
	node := &models.Node{
		VCPU:     "4",
		RAMBytes: strconv.FormatInt(16*gib, 10),
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

	if diff := nodeTotal(t, node) - effectiveRate; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("reconstructed total = %f, want %f", nodeTotal(t, node), effectiveRate)
	}
	wantCPUUnit := effectiveRate * 0.5 / 4
	if diff := parseFloat(t, node.VCPUCost) - wantCPUUnit; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("VCPUCost = %s, want %f", node.VCPUCost, wantCPUUnit)
	}
}

func TestApplyEffectiveRate_RAMFromGiBFallback(t *testing.T) {
	// RAMBytes unset; RAM carries GiB directly.
	node := &models.Node{
		VCPU:     "2",
		RAM:      "8",
		VCPUCost: "0.05",
		RAMCost:  "0.0125", // totals: 0.10 + 0.10 => 50/50
		Cost:     "0.20",
	}
	effectiveRate := 0.10

	if err := applyEffectiveRate(node, effectiveRate); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := parseFloat(t, node.VCPUCost)*2 + parseFloat(t, node.RAMCost)*8
	if diff := got - effectiveRate; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("reconstructed total = %f, want %f", got, effectiveRate)
	}
}

func TestApplyEffectiveRate_NoCapacityErrors(t *testing.T) {
	node := &models.Node{
		VCPUCost: "0.40",
		RAMCost:  "0.10",
	}
	if err := applyEffectiveRate(node, 0.30); err == nil {
		t.Error("expected error for node without parsable vCPU/RAM capacity")
	}
}

// ---------------------------------------------------------------------------
// curHoursDenominator — granularity selection
// ---------------------------------------------------------------------------

func TestCURHoursDenominator(t *testing.T) {
	if got := curHoursDenominator("hourly"); got != "count(distinct line_item_usage_start_date)" {
		t.Errorf("hourly denominator = %q", got)
	}
	if got := curHoursDenominator("daily"); got != "count(distinct line_item_usage_start_date) * 24" {
		t.Errorf("daily denominator = %q", got)
	}
	if got := curHoursDenominator("auto"); !strings.Contains(got, "date_diff") {
		t.Errorf("auto denominator should derive hours via date_diff, got %q", got)
	}
	if got := curHoursDenominator("nonsense"); !strings.Contains(got, "date_diff") {
		t.Errorf("unknown mode should fall back to auto, got %q", got)
	}
}

func TestGetCURNodePricingGranularity(t *testing.T) {
	t.Setenv(env.CURNodePricingGranularityEnvVar, "daily")
	if got := env.GetCURNodePricingGranularity(); got != "daily" {
		t.Errorf("granularity = %q, want daily", got)
	}
	t.Setenv(env.CURNodePricingGranularityEnvVar, "bogus")
	if got := env.GetCURNodePricingGranularity(); got != "auto" {
		t.Errorf("invalid granularity should fall back to auto, got %q", got)
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
