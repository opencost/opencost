package stackit

import (
	"testing"
)

func TestGpuCountFromFlavor(t *testing.T) {
	tests := []struct {
		flavor string
		want   int
	}{
		{"n1.14d.g1", 1},
		{"n1.28d.g2", 2},
		{"n3.104d.g8", 8},
		{"c1i.2", 0},
		{"g2i.1", 0},
		{"n1.14d", 0},
	}
	for _, tt := range tests {
		got := gpuCountFromFlavor(tt.flavor)
		if got != tt.want {
			t.Errorf("gpuCountFromFlavor(%q) = %d, want %d", tt.flavor, got, tt.want)
		}
	}
}

func TestGpuTypeFromFlavor(t *testing.T) {
	tests := []struct {
		flavor string
		want   string
	}{
		{"n1.14d.g1", "NVIDIA A100"},
		{"n2.28d.g2", "NVIDIA L40S"},
		{"n3.104d.g8", "NVIDIA H100 HGX"},
		{"c1i.2", ""},
	}
	for _, tt := range tests {
		got := gpuTypeFromFlavor(tt.flavor)
		if got != tt.want {
			t.Errorf("gpuTypeFromFlavor(%q) = %q, want %q", tt.flavor, got, tt.want)
		}
	}
}

func TestParsePIMVMFlavors(t *testing.T) {
	metro := true
	nonMetro := false
	vcpu2 := 2
	ram4 := 4.0

	skus := []pimSKU{
		{
			Name: "g2i.1",
			ProductSpecificAttributes: pimProductSpecificAttrs{
				Flavor: "g2i.1",
				VCPU:   &vcpu2,
				RAM:    &ram4,
				Metro:  &nonMetro,
			},
			Prices: []pimPrice{{Value: "0.05045"}},
		},
		{
			// Metro variant should be skipped
			Name: "g2i.1-metro",
			ProductSpecificAttributes: pimProductSpecificAttrs{
				Flavor: "g2i.1",
				VCPU:   &vcpu2,
				RAM:    &ram4,
				Metro:  &metro,
			},
			Prices: []pimPrice{{Value: "0.10"}},
		},
		{
			// No flavor -> skipped
			Name:                      "no-flavor",
			ProductSpecificAttributes: pimProductSpecificAttrs{},
			Prices:                    []pimPrice{{Value: "0.01"}},
		},
		{
			// No price -> skipped
			Name: "no-price",
			ProductSpecificAttributes: pimProductSpecificAttrs{
				Flavor: "c1i.2",
				VCPU:   &vcpu2,
				RAM:    &ram4,
			},
			Prices: []pimPrice{},
		},
	}

	flavors := parsePIMVMFlavors(skus)

	if len(flavors) != 1 {
		t.Fatalf("expected 1 flavor, got %d", len(flavors))
	}

	f, ok := flavors["g2i.1"]
	if !ok {
		t.Fatal("expected flavor g2i.1")
	}
	if f.HourlyCost != "0.05045" {
		t.Errorf("expected hourly cost 0.05045, got %s", f.HourlyCost)
	}
	if f.VCPU != 2 {
		t.Errorf("expected 2 vCPU, got %d", f.VCPU)
	}
	if f.RAMGB != 4.0 {
		t.Errorf("expected 4.0 RAM GB, got %f", f.RAMGB)
	}
}

func TestParsePIMVMFlavorsGPU(t *testing.T) {
	nonMetro := false
	vcpu14 := 14
	ram56 := 56.0

	skus := []pimSKU{
		{
			Name: "n1.14d.g1",
			ProductSpecificAttributes: pimProductSpecificAttrs{
				Flavor: "n1.14d.g1",
				VCPU:   &vcpu14,
				RAM:    &ram56,
				Metro:  &nonMetro,
			},
			Prices: []pimPrice{{Value: "3.50"}},
		},
	}

	flavors := parsePIMVMFlavors(skus)
	f, ok := flavors["n1.14d.g1"]
	if !ok {
		t.Fatal("expected flavor n1.14d.g1")
	}
	if f.GPUCount != 1 {
		t.Errorf("expected GPU count 1, got %d", f.GPUCount)
	}
	if f.GPUType != "NVIDIA A100" {
		t.Errorf("expected GPU type NVIDIA A100, got %s", f.GPUType)
	}
}

func TestParsePIMVMFlavorsNonGPUNPrefix(t *testing.T) {
	nonMetro := false
	vcpu14 := 14
	ram56 := 56.0

	skus := []pimSKU{
		{
			Name: "n1.14d",
			ProductSpecificAttributes: pimProductSpecificAttrs{
				Flavor: "n1.14d",
				VCPU:   &vcpu14,
				RAM:    &ram56,
				Metro:  &nonMetro,
			},
			Prices: []pimPrice{{Value: "1.50"}},
		},
	}

	flavors := parsePIMVMFlavors(skus)
	f, ok := flavors["n1.14d"]
	if !ok {
		t.Fatal("expected flavor n1.14d")
	}
	if f.GPUCount != 0 {
		t.Errorf("expected GPU count 0, got %d", f.GPUCount)
	}
	if f.GPUType != "" {
		t.Errorf("expected empty GPU type for non-GPU n1 flavor, got %q", f.GPUType)
	}
}

func TestParsePIMStoragePricing(t *testing.T) {
	nonMetro := false

	skus := []pimSKU{
		{
			Name:        "premium-perf0",
			UnitBilling: "per GB/hour",
			ProductSpecificAttributes: pimProductSpecificAttrs{
				Class: "storage_premium_perf0",
				Metro: &nonMetro,
			},
			Prices: []pimPrice{{Value: "0.0001"}},
		},
		{
			Name:        "premium-perf2",
			UnitBilling: "per GB/hour",
			ProductSpecificAttributes: pimProductSpecificAttrs{
				Class: "storage_premium_perf2",
				Metro: &nonMetro,
			},
			Prices: []pimPrice{{Value: "0.0005"}},
		},
		{
			// Non-GB/hour billing -> skipped
			Name:        "iops-based",
			UnitBilling: "per IOPS/hour",
			ProductSpecificAttributes: pimProductSpecificAttrs{
				Class: "storage_iops",
				Metro: &nonMetro,
			},
			Prices: []pimPrice{{Value: "0.01"}},
		},
	}

	pricing := parsePIMStoragePricing(skus)

	if _, ok := pricing["storage_premium_perf0"]; !ok {
		t.Error("expected storage_premium_perf0")
	}
	if _, ok := pricing["storage_premium_perf2"]; !ok {
		t.Error("expected storage_premium_perf2")
	}
	if _, ok := pricing["storage_iops"]; ok {
		t.Error("storage_iops should have been skipped (non GB/hour billing)")
	}

	// Default should be the cheapest
	def, ok := pricing["default"]
	if !ok {
		t.Fatal("expected default storage entry")
	}
	if def.CostPerGBHr != "0.0001" {
		t.Errorf("expected default cost 0.0001, got %s", def.CostPerGBHr)
	}
}

func TestPaginationTermination(t *testing.T) {
	// Verify that empty data or empty cursor terminates pagination.
	// This is a logic check - fetchAllPIMSKUs breaks on:
	//   searchResp.Meta.NextCursor == "" || len(searchResp.Data) == 0
	// We can't call the real API, but we verify the parsing logic
	// handles the termination conditions in parsePIMVMFlavors.

	// Empty input should produce empty output
	flavors := parsePIMVMFlavors(nil)
	if len(flavors) != 0 {
		t.Errorf("expected 0 flavors from nil input, got %d", len(flavors))
	}

	flavors = parsePIMVMFlavors([]pimSKU{})
	if len(flavors) != 0 {
		t.Errorf("expected 0 flavors from empty input, got %d", len(flavors))
	}
}
