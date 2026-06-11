package stackit

import (
	"testing"
)

func newTestProvider(flavors map[string]*pimFlavorPricing, storage map[string]*pimStoragePricing) *STACKIT {
	return &STACKIT{
		ClusterRegion: "eu01",
		pimFlavors:    flavors,
		pimStorage:    storage,
	}
}

func TestNodePricing(t *testing.T) {
	s := newTestProvider(map[string]*pimFlavorPricing{
		"g2i.4": {HourlyCost: "0.201", VCPU: 4, RAMGB: 16.0},
	}, nil)

	key := &stackitKey{Labels: map[string]string{
		"node.kubernetes.io/instance-type":        "g2i.4",
		"topology.kubernetes.io/zone":             "eu01-3",
	}}

	node, _, err := s.NodePricing(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.Cost != "0.201" {
		t.Errorf("Cost = %q, want %q", node.Cost, "0.201")
	}
	if node.VCPU != "4" {
		t.Errorf("VCPU = %q, want %q", node.VCPU, "4")
	}
	if node.RAM != "16" {
		t.Errorf("RAM = %q, want %q", node.RAM, "16")
	}
	if node.RAMBytes != "17179869184" {
		t.Errorf("RAMBytes = %q, want %q", node.RAMBytes, "17179869184")
	}
	if node.InstanceType != "g2i.4" {
		t.Errorf("InstanceType = %q, want %q", node.InstanceType, "g2i.4")
	}
	if node.Region != "eu01" {
		t.Errorf("Region = %q, want %q", node.Region, "eu01")
	}
}

func TestNodePricingUnknownType(t *testing.T) {
	s := newTestProvider(map[string]*pimFlavorPricing{}, nil)

	key := &stackitKey{Labels: map[string]string{
		"node.kubernetes.io/instance-type": "unknown.1",
	}}

	_, _, err := s.NodePricing(key)
	if err == nil {
		t.Error("expected error for unknown instance type")
	}
}

func TestNodePricingGPU(t *testing.T) {
	s := newTestProvider(map[string]*pimFlavorPricing{
		"n1.14d.g1": {HourlyCost: "3.50", VCPU: 14, RAMGB: 56.0, GPUCount: 1, GPUType: "NVIDIA A100"},
	}, nil)

	key := &stackitKey{Labels: map[string]string{
		"node.kubernetes.io/instance-type": "n1.14d.g1",
	}}

	node, _, err := s.NodePricing(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.GPU != "1" {
		t.Errorf("GPU = %q, want %q", node.GPU, "1")
	}
	if node.GPUName != "NVIDIA A100" {
		t.Errorf("GPUName = %q, want %q", node.GPUName, "NVIDIA A100")
	}
}

func TestGpuPricingPerGPU(t *testing.T) {
	s := newTestProvider(map[string]*pimFlavorPricing{
		"n1.28d.g2": {HourlyCost: "7.00", VCPU: 28, RAMGB: 112.0, GPUCount: 2, GPUType: "NVIDIA A100"},
	}, nil)

	labels := map[string]string{
		"node.kubernetes.io/instance-type": "n1.28d.g2",
	}

	cost, err := s.GpuPricing(labels)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != "3.5" {
		t.Errorf("per-GPU cost = %q, want %q", cost, "3.5")
	}
}

func TestGpuPricingNoGPU(t *testing.T) {
	s := newTestProvider(map[string]*pimFlavorPricing{
		"g2i.4": {HourlyCost: "0.201", VCPU: 4, RAMGB: 16.0, GPUCount: 0},
	}, nil)

	labels := map[string]string{
		"node.kubernetes.io/instance-type": "g2i.4",
	}

	cost, err := s.GpuPricing(labels)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != "" {
		t.Errorf("expected empty cost for non-GPU instance, got %q", cost)
	}
}

func TestGpuPricingUnknownInstance(t *testing.T) {
	s := newTestProvider(map[string]*pimFlavorPricing{}, nil)

	labels := map[string]string{
		"node.kubernetes.io/instance-type": "unknown.1",
	}

	cost, err := s.GpuPricing(labels)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != "" {
		t.Errorf("expected empty cost for unknown instance, got %q", cost)
	}
}

func TestPVPricingExactMatch(t *testing.T) {
	s := newTestProvider(nil, map[string]*pimStoragePricing{
		"storage_premium_perf2": {CostPerGBHr: "0.0005"},
		"default":              {CostPerGBHr: "0.0001"},
	})

	pvk := &stackitPVKey{StorageClassName: "storage_premium_perf2"}
	pv, err := s.PVPricing(pvk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pv.Cost != "0.0005" {
		t.Errorf("Cost = %q, want %q", pv.Cost, "0.0005")
	}
}

func TestPVPricingDefaultFallback(t *testing.T) {
	s := newTestProvider(nil, map[string]*pimStoragePricing{
		"default": {CostPerGBHr: "0.0001"},
	})

	pvk := &stackitPVKey{StorageClassName: "unknown-class"}
	pv, err := s.PVPricing(pvk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pv.Cost != "0.0001" {
		t.Errorf("Cost = %q, want default %q", pv.Cost, "0.0001")
	}
	if pv.Class != "unknown-class" {
		t.Errorf("Class = %q, want %q", pv.Class, "unknown-class")
	}
}

func TestPVPricingNoPricing(t *testing.T) {
	s := newTestProvider(nil, nil)

	pvk := &stackitPVKey{StorageClassName: "some-class"}
	pv, err := s.PVPricing(pvk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pv.Cost != "" {
		t.Errorf("expected empty cost when no pricing data, got %q", pv.Cost)
	}
}

func TestStackitKeyFeatures(t *testing.T) {
	key := &stackitKey{Labels: map[string]string{
		"node.kubernetes.io/instance-type":    "g2i.4",
		"topology.kubernetes.io/zone":         "eu01-3",
	}}

	features := key.Features()
	if features != "eu01-3,g2i.4" {
		t.Errorf("Features() = %q, want %q", features, "eu01-3,g2i.4")
	}
}
