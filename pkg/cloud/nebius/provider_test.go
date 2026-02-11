package nebius

import (
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
)

func TestParsePreset(t *testing.T) {
	tests := []struct {
		name     string
		preset   string
		wantGPU  int
		wantVCPU int
		wantRAM  int
	}{
		{
			name:     "GPU preset",
			preset:   "1gpu-16vcpu-200gb",
			wantGPU:  1,
			wantVCPU: 16,
			wantRAM:  200,
		},
		{
			name:     "8 GPU preset",
			preset:   "8gpu-128vcpu-1600gb",
			wantGPU:  8,
			wantVCPU: 128,
			wantRAM:  1600,
		},
		{
			name:     "CPU only preset",
			preset:   "16vcpu-64gb",
			wantGPU:  0,
			wantVCPU: 16,
			wantRAM:  64,
		},
		{
			name:     "small CPU preset",
			preset:   "2vcpu-8gb",
			wantGPU:  0,
			wantVCPU: 2,
			wantRAM:  8,
		},
		{
			name:     "uppercase preset",
			preset:   "1GPU-16VCPU-200GB",
			wantGPU:  1,
			wantVCPU: 16,
			wantRAM:  200,
		},
		{
			name:     "unknown format",
			preset:   "custom-instance",
			wantGPU:  0,
			wantVCPU: 0,
			wantRAM:  0,
		},
		{
			name:     "empty string",
			preset:   "",
			wantGPU:  0,
			wantVCPU: 0,
			wantRAM:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gpu, vcpu, ram := parsePreset(tt.preset)
			if gpu != tt.wantGPU {
				t.Errorf("GPU count: got %d, want %d", gpu, tt.wantGPU)
			}
			if vcpu != tt.wantVCPU {
				t.Errorf("vCPU count: got %d, want %d", vcpu, tt.wantVCPU)
			}
			if ram != tt.wantRAM {
				t.Errorf("RAM GB: got %d, want %d", ram, tt.wantRAM)
			}
		})
	}
}

func TestNebiusKey_Features(t *testing.T) {
	key := &nebiusKey{
		Labels: map[string]string{
			"node.kubernetes.io/instance-type":       "1gpu-16vcpu-200gb",
			"topology.kubernetes.io/zone":            "eu-west1-a",
			"failure-domain.beta.kubernetes.io/zone": "eu-west1-a",
		},
	}

	features := key.Features()
	if features != "eu-west1-a,1gpu-16vcpu-200gb" {
		t.Errorf("Features(): got %q, want %q", features, "eu-west1-a,1gpu-16vcpu-200gb")
	}
}

func TestNebiusKey_GPUCount(t *testing.T) {
	tests := []struct {
		name         string
		instanceType string
		want         int
	}{
		{"GPU instance", "1gpu-16vcpu-200gb", 1},
		{"8 GPU instance", "8gpu-128vcpu-1600gb", 8},
		{"CPU-only instance", "16vcpu-64gb", 0},
		{"Unknown instance", "custom-type", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := &nebiusKey{
				Labels: map[string]string{
					"node.kubernetes.io/instance-type": tt.instanceType,
				},
			}
			if got := key.GPUCount(); got != tt.want {
				t.Errorf("GPUCount(): got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestNebiusKey_GPUType(t *testing.T) {
	tests := []struct {
		name         string
		instanceType string
		labels       map[string]string
		want         string
	}{
		{
			name:         "H100 from platform label",
			instanceType: "1gpu-16vcpu-200gb",
			labels: map[string]string{
				"node.kubernetes.io/instance-type": "1gpu-16vcpu-200gb",
				"nebius.com/platform":              "gpu-h100-sxm",
			},
			want: "H100",
		},
		{
			name:         "H200 from platform label",
			instanceType: "8gpu-128vcpu-1600gb",
			labels: map[string]string{
				"node.kubernetes.io/instance-type": "8gpu-128vcpu-1600gb",
				"nebius.com/platform":              "gpu-h200-sxm",
			},
			want: "H200",
		},
		{
			name:         "L40S from instance type containing platform",
			instanceType: "gpu-l40s-pcie",
			labels: map[string]string{
				"node.kubernetes.io/instance-type": "gpu-l40s-pcie",
			},
			want: "L40S",
		},
		{
			name:         "CPU-only instance",
			instanceType: "16vcpu-64gb",
			labels: map[string]string{
				"node.kubernetes.io/instance-type": "16vcpu-64gb",
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := &nebiusKey{
				Labels: tt.labels,
			}
			if got := key.GPUType(); got != tt.want {
				t.Errorf("GPUType(): got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNebiusPVKey(t *testing.T) {
	key := &nebiusPVKey{
		StorageClassName: "network-ssd",
		Zone:             "eu-west1-a",
	}

	if key.GetStorageClass() != "network-ssd" {
		t.Errorf("GetStorageClass(): got %q, want %q", key.GetStorageClass(), "network-ssd")
	}
	if key.Features() != "eu-west1-a" {
		t.Errorf("Features(): got %q, want %q", key.Features(), "eu-west1-a")
	}
	if key.ID() != "" {
		t.Errorf("ID(): got %q, want empty string", key.ID())
	}
}

func TestNetworkPricing(t *testing.T) {
	n := &Nebius{}
	pricing, err := n.NetworkPricing()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pricing.ZoneNetworkEgressCost != 0 {
		t.Errorf("ZoneNetworkEgressCost: got %f, want 0", pricing.ZoneNetworkEgressCost)
	}
	if pricing.RegionNetworkEgressCost != 0 {
		t.Errorf("RegionNetworkEgressCost: got %f, want 0", pricing.RegionNetworkEgressCost)
	}
	if pricing.InternetNetworkEgressCost != 0 {
		t.Errorf("InternetNetworkEgressCost: got %f, want 0", pricing.InternetNetworkEgressCost)
	}
}

func TestClusterManagementPricing(t *testing.T) {
	n := &Nebius{}
	_, cost, err := n.ClusterManagementPricing()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != 0.0 {
		t.Errorf("cluster management cost: got %f, want 0", cost)
	}
}

func TestLoadBalancerPricing(t *testing.T) {
	n := &Nebius{}
	lb, err := n.LoadBalancerPricing()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lb.Cost <= 0 {
		t.Errorf("LB cost should be positive, got %f", lb.Cost)
	}
}

func TestPricingSourceStatus(t *testing.T) {
	n := &Nebius{}
	status := n.PricingSourceStatus()
	if status == nil {
		t.Fatal("expected non-nil status")
	}
	source, ok := status[NebiusConfigPricing]
	if !ok {
		t.Fatalf("expected %q in pricing source status", NebiusConfigPricing)
	}
	if !source.Enabled {
		t.Error("expected pricing source to be enabled")
	}
	if !source.Available {
		t.Error("expected pricing source to be available")
	}
}

func TestRegions(t *testing.T) {
	n := &Nebius{}
	regions := n.Regions()
	if len(regions) == 0 {
		t.Error("expected at least one region")
	}
}

func TestCombinedDiscountForNode(t *testing.T) {
	n := &Nebius{}
	discount := n.CombinedDiscountForNode("1gpu-16vcpu-200gb", false, 0.1, 0.2)
	expected := 1.0 - ((1.0 - 0.1) * (1.0 - 0.2))
	if discount != expected {
		t.Errorf("CombinedDiscountForNode: got %f, want %f", discount, expected)
	}
}

func TestGPUPlatformModels(t *testing.T) {
	tests := []struct {
		platform string
		model    string
	}{
		{"gpu-h100-sxm", "H100"},
		{"gpu-h200-sxm", "H200"},
		{"gpu-l40s-pcie", "L40S"},
		{"gpu-b200-sxm", "B200"},
		{"gpu-b200-sxm-a", "B200"},
		{"gpu-b300-sxm", "B300"},
	}

	for _, tt := range tests {
		if got, ok := gpuPlatformModels[tt.platform]; !ok || got != tt.model {
			t.Errorf("gpuPlatformModels[%q]: got %q (ok=%v), want %q", tt.platform, got, ok, tt.model)
		}
	}
}

func TestGetAddresses(t *testing.T) {
	n := &Nebius{}
	data, err := n.GetAddresses()
	if data != nil || err != nil {
		t.Errorf("GetAddresses: expected (nil, nil), got (%v, %v)", data, err)
	}
}

func TestGetDisks(t *testing.T) {
	n := &Nebius{}
	data, err := n.GetDisks()
	if data != nil || err != nil {
		t.Errorf("GetDisks: expected (nil, nil), got (%v, %v)", data, err)
	}
}

func TestGetOrphanedResources(t *testing.T) {
	n := &Nebius{}
	_, err := n.GetOrphanedResources()
	if err == nil {
		t.Error("GetOrphanedResources: expected error, got nil")
	}
}

// Verify that Nebius implements the Provider interface at compile time.
var _ models.Provider = (*Nebius)(nil)
