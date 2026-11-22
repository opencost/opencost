package provider

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
)

func TestCustomProviderNodePricingSpotUsageType(t *testing.T) {
	cp := &CustomProvider{
		SpotLabel:      "opencost.node-type",
		SpotLabelValue: "worker",
		Pricing: map[string]*NodePrice{
			"default":      {CPU: "0.01213", RAM: "0.00273"},
			"default,spot": {CPU: "0.32455", RAM: "0.08114"},
		},
	}

	tests := []struct {
		name     string
		labels   map[string]string
		wantCPU  string
		wantSpot bool
	}{
		{
			name:     "node with the spot label",
			labels:   map[string]string{"opencost.node-type": "worker"},
			wantCPU:  "0.32455",
			wantSpot: true,
		},
		{
			name:     "node with another spot label value",
			labels:   map[string]string{"opencost.node-type": "master"},
			wantCPU:  "0.01213",
			wantSpot: false,
		},
		{
			name:     "node without the spot label",
			labels:   map[string]string{},
			wantCPU:  "0.01213",
			wantSpot: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, _, err := cp.NodePricing(cp.GetKey(tt.labels, &clustercache.Node{Labels: tt.labels}))
			if err != nil {
				t.Fatalf("NodePricing: %v", err)
			}
			if node.VCPUCost != tt.wantCPU {
				t.Errorf("VCPUCost: got %s, want %s", node.VCPUCost, tt.wantCPU)
			}
			if node.IsSpot() != tt.wantSpot {
				t.Errorf("IsSpot: got %v, want %v (UsageType %q)", node.IsSpot(), tt.wantSpot, node.UsageType)
			}
		})
	}
}

func TestCustomProviderNodePricingSpotWithoutSpotPrices(t *testing.T) {
	cp := &CustomProvider{
		SpotLabel:      "opencost.node-type",
		SpotLabelValue: "worker",
		Pricing: map[string]*NodePrice{
			"default": {CPU: "0.01213", RAM: "0.00273"},
		},
	}

	labels := map[string]string{"opencost.node-type": "worker"}
	node, _, err := cp.NodePricing(cp.GetKey(labels, &clustercache.Node{Labels: labels}))
	if err != nil {
		t.Fatalf("NodePricing: %v", err)
	}
	if node.IsSpot() {
		t.Errorf("node priced at default rates should not be reported as spot")
	}
}
