package gcp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
	"github.com/opencost/opencost/core/pkg/model/shared"
)

func TestGCPNormalizeFamily(t *testing.T) {
	cases := []struct{ in, want string }{
		{"N1Standard", "n1"},
		{"N2Standard", "n2"},
		{"N2DStandard", "n2d"},
		{"E2", "e2"},
		{"A2", "a2"},
		{"A3", "a3"},
		{"G2", "g2"},
		{"C2Standard", "c2"},
		{"C2DStandard", "c2d"},
		{"C3Standard", "c3"},
		{"C3DStandard", "c3d"},
		{"M1", "m1"},
		{"M2", "m2"},
		{"M3", "m3"},
		{"T2DStandard", "t2d"},
		{"T2AStandard", "t2a"},
		{"N4Standard", "n4"},
		{"H3Standard", "h3"},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := gcpNormalizeFamily(tc.in)
			if got != tc.want {
				t.Errorf("gcpNormalizeFamily(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestGCPUsageType(t *testing.T) {
	cases := []struct {
		in   string
		want shared.UsageType
	}{
		{"OnDemand", shared.UsageTypeOnDemand},
		{"Preemptible", shared.UsageTypeSpot},
		{"Commit1Yr", shared.UsageTypeEmpty},
		{"Commit3Yr", shared.UsageTypeEmpty},
		{"", shared.UsageTypeEmpty},
		{"unknown", shared.UsageTypeEmpty},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := gcpUsageType(tc.in)
			if got != tc.want {
				t.Errorf("gcpUsageType(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestGCPExtractHourlyRate(t *testing.T) {
	cases := []struct {
		name   string
		sku    *GCPPricing
		want   float64
		wantOK bool
	}{
		{
			name:   "nanos only",
			sku:    skuWithRate("0", 48000000),
			want:   0.048,
			wantOK: true,
		},
		{
			name:   "units and nanos",
			sku:    skuWithRate("1", 500000000),
			want:   1.5,
			wantOK: true,
		},
		{
			name:   "whole units no nanos",
			sku:    skuWithRate("2", 0),
			want:   2.0,
			wantOK: true,
		},
		{
			name:   "no pricing info",
			sku:    &GCPPricing{PricingInfo: []*PricingInfo{}},
			want:   0,
			wantOK: false,
		},
		{
			name:   "no tiered rates",
			sku:    skuWithRates([]*TieredRates{}),
			want:   0,
			wantOK: false,
		},
		{
			name:   "nil pricing info",
			sku:    &GCPPricing{},
			want:   0,
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := gcpExtractHourlyRate(tc.sku)
			if ok != tc.wantOK {
				t.Errorf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && abs(got-tc.want) > 1e-9 {
				t.Errorf("rate = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGCPBillingPricingSource_ParsePage(t *testing.T) {
	page := gcpBillingPage{
		NextPageToken: "token-xyz",
		SKUs: []*GCPPricing{
			// N1 CPU OnDemand — us-central1
			{
				Category:       &GCPResourceInfo{ResourceFamily: "Compute", ResourceGroup: "N1Standard", UsageType: "OnDemand"},
				ServiceRegions: []string{"us-central1"},
				Description:    "N1 Predefined Instance Core running in Americas",
				PricingInfo:    []*PricingInfo{pricingInfo("0", 31611000)},
			},
			// N1 RAM OnDemand — us-central1
			{
				Category:       &GCPResourceInfo{ResourceFamily: "Compute", ResourceGroup: "N1Standard", UsageType: "OnDemand"},
				ServiceRegions: []string{"us-central1"},
				Description:    "N1 Predefined Instance RAM running in Americas",
				PricingInfo:    []*PricingInfo{pricingInfo("0", 4237000)},
			},
			// T4 GPU OnDemand — us-central1
			{
				Category:       &GCPResourceInfo{ResourceFamily: "Compute", ResourceGroup: "GPU", UsageType: "OnDemand"},
				ServiceRegions: []string{"us-central1"},
				Description:    "NVIDIA Tesla T4 running in Americas",
				PricingInfo:    []*PricingInfo{pricingInfo("0", 400000000)},
			},
			// N1 CPU Commit1Yr — should be skipped
			{
				Category:       &GCPResourceInfo{ResourceFamily: "Compute", ResourceGroup: "N1Standard", UsageType: "Commit1Yr"},
				ServiceRegions: []string{"us-central1"},
				Description:    "Commitment v1: N1 in Americas for 1 year",
				PricingInfo:    []*PricingInfo{pricingInfo("0", 20000000)},
			},
			// Non-Compute resourceFamily — should be skipped
			{
				Category:       &GCPResourceInfo{ResourceFamily: "Storage", ResourceGroup: "SSD", UsageType: "OnDemand"},
				ServiceRegions: []string{"us-central1"},
				Description:    "SSD backed Local Storage",
				PricingInfo:    []*PricingInfo{pricingInfo("0", 17000000)},
			},
			// GPU SKU with zero rate — should be skipped (reserved)
			{
				Category:       &GCPResourceInfo{ResourceFamily: "Compute", ResourceGroup: "GPU", UsageType: "OnDemand"},
				ServiceRegions: []string{"us-central1"},
				Description:    "NVIDIA Tesla V100 running in Americas",
				PricingInfo:    []*PricingInfo{pricingInfo("0", 0)},
			},
		},
	}

	body, _ := json.Marshal(page)
	src, err := NewGCPBillingPricingSource(GCPBillingPricingSourceConfig{APIKey: "test-key"})
	if err != nil {
		t.Fatalf("NewGCPBillingPricingSource error: %v", err)
	}
	pms := pricingmodel.NewPricingModelSet(time.Now().UTC(), GCPBillingPricingSourceKey)

	nextToken, err := src.parsePage(strings.NewReader(string(body)), pms)
	if err != nil {
		t.Fatalf("parsePage error: %v", err)
	}

	if nextToken != "token-xyz" {
		t.Errorf("nextToken = %q, want %q", nextToken, "token-xyz")
	}

	if len(pms.NodePricing) != 3 {
		t.Errorf("expected 3 pricing entries (CPU, RAM, GPU), got %d", len(pms.NodePricing))
	}

	cpuKey := pricingmodel.NodeKey{
		Provider:    shared.ProviderGCP,
		Region:      "us-central1",
		Family:      "n1",
		UsageType:   shared.UsageTypeOnDemand,
		PricingType: pricingmodel.NodePricingTypeCPUCore,
	}
	if _, ok := pms.NodePricing[cpuKey]; !ok {
		t.Error("missing CPU entry for n1/us-central1")
	}

	ramKey := pricingmodel.NodeKey{
		Provider:    shared.ProviderGCP,
		Region:      "us-central1",
		Family:      "n1",
		UsageType:   shared.UsageTypeOnDemand,
		PricingType: pricingmodel.NodePricingTypeRamGB,
	}
	if _, ok := pms.NodePricing[ramKey]; !ok {
		t.Error("missing RAM entry for n1/us-central1")
	}

	gpuKey := pricingmodel.NodeKey{
		Provider:    shared.ProviderGCP,
		Region:      "us-central1",
		UsageType:   shared.UsageTypeOnDemand,
		DeviceType:  "nvidia-tesla-t4",
		PricingType: pricingmodel.NodePricingTypeDevice,
	}
	if entry, ok := pms.NodePricing[gpuKey]; !ok {
		t.Error("missing GPU entry for T4/us-central1")
	} else if abs(entry.HourlyRate-0.4) > 1e-9 {
		t.Errorf("GPU HourlyRate = %v, want 0.4", entry.HourlyRate)
	}
}

func TestGCPBillingPricingSource_ParsePage_Preemptible(t *testing.T) {
	page := gcpBillingPage{
		SKUs: []*GCPPricing{
			{
				Category:       &GCPResourceInfo{ResourceFamily: "Compute", ResourceGroup: "N1Standard", UsageType: "Preemptible"},
				ServiceRegions: []string{"us-east1"},
				Description:    "Preemptible N1 Predefined Instance Core",
				PricingInfo:    []*PricingInfo{pricingInfo("0", 6655000)},
			},
		},
	}

	body, _ := json.Marshal(page)
	src, err := NewGCPBillingPricingSource(GCPBillingPricingSourceConfig{APIKey: "test-key"})
	if err != nil {
		t.Fatalf("NewGCPBillingPricingSource error: %v", err)
	}
	pms := pricingmodel.NewPricingModelSet(time.Now().UTC(), GCPBillingPricingSourceKey)

	if _, err := src.parsePage(strings.NewReader(string(body)), pms); err != nil {
		t.Fatalf("parsePage error: %v", err)
	}

	key := pricingmodel.NodeKey{
		Provider:    shared.ProviderGCP,
		Region:      "us-east1",
		Family:      "n1",
		UsageType:   shared.UsageTypeSpot,
		PricingType: pricingmodel.NodePricingTypeCPUCore,
	}
	if _, ok := pms.NodePricing[key]; !ok {
		t.Error("missing Preemptible CPU entry")
	}
}

// --- helpers ---

func skuWithRate(units string, nanos float64) *GCPPricing {
	return skuWithRates([]*TieredRates{
		{UnitPrice: &UnitPriceInfo{Units: units, Nanos: nanos}},
	})
}

func skuWithRates(rates []*TieredRates) *GCPPricing {
	return &GCPPricing{
		PricingInfo: []*PricingInfo{
			{PricingExpression: &PricingExpression{TieredRates: rates}},
		},
	}
}

func pricingInfo(units string, nanos float64) *PricingInfo {
	return &PricingInfo{
		PricingExpression: &PricingExpression{
			TieredRates: []*TieredRates{
				{UnitPrice: &UnitPriceInfo{Units: units, Nanos: nanos}},
			},
		},
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
