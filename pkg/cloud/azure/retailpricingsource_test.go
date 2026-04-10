package azure

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
	"github.com/opencost/opencost/core/pkg/model/shared"
)

func TestAzureRetailPricingSource_BuildInitialURL(t *testing.T) {
	cases := []struct {
		name         string
		currencyCode string
	}{
		{"no currency", ""},
		{"USD", "USD"},
		{"EUR", "EUR"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := NewAzureRetailPricingSource(AzureRetailPricingSourceConfig{CurrencyCode: tc.currencyCode})
			u := src.buildInitialURL()

			if strings.Contains(u, " ") {
				t.Errorf("URL contains unencoded space: %s", u)
			}
			if strings.Contains(u, "'") {
				t.Errorf("URL contains unencoded single quote: %s", u)
			}
			if !strings.Contains(u, "$filter=") {
				t.Errorf("URL missing $filter param: %s", u)
			}
			if tc.currencyCode != "" {
				if !strings.Contains(u, "currencyCode="+tc.currencyCode) {
					t.Errorf("URL missing or malformed currencyCode param: %s", u)
				}
				// ensure no single quotes wrapped around the currency value
				if strings.Contains(u, "'"+tc.currencyCode+"'") {
					t.Errorf("currency code is wrapped in single quotes: %s", u)
				}
			}
		})
	}
}

func TestAzureRetailPricingSource_IncludeItem(t *testing.T) {
	src := NewAzureRetailPricingSource(AzureRetailPricingSourceConfig{})

	cases := []struct {
		name string
		item AzureRetailPricingAttributes
		want bool
	}{
		{
			name: "valid linux VM",
			item: AzureRetailPricingAttributes{ArmSkuName: "Standard_D4s_v3", ArmRegionName: "eastus", SkuName: "D4s v3", ProductName: "Virtual Machines D Series"},
			want: true,
		},
		{
			name: "spot SKU is included",
			item: AzureRetailPricingAttributes{ArmSkuName: "Standard_D4s_v3", ArmRegionName: "eastus", SkuName: "D4s v3 Spot", ProductName: "Virtual Machines D Series"},
			want: true,
		},
		{
			name: "missing ArmSkuName",
			item: AzureRetailPricingAttributes{ArmRegionName: "eastus", SkuName: "D4s v3", ProductName: "Virtual Machines D Series"},
			want: false,
		},
		{
			name: "missing ArmRegionName",
			item: AzureRetailPricingAttributes{ArmSkuName: "Standard_D4s_v3", SkuName: "D4s v3", ProductName: "Virtual Machines D Series"},
			want: false,
		},
		{
			name: "Windows product excluded",
			item: AzureRetailPricingAttributes{ArmSkuName: "Standard_D4s_v3", ArmRegionName: "eastus", SkuName: "D4s v3", ProductName: "Windows Virtual Machines"},
			want: false,
		},
		{
			name: "low priority excluded",
			item: AzureRetailPricingAttributes{ArmSkuName: "Standard_D4s_v3", ArmRegionName: "eastus", SkuName: "D4s v3 Low Priority", ProductName: "Virtual Machines D Series"},
			want: false,
		},
		{
			name: "cloud services excluded",
			item: AzureRetailPricingAttributes{ArmSkuName: "Standard_D4s_v3", ArmRegionName: "eastus", SkuName: "D4s v3", ProductName: "Cloud Services General"},
			want: false,
		},
		{
			name: "cloudservices excluded",
			item: AzureRetailPricingAttributes{ArmSkuName: "Standard_D4s_v3", ArmRegionName: "eastus", SkuName: "D4s v3", ProductName: "CloudServices General"},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := src.includeItem(tc.item)
			if got != tc.want {
				t.Errorf("includeItem() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUsageTypeFromSku(t *testing.T) {
	cases := []struct {
		skuName string
		want    shared.UsageType
	}{
		{"D4s v3", shared.UsageTypeOnDemand},
		{"D4s v3 Spot", shared.UsageTypeSpot},
		{"D4s v3 spot", shared.UsageTypeSpot},
		{"D4s V3 SPOT", shared.UsageTypeSpot},
		{"E8s v5 Spot Extra", shared.UsageTypeSpot},
		{"", shared.UsageTypeOnDemand},
	}

	for _, tc := range cases {
		t.Run(tc.skuName, func(t *testing.T) {
			got := usageTypeFromSku(tc.skuName)
			if got != tc.want {
				t.Errorf("usageTypeFromSku(%q) = %q, want %q", tc.skuName, got, tc.want)
			}
		})
	}
}

func absf(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func TestAzureRetailPricingSource_ParsePage(t *testing.T) {
	items := []AzureRetailPricingAttributes{
		// included: standard on-demand VM
		{ArmSkuName: "Standard_D4s_v3", ArmRegionName: "eastus", SkuName: "D4s v3", ProductName: "Virtual Machines D Series", RetailPrice: 0.192},
		// included: spot VM
		{ArmSkuName: "Standard_D4s_v3", ArmRegionName: "eastus", SkuName: "D4s v3 Spot", ProductName: "Virtual Machines D Series", RetailPrice: 0.05},
		// excluded: Windows
		{ArmSkuName: "Standard_D4s_v3", ArmRegionName: "eastus", SkuName: "D4s v3", ProductName: "Windows Virtual Machines", RetailPrice: 0.3},
		// excluded: missing region
		{ArmSkuName: "Standard_D4s_v3", SkuName: "D4s v3", ProductName: "Virtual Machines D Series", RetailPrice: 0.192},
	}

	page := AzureRetailPricing{Items: items}
	body, _ := json.Marshal(page)

	src := NewAzureRetailPricingSource(AzureRetailPricingSourceConfig{})
	pms := pricingmodel.NewPricingModelSet(time.Now().UTC(), AzureRetailPricingSourceKey)

	_, err := src.parsePage(strings.NewReader(string(body)), pms)
	if err != nil {
		t.Fatalf("parsePage error: %v", err)
	}

	if len(pms.NodePricing) != 2 {
		t.Errorf("expected 2 pricing entries, got %d", len(pms.NodePricing))
	}

	onDemandKey := pricingmodel.NodeKey{
		Provider:    shared.ProviderAzure,
		Region:      "eastus",
		NodeType:    "Standard_D4s_v3",
		UsageType:   shared.UsageTypeOnDemand,
		PricingType: pricingmodel.NodePricingTypeTotal,
	}
	if entry, ok := pms.NodePricing[onDemandKey]; !ok {
		t.Error("missing OnDemand entry")
	} else if absf(entry.HourlyRate-0.192) > 1e-5 {
		t.Errorf("OnDemand HourlyRate = %v, want ~0.192", entry.HourlyRate)
	}

	spotKey := pricingmodel.NodeKey{
		Provider:    shared.ProviderAzure,
		Region:      "eastus",
		NodeType:    "Standard_D4s_v3",
		UsageType:   shared.UsageTypeSpot,
		PricingType: pricingmodel.NodePricingTypeTotal,
	}
	if _, ok := pms.NodePricing[spotKey]; !ok {
		t.Error("missing Spot entry")
	}
}
