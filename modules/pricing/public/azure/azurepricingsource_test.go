package azure

import (
	"strings"
	"testing"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

func TestBuildVMURL(t *testing.T) {
	t.Run("includes encoded filter and currency", func(t *testing.T) {
		source := NewAzurePricingSource(AzurePricingSourceConfig{
			CurrencyCode: "EUR",
		})

		got := source.buildVMURL()

		if !strings.HasPrefix(got, azurePricingBaseURL+"?$filter=") {
			t.Fatalf("expected base URL prefix, got %q", got)
		}
		if !strings.Contains(got, "currencyCode=EUR") {
			t.Fatalf("expected currency code in URL, got %q", got)
		}
		if strings.Contains(got, azureVMFilter) {
			t.Fatalf("expected filter to be URL-escaped, got %q", got)
		}
	})

	t.Run("omits currency when not configured", func(t *testing.T) {
		source := NewAzurePricingSource(AzurePricingSourceConfig{})

		got := source.buildVMURL()

		if strings.Contains(got, "currencyCode=") {
			t.Fatalf("did not expect currency code in URL, got %q", got)
		}
	})
}

func TestIncludeItem(t *testing.T) {
	source := NewAzurePricingSource(AzurePricingSourceConfig{})

	tests := []struct {
		name string
		item AzurePricingAttributes
		want bool
	}{
		{
			name: "includes valid linux VM item",
			item: AzurePricingAttributes{
				ArmSkuName:    "Standard_D2s_v5",
				ArmRegionName: "eastus",
				ProductName:   "Virtual Machines Dsv5 Series",
				SkuName:       "D2s v5",
			},
			want: true,
		},
		{
			name: "excludes missing sku",
			item: AzurePricingAttributes{
				ArmRegionName: "eastus",
				ProductName:   "Virtual Machines Dsv5 Series",
				SkuName:       "D2s v5",
			},
			want: false,
		},
		{
			name: "excludes missing region",
			item: AzurePricingAttributes{
				ArmSkuName:  "Standard_D2s_v5",
				ProductName: "Virtual Machines Dsv5 Series",
				SkuName:     "D2s v5",
			},
			want: false,
		},
		{
			name: "excludes windows items",
			item: AzurePricingAttributes{
				ArmSkuName:    "Standard_D2s_v5",
				ArmRegionName: "eastus",
				ProductName:   "Virtual Machines Windows",
				SkuName:       "D2s v5",
			},
			want: false,
		},
		{
			name: "excludes low priority items",
			item: AzurePricingAttributes{
				ArmSkuName:    "Standard_D2s_v5",
				ArmRegionName: "eastus",
				ProductName:   "Virtual Machines Dsv5 Series",
				SkuName:       "Low Priority D2s v5",
			},
			want: false,
		},
		{
			name: "excludes cloud services items",
			item: AzurePricingAttributes{
				ArmSkuName:    "Standard_D2s_v5",
				ArmRegionName: "eastus",
				ProductName:   "Cloud Services",
				SkuName:       "D2s v5",
			},
			want: false,
		},
		{
			name: "excludes cloudservices items without space",
			item: AzurePricingAttributes{
				ArmSkuName:    "Standard_D2s_v5",
				ArmRegionName: "eastus",
				ProductName:   "CloudServices Extended Support",
				SkuName:       "D2s v5",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := source.includeItem(tt.item)
			if got != tt.want {
				t.Fatalf("expected %t, got %t", tt.want, got)
			}
		})
	}
}

func TestParseVMPage(t *testing.T) {
	t.Run("adds included items and returns next page link", func(t *testing.T) {
		source := NewAzurePricingSource(AzurePricingSourceConfig{
			CurrencyCode: "EUR",
		})
		ps := &pricing.PricingSet{}

		body := `{
			"Items": [
				{
					"armSkuName": "Standard_D2s_v5",
					"armRegionName": "eastus",
					"productName": "Virtual Machines Dsv5 Series",
					"skuName": "D2s v5",
					"retailPrice": 0.25
				},
				{
					"armSkuName": "Standard_D2s_v5",
					"armRegionName": "eastus",
					"productName": "Virtual Machines Windows",
					"skuName": "D2s v5",
					"retailPrice": 0.99
				}
			],
			"NextPageLink": "https://prices.azure.com/next"
		}`

		next, err := source.parseVMPage(strings.NewReader(body), ps)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if next != "https://prices.azure.com/next" {
			t.Fatalf("expected next page link, got %q", next)
		}
		if len(ps.Nodes) != 1 {
			t.Fatalf("expected 1 included node pricing entry, got %d", len(ps.Nodes))
		}

		node := ps.Nodes[0]
		if node.Properties.Provider != pricing.Provider("Azure") {
			t.Fatalf("expected provider %q, got %q", pricing.Provider("Azure"), node.Properties.Provider)
		}
		if node.Properties.Region != "eastus" {
			t.Fatalf("expected region eastus, got %q", node.Properties.Region)
		}
		if node.Properties.InstanceType != "Standard_D2s_v5" {
			t.Fatalf("expected instance type Standard_D2s_v5, got %q", node.Properties.InstanceType)
		}

		prices := node.Prices[unit.EUR]
		if len(prices) != 1 {
			t.Fatalf("expected 1 EUR price entry, got %d", len(prices))
		}
		if prices[0].Price != 0.25 {
			t.Fatalf("expected price 0.25, got %v", prices[0].Price)
		}
		if prices[0].Unit != unit.Hour {
			t.Fatalf("expected unit Hour, got %v", prices[0].Unit)
		}
	})

	t.Run("defaults to USD for invalid configured currency", func(t *testing.T) {
		source := NewAzurePricingSource(AzurePricingSourceConfig{
			CurrencyCode: "INVALID",
		})
		ps := &pricing.PricingSet{}

		body := `{
			"Items": [
				{
					"armSkuName": "Standard_D4s_v5",
					"armRegionName": "westus",
					"productName": "Virtual Machines Dsv5 Series",
					"skuName": "D4s v5",
					"retailPrice": 0.5
				}
			]
		}`

		_, err := source.parseVMPage(strings.NewReader(body), ps)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(ps.Nodes) != 1 {
			t.Fatalf("expected 1 node pricing entry, got %d", len(ps.Nodes))
		}

		prices := ps.Nodes[0].Prices[unit.USD]
		if len(prices) != 1 {
			t.Fatalf("expected 1 USD price entry, got %d", len(prices))
		}
		if prices[0].Currency != unit.USD {
			t.Fatalf("expected USD currency, got %v", prices[0].Currency)
		}
	})

	t.Run("returns error for invalid json", func(t *testing.T) {
		source := NewAzurePricingSource(AzurePricingSourceConfig{})
		ps := &pricing.PricingSet{}

		_, err := source.parseVMPage(strings.NewReader(`{invalid json`), ps)
		if err == nil {
			t.Fatal("expected error for invalid JSON")
		}
	})
}

// Made with Bob
