package public

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

// TestPricingModuleConfig tests that the config struct is properly defined
func TestPricingModuleConfig(t *testing.T) {
	config := PricingModuleConfig{
		ProviderConfigs: []ProviderConfig{
			{
				Provider:   pricing.AWSProvider,
				Currencies: []unit.Currency{unit.USD},
			},
		},
	}

	if len(config.ProviderConfigs) != 1 {
		t.Errorf("ProviderConfigs length = %v, want %v", len(config.ProviderConfigs), 1)
	}
	if config.ProviderConfigs[0].Provider != pricing.AWSProvider {
		t.Errorf("Provider = %v, want %v", config.ProviderConfigs[0].Provider, pricing.AWSProvider)
	}
	if len(config.ProviderConfigs[0].Currencies) != 1 || config.ProviderConfigs[0].Currencies[0] != unit.USD {
		t.Errorf("Currencies = %v, want %v", config.ProviderConfigs[0].Currencies, []unit.Currency{unit.USD})
	}
}

// TestProviderPricingStructure tests the nested map structure
func TestProviderPricingStructure(t *testing.T) {
	providers := make(ProviderPricing)

	// Create a simple structure
	instanceMap := make(InstanceTypePricing)
	regionMap := make(RegionPricing)

	prices := &pricing.Prices{
		unit.USD: []pricing.Price{
			{Currency: unit.USD, Unit: unit.Hour, Price: 0.0416},
		},
	}

	regionMap["us-east-1"] = prices
	instanceMap["t3.medium"] = &regionMap
	providers[pricing.AWSProvider] = &instanceMap

	// Verify structure
	if providers[pricing.AWSProvider] == nil {
		t.Fatal("AWS provider not found")
	}

	if (*providers[pricing.AWSProvider])["t3.medium"] == nil {
		t.Fatal("t3.medium instance type not found")
	}

	if (*(*providers[pricing.AWSProvider])["t3.medium"])["us-east-1"] == nil {
		t.Fatal("us-east-1 region not found")
	}

	retrievedPrices := (*(*providers[pricing.AWSProvider])["t3.medium"])["us-east-1"]
	if len((*retrievedPrices)[unit.USD]) == 0 {
		t.Fatal("No USD prices found")
	}

	if (*retrievedPrices)[unit.USD][0].Price != 0.0416 {
		t.Errorf("Price = %v, want %v", (*retrievedPrices)[unit.USD][0].Price, 0.0416)
	}
}

// Made with Bob
