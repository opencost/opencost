package public

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/model/shared"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

// TestPricingModuleConfig tests that the config struct is properly defined
func TestPricingModuleConfig(t *testing.T) {
	config := PricingModuleConfig{
		Provider: shared.ProviderAWS,
		Currency: unit.USD,
	}

	if config.Provider != shared.ProviderAWS {
		t.Errorf("Provider = %v, want %v", config.Provider, shared.ProviderAWS)
	}
	if config.Currency != unit.USD {
		t.Errorf("Currency = %v, want %v", config.Currency, unit.USD)
	}
}

// TestProviderPricingStructure tests the nested map structure
func TestProviderPricingStructure(t *testing.T) {
	providers := make(ProviderPricing)

	// Create a simple structure
	instanceMap := make(InstanceTypePricing)
	regionMap := make(RegionPricing)

	prices := &pricing.Prices{
		pricing.ResourceNode: pricing.Price{
			Unit:  unit.Hour,
			Price: 0.0416,
		},
	}

	regionMap["us-east-1"] = prices
	instanceMap["t3.medium"] = &regionMap
	providers[shared.ProviderAWS] = &instanceMap

	// Verify structure
	if providers[shared.ProviderAWS] == nil {
		t.Fatal("AWS provider not found")
	}

	if (*providers[shared.ProviderAWS])["t3.medium"] == nil {
		t.Fatal("t3.medium instance type not found")
	}

	if (*(*providers[shared.ProviderAWS])["t3.medium"])["us-east-1"] == nil {
		t.Fatal("us-east-1 region not found")
	}
}

// Made with Bob
