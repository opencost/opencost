package public

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
	"github.com/opencost/opencost/modules/pricing/public/aws"
	"github.com/opencost/opencost/modules/pricing/public/azure"
)

// GenerateAWSPricing fetches AWS pricing data in the specified currency
func GenerateAWSPricing(currency unit.Currency) (*pricing.PricingSet, error) {
	log.Infof("Generating AWS pricing for currency: %s", currency)

	source := aws.NewAWSPricingSource(aws.AWSPricingSourceConfig{
		CurrencyCode: string(currency),
	})

	pricingSet, err := source.GetPricing()
	if err != nil {
		return nil, fmt.Errorf("failed to get AWS pricing: %w", err)
	}

	log.Infof("Generated %d AWS node pricing entries", len(pricingSet.Nodes))
	return pricingSet, nil
}

// GenerateAzurePricing fetches Azure pricing data in the specified currency
func GenerateAzurePricing(currency unit.Currency) (*pricing.PricingSet, error) {
	log.Infof("Generating Azure pricing for currency: %s", currency)

	source := azure.NewAzurePricingSource(azure.AzurePricingSourceConfig{
		CurrencyCode: string(currency),
	})

	pricingSet, err := source.GetPricing()
	if err != nil {
		return nil, fmt.Errorf("failed to get Azure pricing: %w", err)
	}

	log.Infof("Generated %d Azure node pricing entries", len(pricingSet.Nodes))
	return pricingSet, nil
}

// GeneratePricingForProvider fetches pricing data for a specific provider
// in the specified currency
func GeneratePricingForProvider(provider pricing.Provider, currency unit.Currency) (*pricing.PricingSet, error) {
	// NOTE: Could add an "all" flag/provider. Maybe it outputs a single frankensteined file for all providers
	switch provider {
	case pricing.AWSProvider:
		return GenerateAWSPricing(currency)
	case pricing.AzureProvider:
		return GenerateAzurePricing(currency)
	case pricing.GCPProvider:
		return nil, fmt.Errorf("not implemented")
		// return GenerateGCPPricing(currency)
	default:
		return nil, fmt.Errorf("unsupported provider: %s", provider)
	}
}
