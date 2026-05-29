package public

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
	"github.com/opencost/opencost/modules/pricing/public/aws"
	"github.com/opencost/opencost/modules/pricing/public/azure"
)

// GenerateAllPricing fetches pricing data from all configured providers
// in the specified currency and returns a consolidated PricingSet
func GenerateAllPricing(currency unit.Currency) (*pricing.PricingSet, error) {
	result := &pricing.PricingSet{
		Nodes:   []*pricing.NodePricing{},
		Volumes: []*pricing.VolumePricing{},
	}

	// Generate AWS pricing
	awsPricing, err := GenerateAWSPricing(currency)
	if err != nil {
		return nil, fmt.Errorf("failed to generate AWS pricing: %w", err)
	}
	result.Nodes = append(result.Nodes, awsPricing.Nodes...)
	result.Volumes = append(result.Volumes, awsPricing.Volumes...)

	// Generate Azure pricing
	azurePricing, err := GenerateAzurePricing(currency)
	if err != nil {
		return nil, fmt.Errorf("failed to generate Azure pricing: %w", err)
	}
	result.Nodes = append(result.Nodes, azurePricing.Nodes...)
	result.Volumes = append(result.Volumes, azurePricing.Volumes...)

	return result, nil
}

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
