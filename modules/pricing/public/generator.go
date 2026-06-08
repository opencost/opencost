package public

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/shared"
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

	// Sort to ensure deterministic output for checksums
	pricingSet.Sort()

	log.Infof("Generated %d AWS node pricing entries", len(pricingSet.NodePricing))
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

	// Sort to ensure deterministic output for checksums
	pricingSet.Sort()

	log.Infof("Generated %d Azure node pricing entries", len(pricingSet.NodePricing))
	return pricingSet, nil
}

// GenerateAllProvidersPricing fetches pricing data for all supported providers
// and combines them into a single PricingSet
func GenerateAllProvidersPricing(currency unit.Currency) (*pricing.PricingSet, error) {
	log.Infof("Generating pricing for all providers in currency: %s", currency)

	// Create a combined pricing set
	combinedSet := &pricing.PricingSet{
		NodePricing:             []*pricing.NodePricing{},
		PersistentVolumePricing: []*pricing.PersistentVolumePricing{},
	}

	// Fetch AWS pricing
	awsSet, err := GenerateAWSPricing(currency)
	if err != nil {
		log.Warnf("Failed to get AWS pricing: %v", err)
	} else {
		combinedSet.NodePricing = append(combinedSet.NodePricing, awsSet.NodePricing...)
		combinedSet.PersistentVolumePricing = append(combinedSet.PersistentVolumePricing, awsSet.PersistentVolumePricing...)
		log.Infof("Added %d AWS node pricing entries", len(awsSet.NodePricing))
	}

	// Fetch Azure pricing
	azureSet, err := GenerateAzurePricing(currency)
	if err != nil {
		log.Warnf("Failed to get Azure pricing: %v", err)
	} else {
		combinedSet.NodePricing = append(combinedSet.NodePricing, azureSet.NodePricing...)
		combinedSet.PersistentVolumePricing = append(combinedSet.PersistentVolumePricing, azureSet.PersistentVolumePricing...)
		log.Infof("Added %d Azure node pricing entries", len(azureSet.NodePricing))
	}

	// Sort the combined set to ensure deterministic output
	combinedSet.Sort()

	log.Infof("Generated combined pricing set with %d total node entries and %d volume entries",
		len(combinedSet.NodePricing), len(combinedSet.PersistentVolumePricing))

	return combinedSet, nil
}

// GeneratePricingForProvider fetches pricing data for a specific provider
// in the specified currency
func GeneratePricingForProvider(provider shared.Provider, currency unit.Currency) (*pricing.PricingSet, error) {
	switch provider {
	case shared.ProviderEmpty:
		return GenerateAllProvidersPricing(currency)
	case shared.ProviderAWS:
		return GenerateAWSPricing(currency)
	case shared.ProviderAzure:
		return GenerateAzurePricing(currency)
	case shared.ProviderGCP:
		return nil, fmt.Errorf("not implemented")
		// return GenerateGCPPricing(currency)
	default:
		return nil, fmt.Errorf("unsupported provider: %s", provider)
	}
}
