package public

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
	"github.com/opencost/opencost/modules/pricing/public/aws"
	"github.com/opencost/opencost/modules/pricing/public/azure"
	"github.com/opencost/opencost/modules/pricing/public/gcp"
)

func GenerateCloudPricing(getSource func(currency unit.Currency) PricingSource, currencies []unit.Currency, provider string,
) (*pricing.PricingSet, error) {
	if len(currencies) == 0 {
		return nil, fmt.Errorf("at least one currency must be specified")
	}

	log.Infof("Generating %s pricing for %d currencies): %v", provider, len(currencies), currencies)

	var mergedSet *pricing.PricingSet

	// Fetch and merge pricing for each currency
	for i, currency := range currencies {
		log.Infof("Fetching %s pricing for currency: %s (%d/%d)", provider, currency, i+1, len(currencies))

		source := getSource(currency)

		pricingSet, err := source.GetPricing()
		if err != nil {
			return nil, fmt.Errorf("failed to get %s pricing for currency %s: %w", provider, currency, err)
		}

		if mergedSet == nil {
			mergedSet = pricingSet
		} else {
			mergedSet.Merge(pricingSet)
		}

		log.Infof("Added %d node and %d volume pricing entries for currency %s",
			len(pricingSet.Nodes), len(pricingSet.Volumes), currency)
	}

	// Sort to ensure deterministic output for checksums
	mergedSet.Sort()

	log.Infof("Generated %s pricing set with %d node entries and %d volume entries across %d currencies",
		provider, len(mergedSet.Nodes), len(mergedSet.Volumes), len(currencies))

	return mergedSet, nil
}

// GenerateAllProvidersPricing fetches pricing data for all supported providers
// and combines them into a single PricingSet
func GenerateAllProvidersPricing(currencies []unit.Currency) (*pricing.PricingSet, error) {
	if len(currencies) == 0 {
		return nil, fmt.Errorf("at least one currency must be specified")
	}

	log.Infof("Generating pricing for all providers (%d currencies): %v", len(currencies), currencies)

	var mergedSet *pricing.PricingSet

	// Fetch and merge pricing for each currency across all providers
	for i, currency := range currencies {
		log.Infof("Fetching all providers pricing for currency: %s (%d/%d)", currency, i+1, len(currencies))

		// Create a combined pricing set for this currency
		currencySet := &pricing.PricingSet{
			Nodes:   []*pricing.NodePricing{},
			Volumes: []*pricing.VolumePricing{},
		}

		// Fetch AWS pricing
		awsSource := aws.NewAWSPricingSource(aws.AWSPricingSourceConfig{
			CurrencyCode: string(currency),
		})
		awsSet, err := awsSource.GetPricing()
		if err != nil {
			log.Warnf("Failed to get AWS pricing for currency %s: %v", currency, err)
		} else {
			currencySet.Nodes = append(currencySet.Nodes, awsSet.Nodes...)
			currencySet.Volumes = append(currencySet.Volumes, awsSet.Volumes...)
			log.Infof("Added %d AWS node and %d volume pricing entries for currency %s",
				len(awsSet.Nodes), len(awsSet.Volumes), currency)
		}

		// Fetch Azure pricing
		azureSource := azure.NewAzurePricingSource(azure.AzurePricingSourceConfig{
			CurrencyCode: string(currency),
		})
		azureSet, err := azureSource.GetPricing()
		if err != nil {
			log.Warnf("Failed to get Azure pricing for currency %s: %v", currency, err)
		} else {
			currencySet.Nodes = append(currencySet.Nodes, azureSet.Nodes...)
			currencySet.Volumes = append(currencySet.Volumes, azureSet.Volumes...)
			log.Infof("Added %d Azure node and %d volume pricing entries for currency %s",
				len(azureSet.Nodes), len(azureSet.Volumes), currency)
		}

		// Fetch GCP pricing
		gcpSource := gcp.NewGCPPricingSource(gcp.GCPPricingSourceConfig{
			CurrencyCode: string(currency),
		})
		gcpSet, err := gcpSource.GetPricing()
		if err != nil {
			log.Warnf("Failed to get GCP pricing for currency %s: %v", currency, err)
		} else {
			currencySet.Nodes = append(currencySet.Nodes, gcpSet.Nodes...)
			currencySet.Volumes = append(currencySet.Volumes, gcpSet.Volumes...)
			log.Infof("Added %d GCP node and %d volume pricing entries for currency %s",
				len(gcpSet.Nodes), len(gcpSet.Volumes), currency)
		}

		if mergedSet == nil {
			mergedSet = currencySet
		} else {
			mergedSet.Merge(currencySet)
		}
	}

	// Sort to ensure deterministic output for checksums
	mergedSet.Sort()

	log.Infof("Generated combined pricing set with %d node entries and %d volume entries across %d currencies",
		len(mergedSet.Nodes), len(mergedSet.Volumes), len(currencies))

	return mergedSet, nil
}

// GeneratePricingForProvider fetches pricing data for a specific provider
// in the specified currencies
func GeneratePricingForProvider(provider pricing.Provider, currencies []unit.Currency) (*pricing.PricingSet, error) {
	switch provider {
	case pricing.AllProvider:
		return GenerateAllProvidersPricing(currencies)

	case pricing.AWSProvider:
		return GenerateCloudPricing(func(currency unit.Currency) PricingSource {
			return aws.NewAWSPricingSource(aws.AWSPricingSourceConfig{
				CurrencyCode: string(currency),
			})
		}, currencies, "AWS")

	case pricing.AzureProvider:
		return GenerateCloudPricing(func(currency unit.Currency) PricingSource {
			return azure.NewAzurePricingSource(azure.AzurePricingSourceConfig{
				CurrencyCode: string(currency),
			})
		}, currencies, "Azure")

	case pricing.GCPProvider:
		return GenerateCloudPricing(func(currency unit.Currency) PricingSource {
			return gcp.NewGCPPricingSource(gcp.GCPPricingSourceConfig{
				CurrencyCode: string(currency),
			})
		}, currencies, "GCP")

	default:
		return nil, fmt.Errorf("unsupported provider: %s", provider)
	}
}
