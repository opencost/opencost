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

// GeneratePricingSet fetches pricing data for all valid provider configs
func GeneratePricingSet(providerConfigs []ProviderConfig) (*pricing.PricingSet, error) {
	if len(providerConfigs) == 0 {
		return nil, fmt.Errorf("at least one provider config must be specified")
	}

	log.Infof("Generating pricing for providers")

	var mergedSet *pricing.PricingSet

	// Fetch and merge pricing by provider by currency
	for _, providerConfig := range providerConfigs {
		for _, currency := range providerConfig.Currencies {
			currentSet, err := GetPricing(providerConfig.Provider, currency)
			if err != nil {
				return nil, err
			}

			if mergedSet == nil {
				mergedSet = currentSet
			} else {
				mergedSet.Merge(currentSet)
			}
		}
	}

	mergedSet.Normalize()
	mergedSet.Sort()

	log.Infof("Generated combined pricing set with %d node entries and %d volume entries",
		len(mergedSet.Nodes), len(mergedSet.Volumes))

	return mergedSet, nil
}

func GetPricing(provider pricing.Provider, currency unit.Currency) (*pricing.PricingSet, error) {
	log.Infof("Fetching %s pricing in %s", provider, currency)
	switch provider {
		case pricing.AWSProvider:
			awsSource := aws.NewAWSPricingSource(aws.AWSPricingSourceConfig{
				CurrencyCode: string(currency),
			})
			awsSet, err := awsSource.GetPricing()
			if err != nil {
				return nil, err
			}
			return awsSet, nil

		case pricing.AzureProvider:
			azureSource := azure.NewAzurePricingSource(azure.AzurePricingSourceConfig{
			CurrencyCode: string(currency),
			})
			azureSet, err := azureSource.GetPricing()
			if err != nil {
				return nil, err
			}
			return azureSet, nil

		case pricing.GCPProvider:
			gcpSource := gcp.NewGCPPricingSource(gcp.GCPPricingSourceConfig{
				CurrencyCode: string(currency),
			})
			gcpSet, err := gcpSource.GetPricing()
			if err != nil {
				return nil, err
			}
			return gcpSet, nil

		default:
			return nil, fmt.Errorf("unsupported provider: %s", provider)
	}
}
