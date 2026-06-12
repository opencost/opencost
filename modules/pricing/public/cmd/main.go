package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
	"github.com/opencost/opencost/modules/pricing/public"
	"github.com/spf13/cobra"
)

var (
	configs    string
)

const outputPath = "pricing-data/pricing-set.json"

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "fetch-pricing",
	Short: "Fetch cloud provider pricing data",
	Long:  `Fetch pricing data from a cloud provider and output as JSON.`,
	RunE:  run,
}

func init() {
	rootCmd.Flags().StringVarP(&configs, "configs", "c", "", "Provider configurations in format: provider,currency1,currency2;provider2,currency1 (e.g. aws,usd,cny;azure,usd;gcp,usd)")
}

func run(cmd *cobra.Command, args []string) error {
	// Parse the config string
	if configs == "" {
		return fmt.Errorf("--configs flag is required")
	}

	providerConfigs, err := parseProviderConfigs(configs)
	if err != nil {
		return fmt.Errorf("failed to parse configs: %w", err)
	}

	log.Infof("Generating pricing for %d provider configurations", len(providerConfigs))
	for i, pc := range providerConfigs {
		log.Infof("Config %d: %s with currencies %v", i+1, pc.Provider, pc.Currencies)
	}

	pricingSet, err := public.GeneratePricingSet(providerConfigs)
	if err != nil {
		return fmt.Errorf("failed to generate pricing: %w", err)
	}

	data, err := json.MarshalIndent(pricingSet, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	log.Infof("Generated %d node pricing entries and %d volume pricing entries",
		len(pricingSet.Nodes), len(pricingSet.Volumes))


	// Write to file
	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}
	log.Infof("Wrote pricing data to %s", outputPath)

	return nil
}

// parseProviderConfigs parses a config string like "aws,usd,cny;azure,usd;gcp,usd"
// into a slice of ProviderConfig structs
func parseProviderConfigs(configStr string) ([]public.ProviderConfig, error) {
	if configStr == "" {
		return nil, fmt.Errorf("config string cannot be empty")
	}

	var configs []public.ProviderConfig
	
	// Split by slash to get individual provider configs
	providerStrs := strings.Split(configStr, "/")
	
	for _, providerStr := range providerStrs {
		providerStr = strings.TrimSpace(providerStr)
		if providerStr == "" {
			continue
		}
		
		// Split by comma to get provider and currencies
		parts := strings.Split(providerStr, ",")
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid config format '%s': must have at least provider and one currency", providerStr)
		}
		
		// First part is the provider
		providerName := strings.TrimSpace(strings.ToLower(parts[0]))
		var prov pricing.Provider
		switch providerName {
		case "aws":
			prov = pricing.AWSProvider
		case "azure":
			prov = pricing.AzureProvider
		case "gcp":
			prov = pricing.GCPProvider
		default:
			return nil, fmt.Errorf("unsupported provider: %s", providerName)
		}
		
		// Remaining parts are currencies
		var currencies []unit.Currency
		for _, currStr := range parts[1:] {
			currStr = strings.TrimSpace(strings.ToUpper(currStr))
			if currStr == "" {
				continue
			}
			curr, err := unit.ParseCurrency(currStr)
			if err != nil {
				return nil, fmt.Errorf("invalid currency '%s' for provider %s: %w", currStr, providerName, err)
			}
			currencies = append(currencies, curr)
		}
		
		if len(currencies) == 0 {
			return nil, fmt.Errorf("no valid currencies specified for provider %s", providerName)
		}
		
		configs = append(configs, public.ProviderConfig{
			Provider:   prov,
			Currencies: currencies,
		})
	}
	
	if len(configs) == 0 {
		return nil, fmt.Errorf("no valid provider configs found")
	}
	
	return configs, nil
}

