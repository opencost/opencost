package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
	"github.com/opencost/opencost/modules/pricing/public"
	"github.com/spf13/cobra"
)

var (
	provider string
	currency string
	output   string
)

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
	rootCmd.Flags().StringVarP(&provider, "provider", "p", "aws", "Cloud provider (aws, azure, gcp). Default: aws")
	rootCmd.Flags().StringVarP(&currency, "currency", "c", "USD", "Currency code (e.g. USD, EUR, CNY). Default: USD")
	rootCmd.Flags().StringVarP(&output, "output", "o", "", "Output file name. Default: stdout")
}

func run(cmd *cobra.Command, args []string) error {
	curr, err := unit.ParseCurrency(currency)
	if err != nil {
		return fmt.Errorf("invalid currency '%s': %w", currency, err)
	}

	var prov pricing.Provider
	switch provider {
	case "aws":
		prov = pricing.AWSProvider
	case "azure":
		prov = pricing.AzureProvider
	case "gcp":
		prov = pricing.GCPProvider
	default:
		return fmt.Errorf("unsupported provider: %s", provider)
	}

	log.Infof("Generating pricing for %s in %s", prov, curr)
	pricingSet, err := public.GeneratePricingForProvider(prov, curr)
	if err != nil {
		return fmt.Errorf("failed to generate pricing: %w", err)
	}

	data, err := json.MarshalIndent(pricingSet, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	if output != "" {
		if err := os.WriteFile(output, data, 0644); err != nil {
			return fmt.Errorf("failed to write output file: %w", err)
		}
		log.Infof("Wrote pricing data to %s", output)
	} else {
		fmt.Println(string(data))
	}

	log.Infof("Generated %d node pricing entries and %d volume pricing entries",
		len(pricingSet.Nodes), len(pricingSet.Volumes))

	return nil
}
