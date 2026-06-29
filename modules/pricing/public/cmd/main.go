package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/unit"
	"github.com/opencost/opencost/modules/pricing/public"
	"github.com/spf13/cobra"
)

var (
	currency string
)

const outputFmt = "modules/pricing/public/%s/pricing-data.json"

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
	rootCmd.Flags().StringVarP(&currency, "currency", "c", "USD", "Currency code (e.g. USD, CNY). Default: USD")
}

func run(cmd *cobra.Command, args []string) error {
	curr, err := unit.ParseCurrency(currency)
	if err != nil {
		return fmt.Errorf("invalid currency '%s': %w", currency, err)
	}

	log.Infof("Generating pricing for %s", curr)
	pricingSet, err := public.GeneratePricing(curr)
	if err != nil {
		return fmt.Errorf("failed to generate pricing: %w", err)
	}

	data, err := json.MarshalIndent(pricingSet, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	log.Infof("Generated %d node pricing entries and %d volume pricing entries",
		len(pricingSet.NodePricing), len(pricingSet.PersistentVolumePricing))


	output := fmt.Sprintf(outputFmt, strings.ToLower(currency))

	// Write to file
	if err := os.WriteFile(output, data, 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}
	log.Infof("Wrote pricing data to %s", output)

	return nil
}
