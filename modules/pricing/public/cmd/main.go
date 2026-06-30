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
	currency string
	compare  bool
)

// Assumes execution from the /cmd directory
const outputFmt = "../%s/pricing-data.json"

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
	rootCmd.Flags().BoolVar(&compare, "compare", false, "Compare freshly fetched pricing against the existing pricing-data.json; exits 1 if they differ")
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

	log.Infof("Generated %d node pricing entries and %d volume pricing entries",
		len(pricingSet.NodePricing), len(pricingSet.PersistentVolumePricing))

	if compare {
		return comparePricing(curr, pricingSet)
	}

	data, err := json.MarshalIndent(pricingSet, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	output := fmt.Sprintf(outputFmt, strings.ToLower(currency))

	// Write to file
	if err := os.WriteFile(output, data, 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}
	log.Infof("Wrote pricing data to %s", output)

	return nil
}

// comparePricing comparesa fresh pricing set against the existing pricing-data.json
// for a given currency
func comparePricing(curr unit.Currency, newSet *pricing.PricingSet) error {
	existingPath := fmt.Sprintf(outputFmt, strings.ToLower(string(curr)))
	existingBytes, err := os.ReadFile(existingPath)
	if err != nil {
		return fmt.Errorf("failed to read existing pricing data at %s: %w", existingPath, err)
	}

	existingSet := &pricing.PricingSet{}
	if err := json.Unmarshal(existingBytes, existingSet); err != nil {
		return fmt.Errorf("failed to parse existing pricing data: %w", err)
	}

	newChecksum, err := newSet.Checksum()
	if err != nil {
		return fmt.Errorf("failed to checksum new pricing data: %w", err)
	}

	existingChecksum, err := existingSet.Checksum()
	if err != nil {
		return fmt.Errorf("failed to checksum existing pricing data: %w", err)
	}

	if newChecksum != existingChecksum {
		fmt.Fprintf(os.Stderr, "pricing drift detected for %s: existing=%s fresh=%s\n", curr, existingChecksum, newChecksum)
		os.Exit(1)
	}

	log.Infof("Pricing data is up to date for %s (checksum: %s)", curr, existingChecksum)
	return nil
}
