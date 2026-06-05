package main

import (
	"crypto/sha256"
	"encoding/hex"
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
	provider    string
	currency    string
	output      string
	compare     bool
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
	rootCmd.Flags().StringVarP(&output, "output", "o", "", "Output file path. Default: /pricing-data/{provider}-{currency}.json. Use 'stdout' to print to console")
	rootCmd.Flags().BoolVarP(&compare, "compare", "x", false, "Compare with existing file and overwrite if different")
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

	log.Infof("Generated %d node pricing entries and %d volume pricing entries",
		len(pricingSet.Nodes), len(pricingSet.Volumes))

	// Set default output path if not specified
	if output == "" {
		output = fmt.Sprintf("pricing-data/%s/%s-%s.json", provider, provider, currency)
	}

	// Check if user wants stdout
	if output == "stdout" {
		fmt.Println(string(data))
		return nil
	}

	// if comparing, check if file differs and overwrite if needed
	if compare {
		return handleCompareMode(data, output)
	}

	// Normal mode: write to file
	if err := os.WriteFile(output, data, 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}
	log.Infof("Wrote pricing data to %s", output)

	return nil
}

func handleCompareMode(newData []byte, outputPath string) error {
	// Try to read existing file
	existingData, err := os.ReadFile(outputPath)
	if err != nil {
		// File doesn't exist or can't be read, write new data
		if os.IsNotExist(err) {
			log.Infof("File does not exist, creating: %s", outputPath)
		} else {
			log.Warnf("Could not read existing file: %v, overwriting", err)
		}
		if err := os.WriteFile(outputPath, newData, 0644); err != nil {
			return fmt.Errorf("failed to write output file: %w", err)
		}
		log.Infof("Wrote pricing data to %s", outputPath)
		return nil
	}

	// Compute checksums
	existingChecksum := computeChecksum(existingData)
	newChecksum := computeChecksum(newData)

	log.Infof("Existing file checksum: %s", existingChecksum)
	log.Infof("New data checksum:      %s", newChecksum)

	// Compare and overwrite if different
	if existingChecksum == newChecksum {
		log.Infof("Pricing data is identical, no update needed")
		return nil
	}

	log.Infof("Pricing data is different, updating file")
	if err := os.WriteFile(outputPath, newData, 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}
	log.Infof("Updated pricing data at %s", outputPath)
	return nil
}

func computeChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
