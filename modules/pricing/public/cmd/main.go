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
	compareFile string
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
	rootCmd.Flags().StringVarP(&output, "output", "o", "", "Output file path (optional). Default: stdout")
	rootCmd.Flags().StringVarP(&compareFile, "compare", "x", "", "Compare generated data with this file. Exit 0 if identical, 2 if different")
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

	// Compare mode: check if generated data differs from existing file
	if compareFile != "" {
		return handleCompareMode(data, compareFile, output)
	}

	// Normal mode: write output
	if output != "" {
		if err := os.WriteFile(output, data, 0644); err != nil {
			return fmt.Errorf("failed to write output file: %w", err)
		}
		log.Infof("Wrote pricing data to %s", output)
	} else {
		fmt.Println(string(data))
	}

	return nil
}

func handleCompareMode(newData []byte, compareFilePath string, outputPath string) error {
	// Read the file to compare against
	existingData, err := os.ReadFile(compareFilePath)
	if err != nil {
		return fmt.Errorf("failed to read compare file '%s': %w", compareFilePath, err)
	}

	// Compute checksums
	existingChecksum := computeChecksum(existingData)
	newChecksum := computeChecksum(newData)

	log.Infof("Compare file checksum: %s", existingChecksum)
	log.Infof("New data checksum:     %s", newChecksum)

	// Write output if specified
	if outputPath != "" {
		if err := os.WriteFile(outputPath, newData, 0644); err != nil {
			return fmt.Errorf("failed to write output file: %w", err)
		}
		log.Infof("Wrote pricing data to %s", outputPath)
	}

	// Compare and exit with appropriate code
	if existingChecksum == newChecksum {
		log.Infof("Pricing data is identical")
		return nil
	}

	log.Infof("Pricing data is different")
	os.Exit(2) // Exit 2 to indicate difference
	return nil
}

func computeChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
