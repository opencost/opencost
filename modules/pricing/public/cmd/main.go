package main

import (
	"bufio"
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
	currency  string
	compare   bool
	outputDir string
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
	Long:  `Fetch pricing data from a cloud provider and output as JSONL files.`,
	RunE:  run,
}

func init() {
	rootCmd.Flags().StringVarP(&currency, "currency", "c", "USD", "Currency code (e.g. USD, CNY). Default: USD")
	rootCmd.Flags().BoolVar(&compare, "compare", false, "Compare freshly fetched pricing against the existing JSONL files; exits 2 if they differ")
	rootCmd.Flags().StringVarP(&outputDir, "output", "o", "..", "Base output directory")
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

	dir := fmt.Sprintf("%s/%s", outputDir, strings.ToLower(string(curr)))
	return writePricingJSONL(dir, pricingSet)
}

// writePricingJSONL writes each pricing kind to its own JSONL file under dir.
func writePricingJSONL(dir string, ps *pricing.PricingSet) error {
	if err := writeJSONL(dir+"/nodes.jsonl", ps.NodePricing); err != nil {
		return err
	}
	if err := writeJSONL(dir+"/persistentvolumes.jsonl", ps.PersistentVolumePricing); err != nil {
		return err
	}
	return nil
}

// writeJSONL marshals each item in items as a single line and writes them to path.
func writeJSONL[T any](path string, items []T) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w) // Encode appends a trailing newline after each value.
	for _, item := range items {
		if err := enc.Encode(item); err != nil {
			return fmt.Errorf("encoding record to %s: %w", path, err)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flushing %s: %w", path, err)
	}
	log.Infof("Wrote %d records to %s", len(items), path)
	return nil
}

// comparePricing compares a fresh pricing set against the existing JSONL files
// for a given currency.
func comparePricing(curr unit.Currency, newSet *pricing.PricingSet) error {
	dir := fmt.Sprintf("%s/%s", outputDir, strings.ToLower(string(curr)))
	existingSet, err := readPricingJSONL(dir)
	if err != nil {
		return fmt.Errorf("reading existing pricing data from %s: %w", dir, err)
	}
	existingSet.Sort()

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
		os.Exit(2)
	}

	log.Infof("Pricing data is up to date for %s (checksum: %s)", curr, existingChecksum)
	return nil
}

// readPricingJSONL reads nodes.jsonl and persistentvolumes.jsonl from dir.
func readPricingJSONL(dir string) (*pricing.PricingSet, error) {
	ps := &pricing.PricingSet{}

	nodes, err := readJSONL[*pricing.NodePricing](dir + "/nodes.jsonl")
	if err != nil {
		return nil, err
	}
	ps.NodePricing = nodes

	pvs, err := readJSONL[*pricing.PersistentVolumePricing](dir + "/persistentvolumes.jsonl")
	if err != nil {
		return nil, err
	}
	ps.PersistentVolumePricing = pvs

	return ps, nil
}

// readJSONL decodes every line of path into a slice of T.
func readJSONL[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()

	var items []T
	dec := json.NewDecoder(f)
	for dec.More() {
		var item T
		if err := dec.Decode(&item); err != nil {
			return nil, fmt.Errorf("decoding record from %s: %w", path, err)
		}
		items = append(items, item)
	}
	return items, nil
}
