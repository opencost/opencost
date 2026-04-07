package stackit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

const (
	pimBaseURL    = "https://pim-service.pim.stackit.cloud"
	pimMaxPage    = 100
	pimMaxPages   = 50 // safety limit to prevent infinite pagination
	pimTimeoutSec = 30
)

var pimHTTPClient = &http.Client{
	Timeout: pimTimeoutSec * time.Second,
}

// pimFlavorPricing holds PIM-fetched pricing for a VM flavor.
type pimFlavorPricing struct {
	HourlyCost string
	VCPU       int
	RAMGB      float64
	GPUCount   int
	GPUType    string
}

// pimStoragePricing holds PIM-fetched pricing for a storage class.
type pimStoragePricing struct {
	CostPerGBHr string
}

// pimSearchRequest is the POST body for /v2/skus/search.
type pimSearchRequest struct {
	GeneralProductGroup string `json:"generalProductGroup,omitempty"`
	CategoryName        string `json:"categoryName,omitempty"`
	ProductName         string `json:"productName,omitempty"`
	Metro               *bool  `json:"metro,omitempty"`
}

// pimSKU represents the relevant fields of a PublicSKU from the PIM API.
type pimSKU struct {
	ID                        string                  `json:"id"`
	Name                      string                  `json:"name"`
	CategoryName              string                  `json:"categoryName"`
	GeneralProductGroup       string                  `json:"generalProductGroup"`
	Unit                      string                  `json:"unit"`
	UnitBilling               string                  `json:"unitBilling"`
	Region                    string                  `json:"region"`
	CPUOverprovisioning       *bool                   `json:"cpuOverprovisioning"`
	Prices                    []pimPrice              `json:"prices"`
	ProductSpecificAttributes pimProductSpecificAttrs `json:"productSpecificAttributes"`
	ServiceID                 []string                `json:"serviceId"`
}

type pimPrice struct {
	Value        string `json:"value"`
	MonthlyPrice string `json:"monthlyPrice"`
	CurrencyCode string `json:"currencyCode"`
}

type pimProductSpecificAttrs struct {
	Discriminator string   `json:"discriminator"`
	Flavor        string   `json:"flavor"`
	Hardware      string   `json:"hardware"`
	VCPU          *int     `json:"vCPU"`
	RAM           *float64 `json:"ram"`
	Metro         *bool    `json:"metro"`
	OS            string   `json:"os"`
	// Storage-specific
	Class       string `json:"class"`
	StorageType string `json:"storage"`
	Type        string `json:"type"`
}

type pimSearchResponse struct {
	Meta struct {
		NextCursor string `json:"nextCursor"`
		PageSize   int    `json:"pageSize"`
	} `json:"meta"`
	Data []pimSKU `json:"data"`
}

// fetchAllPIMSKUs fetches all SKUs matching the search criteria, handling pagination.
func fetchAllPIMSKUs(req pimSearchRequest) ([]pimSKU, error) {
	var allSKUs []pimSKU
	cursor := ""

	for page := 0; page < pimMaxPages; page++ {
		url := fmt.Sprintf("%s/v2/skus/search?pageSize=%d", pimBaseURL, pimMaxPage)
		if cursor != "" {
			url += "&cursor=" + cursor
		}

		body, err := json.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("marshaling PIM search request: %w", err)
		}

		httpReq, err := http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("creating PIM request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := pimHTTPClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("PIM API request failed: %w", err)
		}

		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("reading PIM response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("PIM API returned status %d: %s", resp.StatusCode, string(respBody))
		}

		var searchResp pimSearchResponse
		if err := json.Unmarshal(respBody, &searchResp); err != nil {
			return nil, fmt.Errorf("decoding PIM response: %w", err)
		}

		allSKUs = append(allSKUs, searchResp.Data...)

		if searchResp.Meta.NextCursor == "" || len(searchResp.Data) == 0 {
			break
		}
		cursor = searchResp.Meta.NextCursor
	}

	return allSKUs, nil
}

// parsePIMVMFlavors converts VM SKUs into a flavor → pricing map.
// It filters for non-metro SKUs and extracts flavor name, vCPU, RAM, and hourly price.
func parsePIMVMFlavors(skus []pimSKU) map[string]*pimFlavorPricing {
	flavors := make(map[string]*pimFlavorPricing)

	for _, sku := range skus {
		attrs := sku.ProductSpecificAttributes
		flavor := attrs.Flavor
		if flavor == "" {
			continue
		}

		// Skip metro variants (priced separately, ~2x)
		if attrs.Metro != nil && *attrs.Metro {
			continue
		}

		// Must have pricing data
		if len(sku.Prices) == 0 {
			continue
		}

		hourlyPrice := sku.Prices[0].Value
		if hourlyPrice == "" {
			continue
		}

		vcpu := 0
		if attrs.VCPU != nil {
			vcpu = *attrs.VCPU
		}
		ramGB := 0.0
		if attrs.RAM != nil {
			ramGB = *attrs.RAM
		}

		// Detect GPU count from flavor name (e.g. "n1.14d.g1" → 1, "n1.28d.g2" → 2)
		gpuCount := 0
		gpuType := ""
		if strings.HasPrefix(flavor, "n1.") || strings.HasPrefix(flavor, "n2.") || strings.HasPrefix(flavor, "n3.") {
			gpuType = gpuTypeFromFlavor(flavor)
			gpuCount = gpuCountFromFlavor(flavor)
		}

		flavors[flavor] = &pimFlavorPricing{
			HourlyCost: hourlyPrice,
			VCPU:       vcpu,
			RAMGB:      ramGB,
			GPUCount:   gpuCount,
			GPUType:    gpuType,
		}
	}

	return flavors
}

// parsePIMStoragePricing extracts per-GB-hour storage pricing from Storage SKUs.
// Returns a map keyed by storage class name (e.g. "storage_premium_perf0").
// Also includes a "default" entry for the cheapest capacity-based storage found.
func parsePIMStoragePricing(skus []pimSKU) map[string]*pimStoragePricing {
	pricing := make(map[string]*pimStoragePricing)
	var defaultCost float64

	for _, sku := range skus {
		attrs := sku.ProductSpecificAttributes

		// Skip metro variants
		if attrs.Metro != nil && *attrs.Metro {
			continue
		}

		// Only capacity-based storage with per-GB/hour billing
		if sku.UnitBilling != "per GB/hour" {
			continue
		}

		if len(sku.Prices) == 0 || sku.Prices[0].Value == "" {
			continue
		}

		costStr := sku.Prices[0].Value

		// Key by storage class if available
		if attrs.Class != "" {
			pricing[attrs.Class] = &pimStoragePricing{CostPerGBHr: costStr}
		}

		// Track cheapest for default
		cost, err := strconv.ParseFloat(costStr, 64)
		if err == nil && (defaultCost == 0 || cost < defaultCost) {
			defaultCost = cost
			pricing["default"] = &pimStoragePricing{CostPerGBHr: costStr}
		}
	}

	return pricing
}

// gpuCountFromFlavor extracts GPU count from a STACKIT GPU flavor name.
// e.g. "n1.14d.g1" → 1, "n1.28d.g2" → 2, "n3.104d.g8" → 8
func gpuCountFromFlavor(flavor string) int {
	parts := strings.Split(flavor, ".g")
	if len(parts) == 2 {
		if count, err := strconv.Atoi(parts[1]); err == nil {
			return count
		}
	}
	return 0
}

// gpuTypeFromFlavor returns the GPU model based on the flavor prefix.
func gpuTypeFromFlavor(flavor string) string {
	switch {
	case strings.HasPrefix(flavor, "n1."):
		return "NVIDIA A100"
	case strings.HasPrefix(flavor, "n2."):
		return "NVIDIA L40S"
	case strings.HasPrefix(flavor, "n3."):
		return "NVIDIA H100 HGX"
	default:
		return ""
	}
}

// downloadPIMPricing fetches all VM, GPU, and storage pricing from the PIM API.
// Returns the flavor map, storage map, and any error.
func downloadPIMPricing() (map[string]*pimFlavorPricing, map[string]*pimStoragePricing, error) {
	metro := false

	// 1. Fetch non-metro VM SKUs
	log.Infof("STACKIT: fetching VM pricing from PIM API...")
	vmSKUs, err := fetchAllPIMSKUs(pimSearchRequest{
		GeneralProductGroup: "Virtual Machines",
		Metro:               &metro,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("fetching VM SKUs: %w", err)
	}

	flavors := parsePIMVMFlavors(vmSKUs)
	log.Infof("STACKIT: fetched %d VM flavor prices from PIM API", len(flavors))

	// 2. Fetch GPU SKUs (separate category)
	log.Infof("STACKIT: fetching GPU pricing from PIM API...")
	gpuSKUs, err := fetchAllPIMSKUs(pimSearchRequest{
		CategoryName: "Compute Engine GPU",
		Metro:        &metro,
	})
	if err != nil {
		log.Warnf("STACKIT: failed to fetch GPU pricing from PIM API: %v", err)
	} else {
		gpuFlavors := parsePIMVMFlavors(gpuSKUs)
		for k, v := range gpuFlavors {
			flavors[k] = v
		}
		log.Infof("STACKIT: fetched %d GPU flavor prices from PIM API", len(gpuFlavors))
	}

	// 3. Fetch Storage SKUs
	log.Infof("STACKIT: fetching Storage pricing from PIM API...")
	storageSKUs, err := fetchAllPIMSKUs(pimSearchRequest{
		CategoryName: "Storage",
		Metro:        &metro,
	})
	var storagePricing map[string]*pimStoragePricing
	if err != nil {
		log.Warnf("STACKIT: failed to fetch storage pricing from PIM API: %v", err)
	} else {
		storagePricing = parsePIMStoragePricing(storageSKUs)
		log.Infof("STACKIT: fetched %d storage price entries from PIM API", len(storagePricing))
	}

	return flavors, storagePricing, nil
}
