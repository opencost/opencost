package gcp

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/cloud"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
	"github.com/opencost/opencost/modules/pricing/public/httpclient"
)

var BillingAPIBaseURL = "https://cloudbilling.googleapis.com/v1/services/6F81-5844-456A/skus"

var gcpHTTPClient = httpclient.NewClient(120 * time.Second)

type GCPPricingSourceConfig struct {
	APIKey       string
	CurrencyCode string
}

type GCPPricingSource struct {
	config GCPPricingSourceConfig
}

func NewGCPPricingSource(cfg GCPPricingSourceConfig) *GCPPricingSource {
	return &GCPPricingSource{
		config: cfg,
	}
}

func (g *GCPPricingSource) GetPricing() (*pricing.PricingSet, error) {
	log.Infof("PricingSource (GCP): starting pricing download")
	start := time.Now()

	ps := &pricing.PricingSet{
		NodePricing:             []*pricing.NodePricing{},
		PersistentVolumePricing: []*pricing.PersistentVolumePricing{},
	}

	// Maps to accumulate CPU and RAM costs per node key
	nodeCPUCosts := make(map[nodeKey]float64)
	nodeRAMCosts := make(map[nodeKey]float64)

	// Track volume pricing
	volumeCosts := make(map[volumeKey]float64)

	pageCount := 0
	nextPageToken := ""

	for {
		pageURL := g.buildURL(nextPageToken)

		resp, err := gcpHTTPClient.Get(pageURL)
		if err != nil {
			return nil, fmt.Errorf("PricingSource (GCP): GET %s: %w", pageURL, err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			closeErr := resp.Body.Close()
			if closeErr != nil {
				log.Warnf("failed to close response body: %v", closeErr)
			}
			return nil, fmt.Errorf("PricingSource (GCP): unexpected status %d on page %d: %s", resp.StatusCode, pageCount, string(body))
		}

		nextToken, err := g.parsePage(resp.Body, nodeCPUCosts, nodeRAMCosts, volumeCosts)
		closeErr := resp.Body.Close()
		if closeErr != nil {
			log.Warnf("failed to close response body: %v", closeErr)
		}
		if err != nil {
			return nil, fmt.Errorf("PricingSource (GCP): parsing page %d: %w", pageCount, err)
		}

		pageCount++
		log.Debugf("PricingSource (GCP): fetched page %d, next token: %s", pageCount, nextToken)

		if nextToken == "" {
			break
		}
		nextPageToken = nextToken
	}

	// Build node pricing from accumulated CPU and RAM costs
	g.buildNodePricing(ps, nodeCPUCosts, nodeRAMCosts)

	// Build volume pricing
	g.buildVolumePricing(ps, volumeCosts)

	log.Infof("PricingSource (GCP): completed in %s — %d pages, %d node pricing, %d volume pricing",
		time.Since(start).Round(time.Second), pageCount, len(ps.NodePricing), len(ps.PersistentVolumePricing))

	return ps, nil
}

func (g *GCPPricingSource) buildURL(pageToken string) string {
	q := url.Values{}
	q.Set("key", g.config.APIKey)
	q.Set("currencyCode", g.config.CurrencyCode)
	if pageToken != "" {
		q.Set("pageToken", pageToken)
	}
	return BillingAPIBaseURL + "?" + q.Encode()
}

func (g *GCPPricingSource) parsePage(body io.Reader, nodeCPUCosts map[nodeKey]float64, nodeRAMCosts map[nodeKey]float64,
	volumeCosts map[volumeKey]float64,
) (nextPageToken string, err error) {

	data, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("reading response body: %w", err)
	}

	var page GCPPricingResponse
	if err := json.Unmarshal(data, &page); err != nil {
		return "", fmt.Errorf("unmarshalling response: %w", err)
	}

	for _, sku := range page.Skus {
		if sku.Category == nil || len(sku.PricingInfo) == 0 {
			continue
		}

		category := sku.Category
		resourceGroup := category.ResourceGroup
		usageType := strings.ToLower(category.UsageType)

		if isStorageResource(resourceGroup) {
			g.parseVolumeSKU(sku, volumeCosts)
			continue
		}

		if isComputeResource(resourceGroup) {
			g.parseComputeSKU(sku, usageType, nodeCPUCosts, nodeRAMCosts)
			continue
		}

		// TODO: Add GPU pricing support
	}

	return page.NextPageToken, nil
}

func (g *GCPPricingSource) parseVolumeSKU(sku *GCPPricing, volumeCosts map[volumeKey]float64) {
	volumeType, isRegional := mapGCPVolumeType(sku.Category.ResourceGroup, sku.Description)
	if volumeType == pricing.VolumeTypeNil {
		return
	}

	// Get the hourly price
	hourlyPrice, err := g.extractHourlyPrice(sku)
	if err != nil || hourlyPrice == 0 {
		return
	}

	// Convert from monthly to hourly (GCP storage is priced per GB-month)
	hourlyPrice = hourlyPrice / 730.0

	// Store pricing for each region
	for _, region := range sku.ServiceRegions {
		key := volumeKey{
			Region:     region,
			VolumeType: volumeType,
			Regional:   isRegional,
		}
		volumeCosts[key] = hourlyPrice
	}
}

func (g *GCPPricingSource) parseComputeSKU(sku *GCPPricing, usageType string, nodeCPUCosts map[nodeKey]float64,
	nodeRAMCosts map[nodeKey]float64,
) {
	resourceGroup := sku.Category.ResourceGroup

	// Normalize the instance type based on description
	instanceType := normalizeInstanceType(resourceGroup, sku.Description)

	// Get hourly price
	hourlyPrice, err := g.extractHourlyPrice(sku)
	if err != nil || hourlyPrice == 0 {
		return
	}

	// Determine if this is CPU or RAM pricing
	isRAM := strings.Contains(strings.ToUpper(sku.Description), "RAM")

	// Handle E2 instance family expansion
	instanceTypes := g.expandInstanceTypes(instanceType, sku.Category.ResourceGroup)

	// Store pricing for each region and instance type
	for _, region := range sku.ServiceRegions {
		for _, instType := range instanceTypes {
			key := nodeKey{
				Region:       region,
				InstanceType: instType,
				UsageType:    usageType,
			}

			if isRAM {
				nodeRAMCosts[key] = hourlyPrice
			} else {
				nodeCPUCosts[key] = hourlyPrice
			}
		}
	}
}

// expandInstanceTypes handles special cases like E2 and A2 families that map to multiple instance types
func (g *GCPPricingSource) expandInstanceTypes(instanceType, resourceGroup string) []string {
	resourceGroupLower := strings.ToLower(resourceGroup)

	// E2 family expands to multiple instance types
	if instanceType == "e2" && (resourceGroupLower == "cpu" || resourceGroupLower == "ram") {
		return []string{"e2-micro", "e2-small", "e2-medium", "e2-standard", "e2-custom"}
	}

	// A2 family expands to multiple GPU-optimized instance types
	if instanceType == "a2" && (resourceGroupLower == "cpu" || resourceGroupLower == "ram") {
		return []string{"a2-highgpu", "a2-megagpu", "a2-ultragpu"}
	}

	return []string{instanceType}
}

// extractHourlyPrice extracts the hourly price from a GCP SKU
func (g *GCPPricingSource) extractHourlyPrice(sku *GCPPricing) (float64, error) {
	if sku == nil || len(sku.PricingInfo) == 0 || sku.PricingInfo[0] == nil || sku.PricingInfo[0].PricingExpression == nil {
		return 0, fmt.Errorf("no pricing info")
	}

	pricingInfo := sku.PricingInfo[0]
	if pricingInfo.PricingExpression == nil || len(pricingInfo.PricingExpression.TieredRates) == 0 {
		return 0, fmt.Errorf("no tiered rates")
	}

	// Get the last tier (highest usage tier, which is the standard rate)
	lastRateIndex := len(pricingInfo.PricingExpression.TieredRates) - 1
	unitPrice := pricingInfo.PricingExpression.TieredRates[lastRateIndex].UnitPrice

	// Parse the base currency units
	unitsBaseCurrency, err := strconv.Atoi(unitPrice.Units)
	if err != nil {
		return 0, fmt.Errorf("parsing base unit price: %w", err)
	}

	// Calculate hourly price: whole currency units + fractional nanos
	// As per https://cloud.google.com/billing/v1/how-tos/catalog-api
	hourlyPrice := float64(unitsBaseCurrency) + (unitPrice.Nanos * math.Pow10(-9))

	return hourlyPrice, nil
}

func (g *GCPPricingSource) buildNodePricing(ps *pricing.PricingSet, nodeCPUCosts map[nodeKey]float64,
	nodeRAMCosts map[nodeKey]float64,
) {
	// Combine CPU and RAM costs into complete node pricing
	processedKeys := make(map[nodeKey]bool)

	// Process all keys that have either CPU or RAM pricing
	allKeys := make(map[nodeKey]bool)
	for k := range nodeCPUCosts {
		allKeys[k] = true
	}
	for k := range nodeRAMCosts {
		allKeys[k] = true
	}

	for key := range allKeys {
		if processedKeys[key] {
			continue
		}
		processedKeys[key] = true

		// GCP's Cloud Billing Catalog API bills both legacy Preemptible VMs
		// and modern Spot VMs under the "Preemptible" usageType, so we map
		// that usage type to our Spot provisioning type.
		provisioning := pricing.ProvisioningOnDemand
		if strings.EqualFold(key.UsageType, "preemptible") {
			provisioning = pricing.ProvisioningSpot
		}

		cpuCost := nodeCPUCosts[key]
		ramCost := nodeRAMCosts[key]

		// Skip if we don't have both CPU and RAM costs
		if cpuCost == 0 || ramCost == 0 {
			continue
		}

		nodePricing := &pricing.NodePricing{
			Properties: pricing.NodePricingProperties{
				Provider:     cloud.ProviderGCP,
				Region:       key.Region,
				InstanceType: key.InstanceType,
				Provisioning: provisioning,
			},
			Prices: pricing.Prices{
				pricing.ResourceCPU: pricing.Price{
					Unit:  unit.Hour,
					Price: cpuCost,
				},
				pricing.ResourceRAM: pricing.Price{
					Unit:  unit.Hour,
					Price: ramCost,
				},
			},
		}

		ps.NodePricing = append(ps.NodePricing, nodePricing)
	}
}

func (g *GCPPricingSource) buildVolumePricing(
	ps *pricing.PricingSet,
	volumeCosts map[volumeKey]float64,
) {
	for key, cost := range volumeCosts {
		volumePricing := &pricing.PersistentVolumePricing{
			Properties: pricing.PersistentVolumePricingProperties{
				Provider:   cloud.ProviderGCP,
				Region:     key.Region,
				VolumeType: key.VolumeType,
			},
			Prices: pricing.Prices{
				pricing.ResourceStorage: pricing.Price{
					Unit:  unit.Hour,
					Price: cost,
				},
			},
		}

		ps.PersistentVolumePricing = append(ps.PersistentVolumePricing, volumePricing)
	}
}
