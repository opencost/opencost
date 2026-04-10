package gcp

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
	"github.com/opencost/opencost/core/pkg/model/shared"
)

const (
	gcpBillingComputeServiceID = "6F81-5844-456A"
	gcpBillingBaseURL          = "https://cloudbilling.googleapis.com/v1/services/" + gcpBillingComputeServiceID + "/skus"
	GCPBillingPricingSourceKey = "gcp_billing_catalog_api"
	gcpResourceFamilyCompute   = "Compute"
	gcpResourceGroupGPU        = "GPU"
	gcpUsageTypeOnDemand       = "OnDemand"
	gcpUsageTypePreemptible    = "Preemptible"
)

// GCPBillingPricingSourceConfig holds configuration for GCPBillingPricingSource.
type GCPBillingPricingSourceConfig struct {
	APIKey       string
	CurrencyCode string
}

// GCPBillingPricingSource implements pricingmodel.PricingSource using the
// GCP Cloud Billing Catalog API. It emits per-vCPU, per-GB RAM, and per-GPU
// hourly rates keyed by family and region, which consumers combine with
// machine specs to compute total instance costs.
type GCPBillingPricingSource struct {
	config GCPBillingPricingSourceConfig
}

func NewGCPBillingPricingSource(cfg GCPBillingPricingSourceConfig) *GCPBillingPricingSource {
	return &GCPBillingPricingSource{config: cfg}
}

func (g *GCPBillingPricingSource) PricingSourceKey() string {
	return GCPBillingPricingSourceKey
}

func (g *GCPBillingPricingSource) GetPricing() (*pricingmodel.PricingModelSet, error) {
	now := time.Now().UTC()
	pms := pricingmodel.NewPricingModelSet(now, GCPBillingPricingSourceKey)

	url := g.buildURL("")
	pageCount := 0

	for url != "" {
		resp, err := http.Get(url)
		if err != nil {
			return nil, fmt.Errorf("GCPBillingPricingSource: GET: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("GCPBillingPricingSource: unexpected status %d on page %d: %s", resp.StatusCode, pageCount, string(body))
		}

		nextToken, err := g.parsePage(resp.Body, pms)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("GCPBillingPricingSource: parsing page %d: %w", pageCount, err)
		}

		pageCount++
		log.Debugf("GCPBillingPricingSource: fetched page %d", pageCount)

		if nextToken == "" {
			break
		}
		url = g.buildURL(nextToken)
	}

	log.Infof("GCPBillingPricingSource: loaded %d pricing entries across %d pages", len(pms.NodePricing), pageCount)
	return pms, nil
}

func (g *GCPBillingPricingSource) buildURL(pageToken string) string {
	url := fmt.Sprintf("%s?key=%s", gcpBillingBaseURL, g.config.APIKey)
	if g.config.CurrencyCode != "" {
		url += "&currencyCode=" + g.config.CurrencyCode
	}
	if pageToken != "" {
		url += "&pageToken=" + pageToken
	}
	return url
}

type gcpBillingPage struct {
	SKUs          []*GCPPricing `json:"skus"`
	NextPageToken string        `json:"nextPageToken"`
}

func (g *GCPBillingPricingSource) parsePage(body io.Reader, pms *pricingmodel.PricingModelSet) (string, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("reading response body: %w", err)
	}

	var page gcpBillingPage
	if err := json.Unmarshal(data, &page); err != nil {
		return "", fmt.Errorf("unmarshalling response: %w", err)
	}

	for _, sku := range page.SKUs {
		g.processSKU(sku, pms)
	}

	return page.NextPageToken, nil
}

func (g *GCPBillingPricingSource) processSKU(sku *GCPPricing, pms *pricingmodel.PricingModelSet) {
	if sku.Category == nil || sku.Category.ResourceFamily != gcpResourceFamilyCompute {
		return
	}

	usageType := gcpUsageType(sku.Category.UsageType)
	if usageType == shared.UsageTypeEmpty {
		return // skip commitments and unrecognized usage types
	}

	hourlyRate, ok := gcpExtractHourlyRate(sku)
	if !ok || hourlyRate == 0 {
		return
	}

	if strings.EqualFold(sku.Category.ResourceGroup, gcpResourceGroupGPU) {
		g.processGPUSKU(sku, usageType, hourlyRate, pms)
		return
	}

	g.processComputeSKU(sku, usageType, hourlyRate, pms)
}

func (g *GCPBillingPricingSource) processGPUSKU(sku *GCPPricing, usageType shared.UsageType, hourlyRate float64, pms *pricingmodel.PricingModelSet) {
	accelerator := NormalizeGPULabel(sku.Description)
	if accelerator == "" {
		return
	}
	for _, region := range sku.ServiceRegions {
		key := pricingmodel.NodeKey{
			Provider:    shared.ProviderGCP,
			Region:      region,
			UsageType:   usageType,
			Accelerator: accelerator,
			Type:        pricingmodel.NodePricingTypeGPU,
		}
		pms.NodePricing[key] = pricingmodel.NodePricing{HourlyRate: hourlyRate}
	}
}

func (g *GCPBillingPricingSource) processComputeSKU(sku *GCPPricing, usageType shared.UsageType, hourlyRate float64, pms *pricingmodel.PricingModelSet) {
	// Skip legacy ambiguous resource groups — family cannot be determined without
	// parsing the description, which is unreliable across SKU generations.
	rg := strings.ToLower(sku.Category.ResourceGroup)
	if rg == "ram" || rg == "cpu" {
		return
	}

	family := gcpNormalizeFamily(sku.Category.ResourceGroup)
	if family == "" {
		return
	}

	pricingType := pricingmodel.NodePricingTypeCPUCore
	if strings.Contains(strings.ToUpper(sku.Description), "RAM") {
		pricingType = pricingmodel.NodePricingTypeRamGB
	}

	for _, region := range sku.ServiceRegions {
		key := pricingmodel.NodeKey{
			Provider:  shared.ProviderGCP,
			Region:    region,
			Family:    family,
			UsageType: usageType,
			Type:      pricingType,
		}
		pms.NodePricing[key] = pricingmodel.NodePricing{HourlyRate: hourlyRate}
	}
}

// gcpNormalizeFamily maps a GCP Billing API ResourceGroup (e.g. "N1Standard",
// "N2DStandard", "E2", "A2") to a lowercase family identifier (e.g. "n1",
// "n2d", "e2", "a2") by lowercasing and stripping the "standard" suffix.
func gcpNormalizeFamily(resourceGroup string) string {
	return strings.TrimSuffix(strings.ToLower(resourceGroup), "standard")
}

// gcpUsageType maps GCP billing usage type strings to shared.UsageType.
// Returns UsageTypeEmpty for commitment SKUs, which should be skipped.
func gcpUsageType(gcpType string) shared.UsageType {
	switch gcpType {
	case gcpUsageTypeOnDemand:
		return shared.UsageTypeOnDemand
	case gcpUsageTypePreemptible:
		return shared.UsageTypeSpot
	default:
		return shared.UsageTypeEmpty
	}
}

// gcpExtractHourlyRate extracts the hourly rate from a SKU's pricing info.
// Per the GCP Billing Catalog API docs, the price is units + nanos*10^-9.
func gcpExtractHourlyRate(sku *GCPPricing) (float64, bool) {
	if len(sku.PricingInfo) == 0 {
		return 0, false
	}
	rates := sku.PricingInfo[0].PricingExpression.TieredRates
	if len(rates) == 0 {
		return 0, false
	}
	last := rates[len(rates)-1]
	units, err := strconv.Atoi(last.UnitPrice.Units)
	if err != nil {
		units = 0
	}
	return (last.UnitPrice.Nanos * math.Pow10(-9)) + float64(units), true
}
