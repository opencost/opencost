package azure

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
	"github.com/opencost/opencost/core/pkg/model/shared"
)

const (
	azureRetailPricingBaseURL   = "https://prices.azure.com/api/retail/prices"
	azureRetailVMFilter         = "serviceName eq 'Virtual Machines' and priceType eq 'Consumption'"
	AzureRetailPricingSourceKey = "azure_retail_pricing_api"
)

// AzureRetailPricingSourceConfig holds configuration for AzureRetailPricingSource.
type AzureRetailPricingSourceConfig struct {
	CurrencyCode string
}

var azureRetailHTTPClient = &http.Client{Timeout: 60 * time.Second}

// AzureRetailPricingSource implements pricingmodel.PricingSource using the
// Azure Retail Prices API (no authentication required).
type AzureRetailPricingSource struct {
	config AzureRetailPricingSourceConfig
}

func NewAzureRetailPricingSource(cfg AzureRetailPricingSourceConfig) *AzureRetailPricingSource {
	return &AzureRetailPricingSource{config: cfg}
}

func (a *AzureRetailPricingSource) PricingSourceKey() string {
	return AzureRetailPricingSourceKey
}

func (a *AzureRetailPricingSource) GetPricing() (*pricingmodel.PricingModelSet, error) {
	now := time.Now().UTC()
	pms := pricingmodel.NewPricingModelSet(now, AzureRetailPricingSourceKey)

	url := a.buildInitialURL()
	pageCount := 0

	for url != "" {
		resp, err := azureRetailHTTPClient.Get(url)
		if err != nil {
			return nil, fmt.Errorf("AzureRetailPricingSource: GET %s: %w", url, err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("AzureRetailPricingSource: unexpected status %d on page %d: %s", resp.StatusCode, pageCount, string(body))
		}

		next, err := a.parsePage(resp.Body, pms)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("AzureRetailPricingSource: parsing page %d: %w", pageCount, err)
		}

		pageCount++
		url = next
		log.Debugf("AzureRetailPricingSource: fetched page %d, next: %s", pageCount, url)
	}

	log.Infof("AzureRetailPricingSource: loaded %d pricing entries across %d pages", len(pms.NodePricing), pageCount)
	return pms, nil
}

func (a *AzureRetailPricingSource) buildInitialURL() string {
	u := azureRetailPricingBaseURL + "?$filter=" + url.QueryEscape(azureRetailVMFilter)
	if a.config.CurrencyCode != "" {
		u += "&currencyCode=" + url.QueryEscape(a.config.CurrencyCode)
	}
	return u
}

func (a *AzureRetailPricingSource) parsePage(body io.Reader, pms *pricingmodel.PricingModelSet) (nextURL string, err error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("reading response body: %w", err)
	}

	var page AzureRetailPricing
	if err := json.Unmarshal(data, &page); err != nil {
		return "", fmt.Errorf("unmarshalling response: %w", err)
	}

	for _, item := range page.Items {
		if !a.includeItem(item) {
			continue
		}

		key := pricingmodel.NodeKey{
			Provider:    shared.ProviderAzure,
			Region:      item.ArmRegionName,
			NodeType:    item.ArmSkuName,
			UsageType:   usageTypeFromSku(item.SkuName),
			PricingType: pricingmodel.NodePricingTypeTotal,
		}

		pms.NodePricing[key] = pricingmodel.NodePricing{
			HourlyRate: float64(item.RetailPrice),
		}
	}

	return page.NextPageLink, nil
}

// includeItem mirrors the filtering logic in the existing Azure provider.
func (a *AzureRetailPricingSource) includeItem(item AzureRetailPricingAttributes) bool {
	if item.ArmSkuName == "" || item.ArmRegionName == "" {
		return false
	}
	if strings.Contains(item.ProductName, "Windows") {
		return false
	}
	skuLower := strings.ToLower(item.SkuName)
	productLower := strings.ToLower(item.ProductName)
	if strings.Contains(skuLower, "low priority") {
		return false
	}
	if strings.Contains(productLower, "cloud services") || strings.Contains(productLower, "cloudservices") {
		return false
	}
	return true
}

func usageTypeFromSku(skuName string) shared.UsageType {
	if strings.Contains(strings.ToLower(skuName), " spot") {
		return shared.UsageTypeSpot
	}
	return shared.UsageTypeOnDemand
}
