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
	"github.com/opencost/opencost/core/pkg/model/shared"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

const (
	azurePricingBaseURL = "https://prices.azure.com/api/retail/prices"
	azureVMFilter       = "serviceName eq 'Virtual Machines' and priceType eq 'Consumption'"
)

// AzurePricingSourceConfig holds configuration for AzurePricingSource.
type AzurePricingSourceConfig struct {
	CurrencyCode string
}

var azureHTTPClient = &http.Client{Timeout: 60 * time.Second}

// AzurePricingSource implements the PricingSource interface using the
// Azure Retail Prices API (no auth required).
type AzurePricingSource struct {
	config AzurePricingSourceConfig
}

func NewAzurePricingSource(cfg AzurePricingSourceConfig) *AzurePricingSource {
	return &AzurePricingSource{config: cfg}
}

func (a *AzurePricingSource) GetPricing() (*pricing.PricingSet, error) {
	log.Infof("PricingSource (Azure): starting pricing download")
	start := time.Now()

	ps := &pricing.PricingSet{
		Nodes:   []*pricing.NodePricing{},
		Volumes: []*pricing.VolumePricing{},
	}

	url := a.buildInitialURL()
	pageCount := 0

	for url != "" {
		resp, err := azureHTTPClient.Get(url)
		if err != nil {
			return nil, fmt.Errorf("PricingSource (Azure): GET %s: %w", url, err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			closeErr := resp.Body.Close()
			if closeErr != nil {
				log.Warnf("failed to close response body: %v", closeErr)
			}
			return nil, fmt.Errorf("PricingSource (Azure): unexpected status %d on page %d: %s", resp.StatusCode, pageCount, string(body))
		}

		next, err := a.parsePage(resp.Body, ps)
		closeErr := resp.Body.Close()
		if closeErr != nil {
			log.Warnf("failed to close response body: %v", closeErr)
		}
		if err != nil {
			return nil, fmt.Errorf("PricingSource (Azure): parsing page %d: %w", pageCount, err)
		}

		pageCount++
		url = next
		log.Debugf("PricingSource (Azure): fetched page %d, next: %s", pageCount, url)
	}

	log.Infof("PricingSource (Azure): completed in %s — %d pricing entries across %d pages",
		time.Since(start).Round(time.Second), len(ps.Nodes), pageCount)

	return ps, nil
}

func (a *AzurePricingSource) buildInitialURL() string {
	u := azurePricingBaseURL + "?$filter=" + url.QueryEscape(azureVMFilter)
	if a.config.CurrencyCode != "" {
		u += "&currencyCode=" + url.QueryEscape(a.config.CurrencyCode)
	}
	return u
}

func (a *AzurePricingSource) parsePage(body io.Reader, ps *pricing.PricingSet) (nextURL string, err error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("reading response body: %w", err)
	}

	var page AzurePricing
	if err := json.Unmarshal(data, &page); err != nil {
		return "", fmt.Errorf("unmarshalling response: %w", err)
	}

	for _, item := range page.Items {
		if !a.includeItem(item) {
			continue
		}

		// Parse the currency from config, default to USD if invalid
		currency, err := unit.ParseCurrency(a.config.CurrencyCode)
		if err != nil {
			log.Warnf("invalid currency code '%s', defaulting to USD: %s", a.config.CurrencyCode, err.Error())
			currency = unit.USD
		}

		priceObj := pricing.Price{
			Currency: currency,
			Unit:     unit.Hour,
			Price:    float64(item.RetailPrice),
		}

		nodePricing := &pricing.NodePricing{
			Properties: pricing.NodePricingProperties{
				Provider:     pricing.Provider(shared.ProviderAzure),
				Region:       item.ArmRegionName,
				InstanceType: item.ArmSkuName,
				Provisioning: pricing.ProvisioningOnDemand,
			},
			Prices: pricing.Prices{
				currency: []pricing.Price{
					priceObj,
				},
			},
		}

		ps.Nodes = append(ps.Nodes, nodePricing)
	}

	return page.NextPageLink, nil
}

// includeItem mirrors the filtering logic in the existing Azure provider.
func (a *AzurePricingSource) includeItem(item AzurePricingAttributes) bool {
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

// AzurePricing represents the response from Azure Retail Prices API
type AzurePricing struct {
	BillingCurrency    string                   `json:"BillingCurrency"`
	CustomerEntityId   string                   `json:"CustomerEntityId"`
	CustomerEntityType string                   `json:"CustomerEntityType"`
	Items              []AzurePricingAttributes `json:"Items"`
	NextPageLink       string                   `json:"NextPageLink"`
	Count              int                      `json:"Count"`
}

// AzurePricingAttributes represents a single pricing item from Azure Retail Prices API
type AzurePricingAttributes struct {
	CurrencyCode         string     `json:"currencyCode"`
	TierMinimumUnits     float32    `json:"tierMinimumUnits"`
	RetailPrice          float32    `json:"retailPrice"`
	UnitPrice            float32    `json:"unitPrice"`
	ArmRegionName        string     `json:"armRegionName"`
	Location             string     `json:"location"`
	EffectiveStartDate   *time.Time `json:"effectiveStartDate"`
	EffectiveEndDate     *time.Time `json:"effectiveEndDate"`
	MeterId              string     `json:"meterId"`
	MeterName            string     `json:"meterName"`
	ProductId            string     `json:"productId"`
	SkuId                string     `json:"skuId"`
	ProductName          string     `json:"productName"`
	SkuName              string     `json:"skuName"`
	ServiceName          string     `json:"serviceName"`
	ServiceId            string     `json:"serviceId"`
	ServiceFamily        string     `json:"serviceFamily"`
	UnitOfMeasure        string     `json:"unitOfMeasure"`
	Type                 string     `json:"type"`
	IsPrimaryMeterRegion bool       `json:"isPrimaryMeterRegion"`
	ArmSkuName           string     `json:"armSkuName"`
}
