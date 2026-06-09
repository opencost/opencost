package gcp

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
	azureDiskFilter     = "serviceName eq 'Storage' and priceType eq 'Consumption'"
)

type GCPPricingSourceConfig struct {
	CurrencyCode string
}

type GCPPricingSource struct {
	config GCPPricingSourceConfig
}

func NewGCPPricingSource(cfg GCPPricingSourceConfig) *GCPPricingSource {
	return &GCPPricingSource{config: cfg}
}

func (g *GCPPricingSource) GetPricing() (*pricing.PricingSet, error) {
	log.Infof("PricingSource (GCP): starting pricing download")
	start := time.Now()

	ps := &pricing.PricingSet{
		Nodes:   []*pricing.NodePricing{},
		Volumes: []*pricing.VolumePricing{},
	}

	// Fetch VM pricing
	url := g.buildVMURL()
	pageCount := 0

	for url != "" {
		resp, err := azureHTTPClient.Get(url)
		if err != nil {
			return nil, fmt.Errorf("PricingSource (GCP): GET %s: %w", url, err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			closeErr := resp.Body.Close()
			if closeErr != nil {
				log.Warnf("failed to close response body: %v", closeErr)
			}
			return nil, fmt.Errorf("PricingSource (GCP): unexpected status %d on VM page %d: %s", resp.StatusCode, pageCount, string(body))
		}

		next, err := g.parseVMPage(resp.Body, ps)
		closeErr := resp.Body.Close()
		if closeErr != nil {
			log.Warnf("failed to close response body: %v", closeErr)
		}
		if err != nil {
			return nil, fmt.Errorf("PricingSource (GCP): parsing VM page %d: %w", pageCount, err)
		}

		pageCount++
		url = next
		log.Debugf("PricingSource (GCP): fetched VM page %d, next: %s", pageCount, url)
	}

	log.Infof("PricingSource (GCP): fetched %d VM pricing entries across %d pages", len(ps.Nodes), pageCount)

	// Fetch disk pricing
	url = a.buildDiskURL()
	diskPageCount := 0

	for url != "" {
		resp, err := azureHTTPClient.Get(url)
		if err != nil {
			log.Warnf("PricingSource (GCP): failed to fetch disk pricing: %v", err)
			break
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			closeErr := resp.Body.Close()
			if closeErr != nil {
				log.Warnf("failed to close response body: %v", closeErr)
			}
			log.Warnf("PricingSource (GCP): unexpected status %d on disk page %d: %s", resp.StatusCode, diskPageCount, string(body))
			break
		}

		next, err := a.parseDiskPage(resp.Body, ps)
		closeErr := resp.Body.Close()
		if closeErr != nil {
			log.Warnf("failed to close response body: %v", closeErr)
		}
		if err != nil {
			log.Warnf("PricingSource (GCP): error parsing disk page %d: %v", diskPageCount, err)
			break
		}

		diskPageCount++
		url = next
		log.Debugf("PricingSource (GCP): fetched disk page %d, next: %s", diskPageCount, url)
	}

	log.Infof("PricingSource (GCP): completed in %s — %d node pricing, %d volume pricing",
		time.Since(start).Round(time.Second), len(ps.Nodes), len(ps.Volumes))

	return ps, nil
}

func (a *AzurePricingSource) buildVMURL() string {
	u := azurePricingBaseURL + "?$filter=" + url.QueryEscape(azureVMFilter)
	if a.config.CurrencyCode != "" {
		u += "&currencyCode=" + url.QueryEscape(a.config.CurrencyCode)
	}
	return u
}

func (a *AzurePricingSource) buildDiskURL() string {
	u := azurePricingBaseURL + "?$filter=" + url.QueryEscape(azureDiskFilter)
	if a.config.CurrencyCode != "" {
		u += "&currencyCode=" + url.QueryEscape(a.config.CurrencyCode)
	}
	return u
}

func (a *AzurePricingSource) parseVMPage(body io.Reader, ps *pricing.PricingSet) (nextURL string, err error) {
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

func (a *AzurePricingSource) parseDiskPage(body io.Reader, ps *pricing.PricingSet) (nextURL string, err error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("reading response body: %w", err)
	}

	var page AzurePricing
	if err := json.Unmarshal(data, &page); err != nil {
		return "", fmt.Errorf("unmarshalling response: %w", err)
	}

	for _, item := range page.Items {
		if !a.includeDiskItem(item) {
			continue
		}

		volumeType := mapAzureDiskType(item.SkuName)
		if volumeType == pricing.VolumeTypeNil {
			continue
		}

		currency, err := unit.ParseCurrency(a.config.CurrencyCode)
		if err != nil {
			log.Warnf("invalid currency code '%s', defaulting to USD: %s", a.config.CurrencyCode, err.Error())
			currency = unit.USD
		}

		// Azure disk pricing is per GB-month, convert to per GB-hour
		hourlyPrice := float64(item.RetailPrice) / 730.0

		volumePricing := &pricing.VolumePricing{
			Properties: pricing.VolumePricingProperties{
				Provider:   pricing.AzureProvider,
				Region:     item.ArmRegionName,
				VolumeType: volumeType,
			},
			Prices: pricing.Prices{
				currency: []pricing.Price{{
					Currency: currency,
					Unit:     unit.Hour,
					Price:    hourlyPrice,
				}},
			},
		}

		ps.Volumes = append(ps.Volumes, volumePricing)
	}

	return page.NextPageLink, nil
}

// includeItem mirrors the filtering logic in the existing Azure provider for VMs.
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

// includeDiskItem filters disk items to include only managed disks.
func (a *AzurePricingSource) includeDiskItem(item AzurePricingAttributes) bool {
	if item.ArmRegionName == "" {
		return false
	}
	productLower := strings.ToLower(item.ProductName)
	// Exclude unmanaged disks explicitly (weird case where "Unmanaged disk" still has managed "managed disk" :\)
	if strings.Contains(productLower, "unmanaged") {
		return false
	}
	// Only include managed disks
	return strings.Contains(productLower, "managed disk")
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
