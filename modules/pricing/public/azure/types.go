package azure

import (
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/pricing"
)

// mapAzureDiskType maps Azure disk SKU names to VolumeType constants
func mapAzureDiskType(skuName string) pricing.VolumeType {
	skuLower := strings.ToLower(skuName)

	if strings.Contains(skuLower, "premium ssd v2") || strings.Contains(skuLower, "premiumv2") {
		return pricing.VolumeTypePremiumV2LRS
	}
	if strings.Contains(skuLower, "premium") {
		return pricing.VolumeTypePremiumLRS
	}
	if strings.Contains(skuLower, "standard ssd") || strings.Contains(skuLower, "standardssd") {
		return pricing.VolumeTypeStandardSSDLRS
	}
	if strings.Contains(skuLower, "standard") {
		return pricing.VolumeTypeStandardHDDLRS
	}
	if strings.Contains(skuLower, "ultra") {
		return pricing.VolumeTypeUltraSSDLRS
	}

	return pricing.VolumeTypeNil
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
