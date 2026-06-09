package gcp

import (
	"strings"

	"github.com/opencost/opencost/core/pkg/pricing"
)

// GCPPricing represents a single SKU from the GCP Cloud Billing API
type GCPPricing struct {
	Name                string           `json:"name"`
	SKUID               string           `json:"skuId"`
	Description         string           `json:"description"`
	Category            *GCPResourceInfo `json:"category"`
	ServiceRegions      []string         `json:"serviceRegions"`
	PricingInfo         []*PricingInfo   `json:"pricingInfo"`
	ServiceProviderName string           `json:"serviceProviderName"`
}

// PricingInfo contains pricing details for a SKU
type PricingInfo struct {
	Summary                string             `json:"summary"`
	PricingExpression      *PricingExpression `json:"pricingExpression"`
	CurrencyConversionRate float64            `json:"currencyConversionRate"`
	EffectiveTime          string             `json:"-"`
}

// PricingExpression contains the actual pricing rates
type PricingExpression struct {
	UsageUnit                string         `json:"usageUnit"`
	UsageUnitDescription     string         `json:"usageUnitDescription"`
	BaseUnit                 string         `json:"baseUnit"`
	BaseUnitConversionFactor int64          `json:"-"`
	DisplayQuantity          int            `json:"displayQuantity"`
	TieredRates              []*TieredRates `json:"tieredRates"`
}

// TieredRates contains pricing tiers
type TieredRates struct {
	StartUsageAmount int            `json:"startUsageAmount"`
	UnitPrice        *UnitPriceInfo `json:"unitPrice"`
}

// UnitPriceInfo contains the actual price in currency units and nanos
type UnitPriceInfo struct {
	CurrencyCode string  `json:"currencyCode"`
	Units        string  `json:"units"`
	Nanos        float64 `json:"nanos"`
}

// GCPResourceInfo contains categorization information for a SKU
type GCPResourceInfo struct {
	ServiceDisplayName string `json:"serviceDisplayName"`
	ResourceFamily     string `json:"resourceFamily"`
	ResourceGroup      string `json:"resourceGroup"`
	UsageType          string `json:"usageType"`
}

// GCPPricingResponse represents the paginated response from GCP Cloud Billing API
type GCPPricingResponse struct {
	Skus          []*GCPPricing `json:"skus"`
	NextPageToken string        `json:"nextPageToken"`
}

// nodeKey is used internally to track node metadata during parsing
type nodeKey struct {
	Region       string
	InstanceType string
	UsageType    string // OnDemand, Preemptible, Spot
}

// volumeKey is used internally to track volume metadata during parsing
type volumeKey struct {
	Region     string
	VolumeType pricing.VolumeType
	Regional   bool // Whether this is a regional disk
}

// NOTE: What is this for?
// partialCPUMap maps GCP instance types with fractional vCPUs
var partialCPUMap = map[string]float64{
	"e2-micro":  0.25,
	"e2-small":  0.5,
	"e2-medium": 1.0,
}

// mapGCPVolumeType maps GCP disk descriptions to VolumeType constants
func mapGCPVolumeType(resourceGroup, description string) (pricing.VolumeType, bool) {
	resourceGroupLower := strings.ToLower(resourceGroup)
	descriptionLower := strings.ToLower(description)

	isRegional := strings.Contains(descriptionLower, "regional")

	switch resourceGroupLower {
	case "ssd":
		if strings.Contains(descriptionLower, "ssd backed") {
			return pricing.VolumeTypePDSSD, isRegional
		}
	case "pdstandard":
		return pricing.VolumeTypePDStandard, isRegional
	case "pdbalanced":
		return pricing.VolumeTypePDBalanced, isRegional
	case "pdextreme":
		return pricing.VolumeTypePDExtreme, isRegional
	case "hyperdiskbalanced":
		return pricing.VolumeTypeHyperdiskBalanced, isRegional
	case "hyperdiskextreme":
		return pricing.VolumeTypeHyperdiskExtreme, isRegional
	case "hyperdiskthroughput":
		return pricing.VolumeTypeHyperdiskThroughput, isRegional
	}

	return pricing.VolumeTypeNil, false
}

// normalizeInstanceType maps GCP resource groups and descriptions to instance type families
func normalizeInstanceType(resourceGroup, description string) string {
	resourceGroupLower := strings.ToLower(resourceGroup)
	descriptionUpper := strings.ToUpper(description)

	// Handle custom instances
	if (resourceGroupLower == "ram" || resourceGroupLower == "cpu") &&
		strings.Contains(descriptionUpper, "CUSTOM") {
		return "custom"
	}

	// Handle N2 instances
	if (resourceGroupLower == "ram" || resourceGroupLower == "cpu") &&
		strings.Contains(descriptionUpper, "N2") &&
		!strings.Contains(descriptionUpper, "PREMIUM") {
		if strings.Contains(descriptionUpper, "N2D AMD") {
			return "n2d-standard"
		}
		return "n2-standard"
	}

	// Handle N4 instances
	if (resourceGroupLower == "ram" || resourceGroupLower == "cpu") &&
		strings.Contains(descriptionUpper, "N4 INSTANCE") {
		return "n4-standard"
	}

	// Handle A2 instances (GPU-optimized)
	if (resourceGroupLower == "ram" || resourceGroupLower == "cpu") &&
		strings.Contains(descriptionUpper, "A2 INSTANCE") {
		return "a2"
	}

	// Handle C2 instances (compute-optimized)
	if (resourceGroupLower == "ram" || resourceGroupLower == "cpu") &&
		strings.Contains(descriptionUpper, "COMPUTE OPTIMIZED") {
		return "c2-standard"
	}

	// Handle E2 instances
	if (resourceGroupLower == "ram" || resourceGroupLower == "cpu") &&
		strings.Contains(descriptionUpper, "E2 INSTANCE") {
		return "e2"
	}

	// Handle T2D instances
	if (resourceGroupLower == "ram" || resourceGroupLower == "cpu") &&
		strings.Contains(descriptionUpper, "T2D AMD") {
		return "t2d-standard"
	}

	// Handle T2A instances
	if (resourceGroupLower == "ram" || resourceGroupLower == "cpu") &&
		strings.Contains(descriptionUpper, "T2A ARM") {
		return "t2a-standard"
	}

	// Default to the resource group as-is
	return resourceGroupLower
}

// isComputeResource checks if a SKU is for compute resources (CPU/RAM)
func isComputeResource(resourceGroup string) bool {
	resourceGroupLower := strings.ToLower(resourceGroup)
	return resourceGroupLower == "cpu" || resourceGroupLower == "ram"
}

// isStorageResource checks if a SKU is for storage resources
func isStorageResource(resourceGroup string) bool {
	resourceGroupLower := strings.ToLower(resourceGroup)
	return resourceGroupLower == "ssd" ||
		resourceGroupLower == "pdstandard" ||
		resourceGroupLower == "pdbalanced" ||
		resourceGroupLower == "pdextreme" ||
		strings.HasPrefix(resourceGroupLower, "hyperdisk")
}
