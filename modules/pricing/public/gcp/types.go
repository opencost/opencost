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

// gpuKey identifies the hourly price of one GPU product in a GCP region and
// purchase option. GPU SKUs do not include a machine type, so they are joined
// with completed CPU/RAM node prices when the PricingSet is built.
type gpuKey struct {
	Region    string
	Product   string
	UsageType string // OnDemand, Preemptible, Spot
}

// volumeKey is used internally to track volume metadata during parsing
type volumeKey struct {
	Region     string
	VolumeType pricing.VolumeType
	Regional   bool // Whether this is a regional disk
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

// isCommitmentOrReservedSKU checks whether a SKU is for committed use discounts (CUD) or reserved instances
func isCommitmentOrReservedSKU(description string) bool {
	d := strings.ToUpper(description)
	return strings.Contains(d, "COMMITMENT") || strings.Contains(d, "RESERVATION")
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

// isGPUResource checks whether a Catalog SKU is priced per attached GPU.
func isGPUResource(resourceGroup string) bool {
	return strings.EqualFold(resourceGroup, "GPU")
}

const gpuProductLabel = "nvidia.com/gpu.product"

// normalizeGPUProduct maps recognized Catalog SKU descriptions to the
// nvidia.com/gpu.product node-label values used by the KCM GPU-pricing path.
// The returned string must match the exact case-sensitive value stamped on
// nodes by the NVIDIA device plugin (e.g. "Tesla-T4", "NVIDIA-A100-SXM4-40GB")
// because ClickHouse stage 02 derivation matches via exact FNV-32a bitmap hashes.
// Unknown descriptions are intentionally skipped rather than emitting a price
// that could match the wrong GPU model.
func normalizeGPUProduct(description string) string {
	desc := strings.ToLower(description)

	// A100 must be checked first: the 80 GB variant has a distinct product string.
	if strings.Contains(desc, "a100") {
		if strings.Contains(desc, "80gb") || strings.Contains(desc, "80 gb") {
			return "NVIDIA-A100-80GB-PCIe"
		}
		return "Tesla-A100"
	}

	switch {
	case strings.Contains(desc, "l4"):
		return "NVIDIA-L4"
	case strings.Contains(desc, "t4"):
		return "Tesla-T4"
	case strings.Contains(desc, "v100"):
		return "Tesla-V100"
	case strings.Contains(desc, "p100"):
		return "Tesla-P100"
	case strings.Contains(desc, "p4"):
		return "Tesla-P4"
	case strings.Contains(desc, "k80"):
		return "Tesla-K80"
	default:
		return ""
	}
}
