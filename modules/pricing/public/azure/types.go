package azure

import (
	"strings"

	"github.com/opencost/opencost/core/pkg/pricing"
)

// nodeKey is used internally to track node metadata during parsing
type nodeKey struct {
	Region       string
	InstanceType string
}

// volumeKey is used internally to track volume metadata during parsing
type volumeKey struct {
	Region     string
	VolumeType pricing.VolumeType
}

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
