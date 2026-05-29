package asset

import (
	"strings"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/autocomplete"
)

// StaticAssetTypes returns the canonical asset type strings used for autocomplete.
func StaticAssetTypes() []string {
	return []string{
		"clustermanagement",
		"disk",
		"loadbalancer",
		"network",
		"node",
	}
}

// StaticAssetCategories returns the canonical asset category strings used for autocomplete.
func StaticAssetCategories() []string {
	return []string{
		strings.ToLower(opencost.ComputeCategory),
		strings.ToLower(opencost.StorageCategory),
		strings.ToLower(opencost.NetworkCategory),
		strings.ToLower(opencost.ManagementCategory),
	}
}

// FilterStaticAutocompleteValues filters static enumeration values by search text.
func FilterStaticAutocompleteValues(values []string, search string) []string {
	return autocomplete.FilterBySearch(values, search)
}
