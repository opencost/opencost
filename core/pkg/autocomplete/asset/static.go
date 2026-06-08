package asset

import (
	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// StaticTypes returns canonical asset type strings for autocomplete.
func StaticTypes() []string {
	return []string{
		"cloud",
		"clustermanagement",
		"disk",
		"loadbalancer",
		"network",
		"node",
		"shared",
	}
}

// StaticCategories returns canonical asset category strings for autocomplete.
func StaticCategories() []string {
	return []string{
		opencost.ComputeCategory,
		opencost.StorageCategory,
		opencost.NetworkCategory,
		opencost.ManagementCategory,
	}
}

// FilterStaticValues filters static enumeration values by search text.
func FilterStaticValues(values []string, search string) []string {
	return autocomplete.FilterBySearch(values, search)
}
