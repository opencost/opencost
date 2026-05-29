package asset

import (
	"strings"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// StaticTypes returns canonical asset type strings for autocomplete.
func StaticTypes() []string {
	return []string{
		"clustermanagement",
		"disk",
		"loadbalancer",
		"network",
		"node",
	}
}

// StaticCategories returns canonical asset category strings for autocomplete.
func StaticCategories() []string {
	return []string{
		strings.ToLower(opencost.ComputeCategory),
		strings.ToLower(opencost.StorageCategory),
		strings.ToLower(opencost.NetworkCategory),
		strings.ToLower(opencost.ManagementCategory),
	}
}

// FilterStaticValues filters static enumeration values by search text.
func FilterStaticValues(values []string, search string) []string {
	return autocomplete.FilterBySearch(values, search)
}
