package allocation

import (
	"github.com/opencost/opencost/core/pkg/util/autocomplete"
)

// AllocationAutocompleteRoute describes how to query a normalized allocation autocomplete field.
type AllocationAutocompleteRoute int

const (
	AllocationAutocompleteRouteField AllocationAutocompleteRoute = iota
	AllocationAutocompleteRouteLabelKeys
	AllocationAutocompleteRouteLabelValue
	AllocationAutocompleteRouteNamespaceLabelKeys
	AllocationAutocompleteRouteNamespaceLabelValue
)

// RouteAllocationAutocompleteField maps a normalized field to a query route and label key when applicable.
func RouteAllocationAutocompleteField(field string) (AllocationAutocompleteRoute, string, error) {
	if kind, key, err := autocomplete.ParseLabelField(field, autocomplete.LabelPrefix); err == nil {
		switch kind {
		case autocomplete.LabelFieldKeys:
			return AllocationAutocompleteRouteLabelKeys, "", nil
		case autocomplete.LabelFieldValue:
			return AllocationAutocompleteRouteLabelValue, key, nil
		}
	}
	if kind, key, err := autocomplete.ParseLabelField(field, autocomplete.NamespaceLabelPrefix); err == nil {
		switch kind {
		case autocomplete.LabelFieldKeys:
			return AllocationAutocompleteRouteNamespaceLabelKeys, "", nil
		case autocomplete.LabelFieldValue:
			return AllocationAutocompleteRouteNamespaceLabelValue, key, nil
		}
	}
	return AllocationAutocompleteRouteField, "", nil
}
