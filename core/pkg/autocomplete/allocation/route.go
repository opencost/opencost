package allocation

import (
	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// Route describes how to query a normalized allocation autocomplete field.
type Route int

const (
	RouteDefault Route = iota
	RouteLabelKeys
	RouteLabelValue
	RouteNamespaceLabelKeys
	RouteNamespaceLabelValue
	// RouteAlias is an aliased label field (department, environment, owner,
	// product, team). The returned key is the alias name; implementations
	// resolve it to configured label keys via the request's LabelConfig.
	RouteAlias
)

// RouteField maps a normalized field to a query route and label key when applicable.
func RouteField(field string) (Route, string, error) {
	if kind, key, err := autocomplete.ParseLabelField(field, autocomplete.LabelPrefix); err == nil {
		switch kind {
		case autocomplete.LabelFieldKeys:
			return RouteLabelKeys, "", nil
		case autocomplete.LabelFieldValue:
			return RouteLabelValue, key, nil
		}
	}
	if kind, key, err := autocomplete.ParseLabelField(field, autocomplete.NamespaceLabelPrefix); err == nil {
		switch kind {
		case autocomplete.LabelFieldKeys:
			return RouteNamespaceLabelKeys, "", nil
		case autocomplete.LabelFieldValue:
			return RouteNamespaceLabelValue, key, nil
		}
	}
	if prop := opencost.AllocationProperty(field); prop.IsAliasedLabel() {
		return RouteAlias, field, nil
	}
	return RouteDefault, "", nil
}
