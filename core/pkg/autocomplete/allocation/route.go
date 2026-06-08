package allocation

import "github.com/opencost/opencost/core/pkg/autocomplete"

// Route describes how to query a normalized allocation autocomplete field.
type Route int

const (
	RouteDefault Route = iota
	RouteLabelKeys
	RouteLabelValue
	RouteNamespaceLabelKeys
	RouteNamespaceLabelValue
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
	return RouteDefault, "", nil
}
