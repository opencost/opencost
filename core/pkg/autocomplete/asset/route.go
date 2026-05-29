package asset

import "github.com/opencost/opencost/core/pkg/autocomplete"

// Route describes how to query a normalized asset autocomplete field.
type Route int

const (
	RouteDefault Route = iota
	RouteLabelKeys
	RouteLabelValue
	RouteStaticType
	RouteStaticCategory
)

// RouteField maps a normalized field to a query route and label key when applicable.
func RouteField(field string) (Route, string, error) {
	switch field {
	case "type":
		return RouteStaticType, "", nil
	case "category":
		return RouteStaticCategory, "", nil
	}
	if kind, key, err := autocomplete.ParseLabelField(field, autocomplete.LabelPrefix); err == nil {
		switch kind {
		case autocomplete.LabelFieldKeys:
			return RouteLabelKeys, "", nil
		case autocomplete.LabelFieldValue:
			return RouteLabelValue, key, nil
		}
	}
	return RouteDefault, "", nil
}
