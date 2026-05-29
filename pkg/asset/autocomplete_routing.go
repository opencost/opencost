package asset

import (
	"github.com/opencost/opencost/core/pkg/util/autocomplete"
)

// AssetAutocompleteRoute describes how to query a normalized asset autocomplete field.
type AssetAutocompleteRoute int

const (
	AssetAutocompleteRouteField AssetAutocompleteRoute = iota
	AssetAutocompleteRouteLabelKeys
	AssetAutocompleteRouteLabelValue
	AssetAutocompleteRouteStaticType
	AssetAutocompleteRouteStaticCategory
)

// RouteAssetAutocompleteField maps a normalized field to a query route and label key when applicable.
func RouteAssetAutocompleteField(field string) (AssetAutocompleteRoute, string, error) {
	switch field {
	case "type":
		return AssetAutocompleteRouteStaticType, "", nil
	case "category":
		return AssetAutocompleteRouteStaticCategory, "", nil
	}
	if kind, key, err := autocomplete.ParseLabelField(field, autocomplete.LabelPrefix); err == nil {
		switch kind {
		case autocomplete.LabelFieldKeys:
			return AssetAutocompleteRouteLabelKeys, "", nil
		case autocomplete.LabelFieldValue:
			return AssetAutocompleteRouteLabelValue, key, nil
		}
	}
	return AssetAutocompleteRouteField, "", nil
}
