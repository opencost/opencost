package asset

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	coreasset "github.com/opencost/opencost/core/pkg/autocomplete/asset"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func QueryAssetAutocompleteFromSet(assetSet *opencost.AssetSet, req autocomplete.Request) (*autocomplete.Response, error) {
	field, err := autocomplete.NormalizeRequest(&req, coreasset.ValidateField, autocomplete.NormalizeOptions{
		RequireTenantID: true,
		WindowValidator: coreasset.ValidateWindow,
	})
	if err != nil {
		return nil, err
	}

	route, _, err := coreasset.RouteField(field)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", autocomplete.ErrBadRequest, err)
	}

	switch route {
	case coreasset.RouteStaticType:
		return &autocomplete.Response{Data: autocomplete.UniqueSortedLimited(
			autocomplete.ToSet(coreasset.FilterStaticValues(coreasset.StaticTypes(), req.Search)),
			req.Limit,
		)}, nil
	case coreasset.RouteStaticCategory:
		return &autocomplete.Response{Data: autocomplete.UniqueSortedLimited(
			autocomplete.ToSet(coreasset.FilterStaticValues(coreasset.StaticCategories(), req.Search)),
			req.Limit,
		)}, nil
	}

	var matcher opencost.AssetMatcher
	if autocomplete.HasFilter(req.Filter) {
		compiler := opencost.NewAssetMatchCompiler()
		matcher, err = compiler.Compile(req.Filter)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to compile filter: %w", autocomplete.ErrBadRequest, err)
		}
	}

	search := strings.ToLower(req.Search)
	results := map[string]struct{}{}
	for _, a := range assetSet.Assets {
		if a == nil {
			continue
		}
		if matcher != nil && !matcher.Matches(a) {
			continue
		}

		values := assetAutocompleteValues(a, field)
		for _, value := range values {
			if value == "" {
				continue
			}
			if search != "" && !strings.Contains(strings.ToLower(value), search) {
				continue
			}
			results[value] = struct{}{}
		}
	}

	return &autocomplete.Response{Data: autocomplete.UniqueSortedLimited(results, req.Limit)}, nil
}

func assetAutocompleteValues(asset opencost.Asset, field string) []string {
	props := asset.GetProperties()
	if props == nil {
		return nil
	}
	switch {
	case field == "account":
		return []string{props.Account}
	case field == "cluster":
		return []string{props.Cluster}
	case field == "name":
		return []string{props.Name}
	case field == "provider":
		return []string{props.Provider}
	case field == "providerid":
		return []string{props.ProviderID}
	case field == "type":
		return []string{asset.Type().String()}
	case field == "category":
		return []string{props.Category}
	case field == "label":
		keys := make([]string, 0, len(asset.GetLabels()))
		for key := range asset.GetLabels() {
			keys = append(keys, key)
		}
		return keys
	case strings.HasPrefix(field, "label:"):
		labelName := strings.TrimPrefix(field, "label:")
		if value, ok := autocomplete.MapValueFold(asset.GetLabels(), labelName); ok {
			return []string{value}
		}
	}
	return nil
}
