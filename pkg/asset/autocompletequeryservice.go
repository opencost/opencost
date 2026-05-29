package asset

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/autocomplete"
)

// ErrAutocompleteBadRequest indicates a client error in an autocomplete request.
var ErrAutocompleteBadRequest = errors.New("autocomplete bad request")

// IsAutocompleteBadRequest reports whether err is a client validation error.
func IsAutocompleteBadRequest(err error) bool {
	return errors.Is(err, ErrAutocompleteBadRequest)
}

const DefaultAutocompleteResultLimit = 100
const MaxAutocompleteResultLimit = 1000

type AssetAutocompleteRequest struct {
	TenantID string
	Search   string
	Field    string
	Limit    int
	Window   opencost.Window
	Filter   filter.Filter
}

type AssetAutocompleteResponse struct {
	Data []string `json:"data"`
}

type AutocompleteQueryService interface {
	QueryAssetAutocomplete(AssetAutocompleteRequest, context.Context) (*AssetAutocompleteResponse, error)
}

func QueryAssetAutocompleteFromSet(assetSet *opencost.AssetSet, req AssetAutocompleteRequest) (*AssetAutocompleteResponse, error) {
	field, err := NormalizeAssetAutocompleteRequest(&req)
	if err != nil {
		return nil, err
	}

	route, labelKey, err := RouteAssetAutocompleteField(field)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAutocompleteBadRequest, err)
	}

	switch route {
	case AssetAutocompleteRouteStaticType:
		return &AssetAutocompleteResponse{Data: autocomplete.UniqueSortedLimited(
			toSet(FilterStaticAutocompleteValues(StaticAssetTypes(), req.Search)),
			req.Limit,
		)}, nil
	case AssetAutocompleteRouteStaticCategory:
		return &AssetAutocompleteResponse{Data: autocomplete.UniqueSortedLimited(
			toSet(FilterStaticAutocompleteValues(StaticAssetCategories(), req.Search)),
			req.Limit,
		)}, nil
	case AssetAutocompleteRouteLabelKeys, AssetAutocompleteRouteLabelValue:
		// in-memory path handles labels below via field string
		_ = labelKey
	}

	var matcher opencost.AssetMatcher
	if req.Filter != nil {
		compiler := opencost.NewAssetMatchCompiler()
		matcher, err = compiler.Compile(req.Filter)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to compile filter: %w", ErrAutocompleteBadRequest, err)
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

	return &AssetAutocompleteResponse{Data: autocomplete.UniqueSortedLimited(results, req.Limit)}, nil
}

// ValidateAutocompleteField normalizes and validates an asset autocomplete field name.
func ValidateAutocompleteField(field string) (string, error) {
	f := strings.ToLower(field)
	switch f {
	case "account", "cluster", "name", "provider", "providerid", "type", "assettype", "category":
		if f == "assettype" {
			return "type", nil
		}
		return f, nil
	}
	if f == "label" {
		return f, nil
	}
	if strings.HasPrefix(f, "label:") {
		_, labelKey, _ := strings.Cut(field, ":")
		return autocomplete.FormatLabelValueField(autocomplete.LabelPrefix, labelKey), nil
	}
	return "", fmt.Errorf("unrecognized field: %s", field)
}

func validateAssetAutocompleteWindow(window opencost.Window) error {
	if window.IsOpen() {
		return fmt.Errorf("%w: invalid window: %s", ErrAutocompleteBadRequest, window.String())
	}
	if window.Start() == nil || window.End() == nil {
		return fmt.Errorf("%w: invalid window: missing start or end", ErrAutocompleteBadRequest)
	}
	return nil
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

func toSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, v := range values {
		out[v] = struct{}{}
	}
	return out
}
