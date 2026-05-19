package asset

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
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
	if req.TenantID == "" {
		return nil, fmt.Errorf("%w: tenant ID is required", ErrAutocompleteBadRequest)
	}

	field, err := validateAutocompleteField(req.Field)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid field: %w", ErrAutocompleteBadRequest, err)
	}

	limit := req.Limit
	if limit <= 0 {
		limit = DefaultAutocompleteResultLimit
	}
	if limit > MaxAutocompleteResultLimit {
		return nil, fmt.Errorf("%w: exceeded maximum autocomplete result limit of %d", ErrAutocompleteBadRequest, MaxAutocompleteResultLimit)
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

	data := make([]string, 0, len(results))
	for value := range results {
		data = append(data, value)
	}
	sort.Strings(data)
	if len(data) > limit {
		data = data[:limit]
	}
	return &AssetAutocompleteResponse{Data: data}, nil
}

func validateAutocompleteField(field string) (string, error) {
	f := strings.ToLower(field)
	switch f {
	case "account", "cluster", "name", "provider", "providerid", "type", "category":
		return f, nil
	}
	if strings.HasPrefix(f, "label") {
		return f, nil
	}
	return "", fmt.Errorf("unrecognized field: %s", field)
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
		if value, ok := asset.GetLabels()[labelName]; ok {
			return []string{value}
		}
	}
	return nil
}
