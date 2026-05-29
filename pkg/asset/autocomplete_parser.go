package asset

import (
	"fmt"
	"time"

	assetfilter "github.com/opencost/opencost/core/pkg/filter/asset"
	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/autocomplete"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

// AutocompleteFilterParser parses a filter query string for autocomplete requests.
type AutocompleteFilterParser func(filterString string) (filter.Filter, error)

// ParseAssetAutocompleteOptions configures ParseAssetAutocompleteRequest.
type ParseAssetAutocompleteOptions struct {
	DefaultWindow   string
	DefaultTenantID string
	// UTCOffset, when non-nil, parses window with ParseWindowWithOffset instead of UTC.
	UTCOffset *time.Duration
}

// ParseAssetAutocompleteRequest builds an AssetAutocompleteRequest from query parameters.
func ParseAssetAutocompleteRequest(qp httputil.QueryParams, opts ParseAssetAutocompleteOptions, parseFilter AutocompleteFilterParser) (*AssetAutocompleteRequest, error) {
	windowStr := qp.Get("window", opts.DefaultWindow)
	if windowStr == "" {
		return nil, fmt.Errorf("%w: missing required 'window' parameter", ErrAutocompleteBadRequest)
	}

	var window opencost.Window
	var err error
	if opts.UTCOffset != nil {
		window, err = opencost.ParseWindowWithOffset(windowStr, *opts.UTCOffset)
	} else {
		window, err = opencost.ParseWindowUTC(windowStr)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: invalid window parameter: %w", ErrAutocompleteBadRequest, err)
	}
	if err := validateAssetAutocompleteWindow(window); err != nil {
		return nil, err
	}

	field, err := ValidateAutocompleteField(qp.Get("field", ""))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid field: %w", ErrAutocompleteBadRequest, err)
	}

	filterString := qp.Get("filter", "")
	var parsedFilter filter.Filter
	if filterString != "" {
		if parseFilter == nil {
			parser := assetfilter.NewAssetFilterParser()
			parsedFilter, err = parser.Parse(filterString)
		} else {
			parsedFilter, err = parseFilter(filterString)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: invalid 'filter' parameter: %w", ErrAutocompleteBadRequest, err)
		}
	}

	tenantID := qp.Get("tenantId", opts.DefaultTenantID)
	if tenantID == "" {
		tenantID = opts.DefaultTenantID
	}

	return &AssetAutocompleteRequest{
		TenantID: tenantID,
		Search:   autocomplete.SanitizeSearch(qp.Get("search", "")),
		Field:    field,
		Limit:    qp.GetInt("limit", 0),
		Window:   window,
		Filter:   parsedFilter,
	}, nil
}
