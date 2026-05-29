package cloudcost

import (
	"fmt"

	cloudcostfilter "github.com/opencost/opencost/core/pkg/filter/cloudcost"
	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/autocomplete"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

// AutocompleteFilterParser parses a filter query string for autocomplete requests.
type AutocompleteFilterParser func(filterString string) (filter.Filter, error)

// ParseCloudCostAutocompleteOptions configures ParseCloudCostAutocompleteRequest.
type ParseCloudCostAutocompleteOptions struct {
	DefaultWindow string
}

// ParseCloudCostAutocompleteRequest builds a CloudCostAutocompleteRequest from query parameters.
func ParseCloudCostAutocompleteRequest(qp httputil.QueryParams, opts ParseCloudCostAutocompleteOptions, parseFilter AutocompleteFilterParser) (*CloudCostAutocompleteRequest, error) {
	windowStr := qp.Get("window", opts.DefaultWindow)
	if windowStr == "" {
		return nil, fmt.Errorf("%w: missing required 'window' parameter", ErrAutocompleteBadRequest)
	}

	window, err := opencost.ParseWindowUTC(windowStr)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid window parameter: %w", ErrAutocompleteBadRequest, err)
	}
	if window.IsOpen() {
		return nil, fmt.Errorf("%w: invalid window parameter: %s", ErrAutocompleteBadRequest, window.String())
	}

	field, err := ValidateAutocompleteField(qp.Get("field", ""))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid field: %w", ErrAutocompleteBadRequest, err)
	}

	filterString := qp.Get("filter", "")
	var parsedFilter filter.Filter
	if filterString != "" {
		if parseFilter == nil {
			parser := cloudcostfilter.NewCloudCostFilterParser()
			parsedFilter, err = parser.Parse(filterString)
		} else {
			parsedFilter, err = parseFilter(filterString)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: invalid 'filter' parameter: %w", ErrAutocompleteBadRequest, err)
		}
	}

	return &CloudCostAutocompleteRequest{
		Search: autocomplete.SanitizeSearch(qp.Get("search", "")),
		Field:  field,
		Limit:  qp.GetInt("limit", 0),
		Window: window,
		Filter: parsedFilter,
	}, nil
}

// ParseCloudCostAutocompleteRequestFromQueryParams parses cloud cost autocomplete query parameters with required window and field.
func ParseCloudCostAutocompleteRequestFromQueryParams(qp httputil.QueryParams) (*CloudCostAutocompleteRequest, error) {
	return ParseCloudCostAutocompleteRequest(qp, ParseCloudCostAutocompleteOptions{}, nil)
}
