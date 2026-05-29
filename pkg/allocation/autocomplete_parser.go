package allocation

import (
	"fmt"
	"time"

	allocationfilter "github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/autocomplete"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

// AutocompleteFilterParser parses a filter query string for autocomplete requests.
type AutocompleteFilterParser func(filterString string) (filter.Filter, error)

// ParseAllocationAutocompleteOptions configures ParseAllocationAutocompleteRequest.
type ParseAllocationAutocompleteOptions struct {
	DefaultWindow string
	LabelConfig   *opencost.LabelConfig
	// UTCOffset, when non-nil, parses window with ParseWindowWithOffset instead of UTC.
	UTCOffset *time.Duration
}

// ParseAllocationAutocompleteRequest builds an AllocationAutocompleteRequest from query parameters.
func ParseAllocationAutocompleteRequest(qp httputil.QueryParams, opts ParseAllocationAutocompleteOptions, parseFilter AutocompleteFilterParser) (*AllocationAutocompleteRequest, error) {
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
			parser := allocationfilter.NewAllocationFilterParser()
			parsedFilter, err = parser.Parse(filterString)
		} else {
			parsedFilter, err = parseFilter(filterString)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: invalid 'filter' parameter: %w", ErrAutocompleteBadRequest, err)
		}
	}

	labelConfig := opts.LabelConfig
	if labelConfig == nil {
		labelConfig = opencost.NewLabelConfig()
	}

	return &AllocationAutocompleteRequest{
		Search:      autocomplete.SanitizeSearch(qp.Get("search", "")),
		Field:       field,
		Limit:       qp.GetInt("limit", 0),
		Window:      window,
		Filter:      parsedFilter,
		LabelConfig: labelConfig,
	}, nil
}
