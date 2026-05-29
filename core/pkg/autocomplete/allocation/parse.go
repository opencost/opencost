package allocation

import (
	"fmt"
	"time"

	allocationfilter "github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

// FilterParser parses a filter query string for autocomplete requests.
type FilterParser func(filterString string) (filter.Filter, error)

// ParseOptions configures ParseRequest.
type ParseOptions struct {
	DefaultWindow string
	LabelConfig   *opencost.LabelConfig
	UTCOffset     *time.Duration
}

// ParseRequest builds an AutocompleteRequest from query parameters.
func ParseRequest(qp httputil.QueryParams, opts ParseOptions, parseFilter FilterParser) (*AutocompleteRequest, error) {
	windowStr := qp.Get("window", opts.DefaultWindow)
	if windowStr == "" {
		return nil, fmt.Errorf("%w: missing required 'window' parameter", autocomplete.ErrBadRequest)
	}

	var window opencost.Window
	var err error
	if opts.UTCOffset != nil {
		window, err = opencost.ParseWindowWithOffset(windowStr, *opts.UTCOffset)
	} else {
		window, err = opencost.ParseWindowUTC(windowStr)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: invalid window parameter: %w", autocomplete.ErrBadRequest, err)
	}
	if window.IsOpen() {
		return nil, fmt.Errorf("%w: invalid window parameter: %s", autocomplete.ErrBadRequest, window.String())
	}

	field, err := ValidateField(qp.Get("field", ""))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid field: %w", autocomplete.ErrBadRequest, err)
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
			return nil, fmt.Errorf("%w: invalid 'filter' parameter: %w", autocomplete.ErrBadRequest, err)
		}
	}

	labelConfig := opts.LabelConfig
	if labelConfig == nil {
		labelConfig = opencost.NewLabelConfig()
	}

	return &AutocompleteRequest{
		Search:      autocomplete.SanitizeSearch(qp.Get("search", "")),
		Field:       field,
		Limit:       qp.GetInt("limit", 0),
		Window:      window,
		Filter:      parsedFilter,
		LabelConfig: labelConfig,
	}, nil
}
