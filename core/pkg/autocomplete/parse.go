package autocomplete

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

// FilterParser parses a filter query string for autocomplete requests.
type FilterParser func(filterString string) (filter.Filter, error)

// ParseOptions configures ParseRequest.
type ParseOptions struct {
	DefaultWindow   string
	DefaultTenantID string
	LabelConfig     *opencost.LabelConfig
	UTCOffset       *time.Duration
	WindowValidator WindowValidator
}

// ParseRequest builds a Request from query parameters.
func ParseRequest(qp httputil.QueryParams, opts ParseOptions, validateField FieldValidator, parseFilter FilterParser) (*Request, error) {
	windowStr := qp.Get("window", opts.DefaultWindow)
	if windowStr == "" {
		return nil, fmt.Errorf("%w: missing required 'window' parameter", ErrBadRequest)
	}

	var window opencost.Window
	var err error
	if opts.UTCOffset != nil {
		window, err = opencost.ParseWindowWithOffset(windowStr, *opts.UTCOffset)
	} else {
		window, err = opencost.ParseWindowUTC(windowStr)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: invalid window parameter: %w", ErrBadRequest, err)
	}

	windowValidator := opts.WindowValidator
	if windowValidator == nil {
		windowValidator = DefaultWindowValidator
	}
	if err := windowValidator(window); err != nil {
		return nil, err
	}

	field, err := validateField(qp.Get("field", ""))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid field: %w", ErrBadRequest, err)
	}

	filterString := qp.Get("filter", "")
	var parsedFilter filter.Filter = &ast.VoidOp{}
	if filterString != "" {
		if parseFilter == nil {
			return nil, fmt.Errorf("%w: invalid 'filter' parameter: filter parser is required", ErrBadRequest)
		}
		parsedFilter, err = parseFilter(filterString)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid 'filter' parameter: %w", ErrBadRequest, err)
		}
		if parsedFilter == nil {
			parsedFilter = &ast.VoidOp{}
		}
	}

	tenantID := qp.Get("tenantId", opts.DefaultTenantID)
	if tenantID == "" {
		tenantID = opts.DefaultTenantID
	}

	labelConfig := opts.LabelConfig
	if labelConfig == nil {
		labelConfig = opencost.NewLabelConfig()
	}

	return &Request{
		TenantID:    tenantID,
		Search:      SanitizeSearch(qp.Get("search", "")),
		Field:       field,
		Limit:       qp.GetInt("limit", 0),
		Window:      window,
		Filter:      parsedFilter,
		LabelConfig: labelConfig,
	}, nil
}
