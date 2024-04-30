package customcost

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

func ParseCustomCostTotalRequest(qp httputil.QueryParams) (*CostTotalRequest, error) {
	windowStr := qp.Get("window", "")
	if windowStr == "" {
		return nil, fmt.Errorf("missing require window param")
	}

	window, err := opencost.ParseWindowUTC(windowStr)
	if err != nil {
		return nil, fmt.Errorf("invalid window parameter: %w", err)
	}
	if window.IsOpen() {
		return nil, fmt.Errorf("invalid window parameter: %s", window.String())
	}

	aggregateByRaw := qp.GetList("aggregate", ",")
	aggregateBy, err := ParseCustomCostProperties(aggregateByRaw)
	if err != nil {
		return nil, err
	}

	var filter filter.Filter
	filterString := qp.Get("filter", "")
	if filterString != "" {
		parser := NewCustomCostFilterParser()
		filter, err = parser.Parse(filterString)
		if err != nil {
			return nil, fmt.Errorf("parsing 'filter' parameter: %s", err)
		}
	}

	opts := &CostTotalRequest{
		Start:       *window.Start(),
		End:         *window.End(),
		AggregateBy: aggregateBy,
		Filter:      filter,
	}

	return opts, nil
}

func ParseCustomCostTimeseriesRequest(qp httputil.QueryParams) (*CostTimeseriesRequest, error) {
	windowStr := qp.Get("window", "")
	if windowStr == "" {
		return nil, fmt.Errorf("missing require window param")
	}

	window, err := opencost.ParseWindowUTC(windowStr)
	if err != nil {
		return nil, fmt.Errorf("invalid window parameter: %w", err)
	}
	if window.IsOpen() {
		return nil, fmt.Errorf("invalid window parameter: %s", window.String())
	}

	aggregateByRaw := qp.GetList("aggregate", ",")
	aggregateBy, err := ParseCustomCostProperties(aggregateByRaw)
	if err != nil {
		return nil, err
	}

	accumulate := opencost.ParseAccumulate(qp.Get("accumulate", ""))

	var filter filter.Filter
	filterString := qp.Get("filter", "")
	if filterString != "" {
		parser := NewCustomCostFilterParser()
		filter, err = parser.Parse(filterString)
		if err != nil {
			return nil, fmt.Errorf("parsing 'filter' parameter: %s", err)
		}
	}

	opts := &CostTimeseriesRequest{
		Start:       *window.Start(),
		End:         *window.End(),
		AggregateBy: aggregateBy,
		Accumulate:  accumulate,
		Filter:      filter,
	}

	return opts, nil
}
