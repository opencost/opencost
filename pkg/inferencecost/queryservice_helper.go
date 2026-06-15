package inferencecost

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/mapper"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// QueryRequest holds the parsed parameters for an inference cost query.
type QueryRequest struct {
	Start      time.Time
	End        time.Time
	CostBasis  CostBasis
	AggregateBy []string
	Accumulate opencost.AccumulateOption
	Filter     []filterSpec
	Step       time.Duration
}

// ParseInferenceCostRequest parses the common query parameters from an HTTP
// request into a QueryRequest. It mirrors the pattern used by
// customcost.ParseCustomCostTotalRequest.
//
// Required: window (RFC3339 start,end or named range).
// Optional: costBasis (default: allocation), aggregate, accumulate, filter.
//
// The step field is derived from the accumulate option.
func ParseInferenceCostRequest(qp mapper.PrimitiveMap) (*QueryRequest, error) {
	// --- window (required) ---
	windowStr := qp.Get("window", "")
	if windowStr == "" {
		return nil, fmt.Errorf("missing required 'window' parameter")
	}
	window, err := opencost.ParseWindowUTC(windowStr)
	if err != nil {
		return nil, fmt.Errorf("invalid 'window' parameter: %w", err)
	}
	if window.IsOpen() {
		return nil, fmt.Errorf("invalid 'window' parameter: window must have both start and end: %s", windowStr)
	}

	// --- costBasis (optional, default: allocation) ---
	basisStr := qp.Get("costBasis", string(CostBasisAllocation))
	var basis CostBasis
	switch basisStr {
	case string(CostBasisUsage):
		basis = CostBasisUsage
	case string(CostBasisAllocation):
		basis = CostBasisAllocation
	default:
		return nil, fmt.Errorf("invalid 'costBasis' parameter %q: must be %q or %q", basisStr, CostBasisUsage, CostBasisAllocation)
	}

	// --- aggregate (optional) ---
	aggregateByRaw := qp.GetList("aggregate", ",")
	for _, dim := range aggregateByRaw {
		if !supportedAggregateProperties[dim] {
			return nil, fmt.Errorf("unsupported 'aggregate' dimension %q: Phase 1 supports model_name, model_version, namespace, cluster", dim)
		}
	}

	// --- accumulate (optional, defaults to none) ---
	accumulate := opencost.ParseAccumulate(qp.Get("accumulate", ""))

	// --- filter (optional) ---
	filterSpecs, err := parseFilter(qp.Get("filter", ""))
	if err != nil {
		return nil, fmt.Errorf("invalid 'filter' parameter: %w", err)
	}

	// Derive step from accumulate. When no accumulate is requested the step
	// spans the full window (produces a single data point).
	step := stepFromAccumulate(accumulate, *window.Start(), *window.End())

	return &QueryRequest{
		Start:       *window.Start(),
		End:         *window.End(),
		CostBasis:   basis,
		AggregateBy: aggregateByRaw,
		Accumulate:  accumulate,
		Filter:      filterSpecs,
		Step:        step,
	}, nil
}

// ParseInferenceCostTimeseriesRequest is identical to ParseInferenceCostRequest
// but requires the accumulate parameter (timeseries must have a defined step).
func ParseInferenceCostTimeseriesRequest(qp mapper.PrimitiveMap) (*QueryRequest, error) {
	req, err := ParseInferenceCostRequest(qp)
	if err != nil {
		return nil, err
	}
	if qp.Get("accumulate", "") == "" {
		return nil, fmt.Errorf("missing required 'accumulate' parameter for timeseries endpoint: must be one of hour, day, week, month")
	}
	if req.Accumulate == opencost.AccumulateOptionNone {
		return nil, fmt.Errorf("invalid 'accumulate' parameter for timeseries endpoint: must be one of hour, day, week, month")
	}
	return req, nil
}

// stepFromAccumulate returns the time.Duration that corresponds to an
// AccumulateOption. When AccumulateOptionNone is given (or not recognised),
// the full window duration is used so the loop produces a single pass.
//
// This is a local reimplementation of the package-private helpers in
// pkg/costmodel (resolveStepFromQuery / resolveStepForAccumulate) — kept here
// to preserve the domain package's self-containment.
func stepFromAccumulate(opt opencost.AccumulateOption, start, end time.Time) time.Duration {
	switch opt {
	case opencost.AccumulateOptionHour:
		return time.Hour
	case opencost.AccumulateOptionDay:
		return timeutil.Day
	case opencost.AccumulateOptionWeek:
		return timeutil.Week
	case opencost.AccumulateOptionMonth:
		// Steps by 30 days (consistent with
		// how OpenCost's CustomCost and CloudCost handle monthly accumulation).
		return timeutil.Day * 30
	default:
		// No accumulation: single window covering the full request range.
		d := end.Sub(start)
		if d <= 0 {
			d = timeutil.Day
		}
		return d
	}
}
