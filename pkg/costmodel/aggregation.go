package costmodel

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/env"
)

const (
	// SplitTypeWeighted signals that shared costs should be shared
	// proportionally, rather than evenly
	SplitTypeWeighted = "weighted"

	// UnallocatedSubfield indicates an allocation datum that does not have the
	// chosen Aggregator; e.g. during aggregation by some label, there may be
	// cost data that do not have the given label.
	UnallocatedSubfield = "__unallocated__"
)

// ParseAggregationProperties attempts to parse and return aggregation properties
// encoded under the given key. If none exist, or if parsing fails, an error
// is returned with empty AllocationProperties.
func ParseAggregationProperties(aggregations []string) ([]string, error) {
	aggregateBy := []string{}
	// In case of no aggregation option, aggregate to the container, with a key Cluster/Node/Namespace/Pod/Container
	if len(aggregations) == 0 {
		aggregateBy = []string{
			opencost.AllocationClusterProp,
			opencost.AllocationNodeProp,
			opencost.AllocationNamespaceProp,
			opencost.AllocationPodProp,
			opencost.AllocationContainerProp,
		}
	} else if len(aggregations) == 1 && aggregations[0] == "all" {
		aggregateBy = []string{}
	} else {
		for _, agg := range aggregations {
			aggregate := strings.TrimSpace(agg)
			if aggregate != "" {
				if prop, err := opencost.ParseProperty(aggregate); err == nil {
					aggregateBy = append(aggregateBy, string(prop))
				} else if strings.HasPrefix(aggregate, "label:") {
					aggregateBy = append(aggregateBy, aggregate)
				} else if strings.HasPrefix(aggregate, "annotation:") {
					aggregateBy = append(aggregateBy, aggregate)
				}
			}
		}
	}
	return aggregateBy, nil
}

func resolveAccumulateOption(accumulate opencost.AccumulateOption, accumulateBy string) (opencost.AccumulateOption, error) {
	accumulateByRaw := strings.TrimSpace(strings.ToLower(accumulateBy))
	if accumulateByRaw == "" {
		return accumulate, nil
	}

	if accumulateByRaw == "all" {
		return opencost.AccumulateOptionAll, nil
	}

	if accumulateByRaw == "none" {
		return opencost.AccumulateOptionNone, nil
	}

	accumulateByOpt := opencost.ParseAccumulate(accumulateByRaw)
	if accumulateByOpt == opencost.AccumulateOptionNone {
		return opencost.AccumulateOptionNone, fmt.Errorf("invalid accumulateBy option: %s", accumulateBy)
	}

	return accumulateByOpt, nil
}

func resolveAccumulateFromQuery(qp httputil.QueryParams) opencost.AccumulateOption {
	rawAccumulate := strings.TrimSpace(qp.Get("accumulate", ""))
	if strings.EqualFold(rawAccumulate, string(opencost.AccumulateOptionAll)) {
		return opencost.AccumulateOptionAll
	}

	accumulate := opencost.ParseAccumulate(rawAccumulate)
	if accumulate == opencost.AccumulateOptionNone && qp.GetBool("accumulate", false) {
		return opencost.AccumulateOptionAll
	}

	return accumulate
}

func resolveStepForAccumulate(step time.Duration, accumulateBy opencost.AccumulateOption) time.Duration {
	const (
		day  = 24 * time.Hour
		week = 7 * day
	)

	switch accumulateBy {
	case opencost.AccumulateOptionHour:
		return time.Hour
	case opencost.AccumulateOptionDay:
		// day accumulation supports either hourly or already-daily sets
		if step == day {
			return day
		}
		return time.Hour
	case opencost.AccumulateOptionWeek, opencost.AccumulateOptionMonth, opencost.AccumulateOptionQuarter:
		// week accumulation supports either daily or already-weekly sets
		if accumulateBy == opencost.AccumulateOptionWeek && step == week {
			return week
		}
		return day
	default:
		return step
	}
}

func resolveDefaultStepFromAccumulate(window opencost.Window, accumulateBy opencost.AccumulateOption) time.Duration {
	switch accumulateBy {
	case opencost.AccumulateOptionHour:
		return time.Hour
	case opencost.AccumulateOptionDay:
		return 24 * time.Hour
	case opencost.AccumulateOptionWeek:
		return 7 * 24 * time.Hour
	case opencost.AccumulateOptionMonth, opencost.AccumulateOptionQuarter:
		// month/quarter accumulation requires daily input sets
		return 24 * time.Hour
	case opencost.AccumulateOptionAll:
		return window.Duration()
	default:
		return window.Duration()
	}
}

func resolveStepFromQuery(qp httputil.QueryParams, window opencost.Window, accumulateBy opencost.AccumulateOption) (time.Duration, error) {
	stepRaw := strings.TrimSpace(strings.ToLower(qp.Get("step", "")))
	if stepRaw == "" {
		step := resolveDefaultStepFromAccumulate(window, accumulateBy)
		return resolveStepForAccumulate(step, accumulateBy), nil
	}

	switch stepRaw {
	case "hour":
		return resolveStepForAccumulate(time.Hour, accumulateBy), nil
	case "day":
		return resolveStepForAccumulate(24*time.Hour, accumulateBy), nil
	case "week":
		return resolveStepForAccumulate(7*24*time.Hour, accumulateBy), nil
	case "month":
		// month accumulation operates on daily inputs and calendar-rounded query windows
		return resolveStepForAccumulate(24*time.Hour, accumulateBy), nil
	case "quarter":
		// quarter accumulation operates on daily inputs and calendar-rounded query windows
		return resolveStepForAccumulate(24*time.Hour, accumulateBy), nil
	default:
		step, err := timeutil.ParseDuration(stepRaw)
		if err != nil {
			return 0, fmt.Errorf("invalid step %q: must be a Go duration or one of hour, day, week, month, quarter: %w", stepRaw, err)
		}
		return resolveStepForAccumulate(step, accumulateBy), nil
	}
}

func resolveQueryWindowForAccumulate(window opencost.Window, accumulateBy opencost.AccumulateOption) (opencost.Window, error) {
	switch accumulateBy {
	case opencost.AccumulateOptionHour, opencost.AccumulateOptionDay, opencost.AccumulateOptionWeek, opencost.AccumulateOptionMonth, opencost.AccumulateOptionQuarter:
		windows, err := window.GetAccumulateWindows(accumulateBy)
		if err != nil {
			return opencost.Window{}, err
		}
		if len(windows) == 0 {
			return opencost.Window{}, fmt.Errorf("no query windows for accumulate option %s", accumulateBy)
		}

		return opencost.NewClosedWindow(*windows[0].Start(), *windows[len(windows)-1].End()), nil
	default:
		return window, nil
	}
}

func trimAllocationSetRangeToRequestWindow(asr *opencost.AllocationSetRange, requestWindow opencost.Window) *opencost.AllocationSetRange {
	if asr == nil {
		return nil
	}

	trimmed := opencost.NewAllocationSetRange()
	trimmed.FromStore = asr.FromStore
	for _, as := range asr.Allocations {
		// Keep only sets that overlap the originally requested window.
		if as.Start().Before(*requestWindow.End()) && as.End().After(*requestWindow.Start()) {
			trimmed.Append(as)
		}
	}

	return trimmed
}

func (a *Accesses) ComputeAllocationHandlerSummary(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	qp := httputil.NewQueryParams(r.URL.Query())

	// Window is a required field describing the window of time over which to
	// compute allocation data.
	window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
	}

	// Step is an optional parameter that defines the duration per-set, i.e.
	// the window for an AllocationSet, of the AllocationSetRange to be
	// computed. Defaults to the window size, making one set.
	// Aggregation is a required comma-separated list of fields by which to
	// aggregate results. Some fields allow a sub-field, which is distinguished
	// with a colon; e.g. "label:app".
	// Examples: "namespace", "namespace,label:app"
	aggregations := qp.GetList("aggregate", ",")
	aggregateBy, err := ParseAggregationProperties(aggregations)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'aggregate' parameter: %s", err), http.StatusBadRequest)
	}

	// Accumulate is an optional parameter that accepts bool-style values (e.g.
	// true/1) or options (e.g. day/week/month) and governs accumulation windowing.
	accumulateOpt := resolveAccumulateFromQuery(qp)
	accumulateBy, err := resolveAccumulateOption(accumulateOpt, qp.Get("accumulateBy", ""))
	if err != nil {
		proto.WriteError(w, proto.BadRequest(fmt.Sprintf("Invalid 'accumulateBy' parameter: %s", err)))
		return
	}
	step, err := resolveStepFromQuery(qp, window, accumulateBy)
	if err != nil {
		proto.WriteError(w, proto.BadRequest(fmt.Sprintf("Invalid step parameter: %s", err)))
		return
	}
	queryWindow, err := resolveQueryWindowForAccumulate(window, accumulateBy)
	if err != nil {
		proto.WriteError(w, proto.BadRequest(fmt.Sprintf("Invalid accumulation configuration: %s", err)))
		return
	}

	// Get allocation filter if provided
	allocationFilter := qp.Get("filter", "")

	// Query for AllocationSets in increments of the given step duration,
	// appending each to the AllocationSetRange.
	asr := opencost.NewAllocationSetRange()
	stepStart := *queryWindow.Start()
	for queryWindow.End().After(stepStart) {
		stepEnd := stepStart.Add(step)
		stepWindow := opencost.NewWindow(&stepStart, &stepEnd)

		as, err := a.Model.ComputeAllocation(*stepWindow.Start(), *stepWindow.End())
		if err != nil {
			proto.WriteError(w, proto.InternalServerError(err.Error()))
			return
		}
		asr.Append(as)

		stepStart = stepEnd
	}

	// Apply allocation filter if provided
	if allocationFilter != "" {
		parser := allocation.NewAllocationFilterParser()
		filterNode, err := parser.Parse(allocationFilter)
		if err != nil {
			proto.WriteError(w, proto.BadRequest(fmt.Sprintf("Invalid filter: %s", err)))
			return
		}
		compiler := opencost.NewAllocationMatchCompiler(nil)
		matcher, err := compiler.Compile(filterNode)
		if err != nil {
			proto.WriteError(w, proto.BadRequest(fmt.Sprintf("Failed to compile filter: %s", err)))
			return
		}
		filteredASR := opencost.NewAllocationSetRange()
		for _, as := range asr.Allocations {
			filteredAS := opencost.NewAllocationSet(as.Start(), as.End())
			for _, alloc := range as.Allocations {
				if matcher.Matches(alloc) {
					filteredAS.Set(alloc)
				}
			}
			if filteredAS.Length() > 0 {
				filteredASR.Append(filteredAS)
			}
		}
		asr = filteredASR
	}

	// Aggregate, if requested
	if len(aggregateBy) > 0 {
		err = asr.AggregateBy(aggregateBy, nil)
		if err != nil {
			proto.WriteError(w, proto.InternalServerError(err.Error()))
			return
		}
	}

	// Accumulate, if requested
	if accumulateBy != opencost.AccumulateOptionNone {
		asr, err = asr.Accumulate(accumulateBy)
		if err != nil {
			proto.WriteError(w, proto.InternalServerError(err.Error()))
			return
		}

		asr = trimAllocationSetRangeToRequestWindow(asr, window)
	}

	sasl := []*opencost.SummaryAllocationSet{}
	for _, as := range asr.Allocations {
		sas := opencost.NewSummaryAllocationSet(as, nil, nil, false, false)
		sasl = append(sasl, sas)
	}
	sasr := opencost.NewSummaryAllocationSetRange(sasl...)

	WriteData(w, sasr, nil)
}

// ComputeAllocationHandler computes an AllocationSetRange from the CostModel.
func (a *Accesses) ComputeAllocationHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	qp := httputil.NewQueryParams(r.URL.Query())

	// Window is a required field describing the window of time over which to
	// compute allocation data.
	window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
	}

	// Step is an optional parameter that defines the duration per-set, i.e.
	// the window for an AllocationSet, of the AllocationSetRange to be
	// computed. Defaults to the window size, making one set.
	// Aggregation is an optional comma-separated list of fields by which to
	// aggregate results. Some fields allow a sub-field, which is distinguished
	// with a colon; e.g. "label:app".
	// Examples: "namespace", "namespace,label:app"
	aggregations := qp.GetList("aggregate", ",")
	aggregateBy, err := ParseAggregationProperties(aggregations)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'aggregate' parameter: %s", err), http.StatusBadRequest)
	}

	// IncludeIdle, if true, uses Asset data to incorporate Idle Allocation
	includeIdle := qp.GetBool("includeIdle", false)
	// Accumulate is an optional parameter that accepts bool-style values (e.g.
	// true/1) or options (e.g. day/week/month) and governs accumulation windowing.
	accumulateOpt := resolveAccumulateFromQuery(qp)

	// AccumulateBy is an optional parameter that overrides accumulate with an
	// explicit accumulation option (e.g. all/day/week/month/quarter/none).
	accumulateBy, err := resolveAccumulateOption(accumulateOpt, qp.Get("accumulateBy", ""))
	if err != nil {
		proto.WriteError(w, proto.BadRequest(fmt.Sprintf("Invalid 'accumulateBy' parameter: %s", err)))
		return
	}
	step, err := resolveStepFromQuery(qp, window, accumulateBy)
	if err != nil {
		proto.WriteError(w, proto.BadRequest(fmt.Sprintf("Invalid step parameter: %s", err)))
		return
	}

	// IdleByNode, if true, computes idle allocations at the node level.
	// Otherwise it is computed at the cluster level. (Not relevant if idle
	// is not included.)
	idleByNode := qp.GetBool("idleByNode", false)
	sharedLoadBalancer := qp.GetBool("sharelb", false)

	// IncludeProportionalAssetResourceCosts, if true,
	includeProportionalAssetResourceCosts := qp.GetBool("includeProportionalAssetResourceCosts", false)

	// include aggregated labels/annotations if true
	includeAggregatedMetadata := qp.GetBool("includeAggregatedMetadata", false)

	shareIdle := qp.GetBool("shareIdle", false)

	// Get allocation filter if provided
	allocationFilter := qp.Get("filter", "")

	// Query allocations with filtering, aggregation, and accumulation.
	// Filtering is done BEFORE aggregation inside QueryAllocation to ensure
	// filters can match on all allocation properties (like cluster, node, etc.)
	// before they are potentially lost or merged during aggregation.
	asr, err := a.Model.QueryAllocation(window, step, aggregateBy, includeIdle, idleByNode, includeProportionalAssetResourceCosts, includeAggregatedMetadata, sharedLoadBalancer, accumulateBy, shareIdle, allocationFilter)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "bad request") {
			proto.WriteError(w, proto.BadRequest(err.Error()))
		} else {
			proto.WriteError(w, proto.InternalServerError(err.Error()))
		}

		return
	}

	WriteData(w, asr, nil)
}
