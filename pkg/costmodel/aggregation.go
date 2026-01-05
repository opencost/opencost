package costmodel

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"

	"github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
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
	step := qp.GetDuration("step", window.Duration())

	// Aggregation is a required comma-separated list of fields by which to
	// aggregate results. Some fields allow a sub-field, which is distinguished
	// with a colon; e.g. "label:app".
	// Examples: "namespace", "namespace,label:app"
	aggregations := qp.GetList("aggregate", ",")
	aggregateBy, err := ParseAggregationProperties(aggregations)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'aggregate' parameter: %s", err), http.StatusBadRequest)
	}

	// Accumulate is an optional parameter, defaulting to false, which if true
	// sums each Set in the Range, producing one Set.
	accumulate := qp.GetBool("accumulate", false)

	// Get allocation filter if provided
	allocationFilter := qp.Get("filter", "")

	// Query for AllocationSets in increments of the given step duration,
	// appending each to the AllocationSetRange.
	asr := opencost.NewAllocationSetRange()
	stepStart := *window.Start()
	for window.End().After(stepStart) {
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
		for _, as := range asr.Slice() {
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
	if accumulate {
		asr, err = asr.Accumulate(opencost.AccumulateOptionAll)
		if err != nil {
			proto.WriteError(w, proto.InternalServerError(err.Error()))
			return
		}
	}

	sasl := []*opencost.SummaryAllocationSet{}
	for _, as := range asr.Slice() {
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
	step := qp.GetDuration("step", window.Duration())

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
	// Accumulate is an optional parameter, defaulting to false, which if true
	// sums each Set in the Range, producing one Set.
	accumulate := qp.GetBool("accumulate", false)

	// Accumulate is an optional parameter that accumulates an AllocationSetRange
	// by the resolution of the given time duration.
	// Defaults to 0. If a value is not passed then the parameter is not used.
	accumulateBy := opencost.AccumulateOption(qp.Get("accumulateBy", ""))

	// if accumulateBy is not explicitly set, and accumulate is true, ensure result is accumulated
	if accumulateBy == opencost.AccumulateOptionNone && accumulate {
		accumulateBy = opencost.AccumulateOptionAll
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
