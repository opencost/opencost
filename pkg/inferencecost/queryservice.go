package inferencecost

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/opencost"
	proto "github.com/opencost/opencost/core/pkg/protocol"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

// protocol is the package-level HTTP response helper, mirroring the pattern
// used in pkg/costmodel/router.go.
var protocol = proto.HTTP()

// QueryService handles HTTP requests for the /inferenceCost endpoints. It
// computes inference costs on demand at request time by driving the Collector
// and Calculator, matching the pattern of OpenCost's /allocation and /assets
// APIs (no stored repository).
//
// Note: each request issues two ComputeAllocation calls per sub-window (one
// with idle sharing for allocation costs, one without for usage costs). This
// is consistent with /allocation which also recomputes from Prometheus on
// every request and has no result cache.
type QueryService struct {
	collector  *Collector
	calculator *Calculator
}

// NewQueryService creates a QueryService. Both collector and calculator must
// be non-nil; if either is nil the handlers return 501.
func NewQueryService(c *Collector, calc *Calculator) *QueryService {
	return &QueryService{
		collector:  c,
		calculator: calc,
	}
}

// GetInferenceCostTotalHandler returns an httprouter-compatible handler for
// GET /inferenceCost/total.
//
// Query parameters:
//   - window (required): RFC3339 "start,end" or named range
//   - costBasis: "usage" or "allocation" (default: allocation)
//   - aggregate: comma-separated list of dimensions (model_name, model_version, namespace, cluster)
//   - accumulate: hour, day, week, month (optional; controls sub-window step for large windows)
//   - filter: prop:value[+prop:value] (optional; Phase-1 minimal, AND semantics)
func (qs *QueryService) GetInferenceCostTotalHandler() func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if qs == nil || qs.collector == nil || qs.calculator == nil {
			http.Error(w, "InferenceCost query service not available", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		req, err := ParseInferenceCostRequest(qp)
		if err != nil {
			protocol.WriteError(w, protocol.BadRequest(err.Error()))
			return
		}

		setRange, err := qs.query(r.Context(), req)
		if err != nil {
			protocol.WriteError(w, protocol.InternalServerError(fmt.Sprintf("InferenceCost query failed: %s", err)))
			return
		}

		// Accumulate all step-windows into a single total set.
		total := accumulate(setRange.InferenceCostSets)

		protocol.WriteData(w, total)
	}
}

// GetInferenceCostTimeseriesHandler returns an httprouter-compatible handler
// for GET /inferenceCost/timeseries.
//
// The 'accumulate' parameter is required for this endpoint (it defines the
// time-series step size). All other parameters are the same as /total.
func (qs *QueryService) GetInferenceCostTimeseriesHandler() func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if qs == nil || qs.collector == nil || qs.calculator == nil {
			http.Error(w, "InferenceCost query service not available", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		req, err := ParseInferenceCostTimeseriesRequest(qp)
		if err != nil {
			protocol.WriteError(w, protocol.BadRequest(err.Error()))
			return
		}

		setRange, err := qs.query(r.Context(), req)
		if err != nil {
			protocol.WriteError(w, protocol.InternalServerError(fmt.Sprintf("query failed: %s", err)))
			return
		}

		protocol.WriteData(w, setRange)
	}
}

// query drives the on-demand compute loop for a QueryRequest:
//  1. Split [Start, End] into Step-sized sub-windows.
//  2. For each sub-window: collect metrics from Prometheus + allocation layer,
//     calculate costs, project to the requested cost basis, apply filter, aggregate.
//  3. Return all sub-window sets in an InferenceCostSetRange.
//
// The caller decides whether to accumulate the range into a single total (/total)
// or return the per-step sets (/timeseries).
func (qs *QueryService) query(ctx context.Context, req *QueryRequest) (*InferenceCostSetRange, error) {
	overallWindow := opencost.NewClosedWindow(req.Start, req.End)
	sets := make([]*InferenceCostSet, 0)

	stepStart := req.Start
	for stepStart.Before(req.End) {
		stepEnd := stepStart.Add(req.Step)
		if stepEnd.After(req.End) {
			stepEnd = req.End
		}

		stepWindow := opencost.NewClosedWindow(stepStart, stepEnd)
		set, err := qs.computeStep(ctx, req, stepWindow)
		if err != nil {
			return nil, fmt.Errorf("computing window [%s, %s]: %w", stepStart.Format(time.RFC3339), stepEnd.Format(time.RFC3339), err)
		}
		sets = append(sets, set)

		stepStart = stepEnd
	}

	return &InferenceCostSetRange{
		InferenceCostSets: sets,
		Window:            overallWindow,
	}, nil
}

// computeStep computes inference costs for a single sub-window, projects to
// the requested cost basis, applies filters, and aggregates.
func (qs *QueryService) computeStep(ctx context.Context, req *QueryRequest, win opencost.Window) (*InferenceCostSet, error) {
	// Collect raw metrics for this sub-window.
	metrics, err := qs.collector.CollectMetrics(ctx, *win.Start(), *win.End())
	if err != nil {
		return nil, fmt.Errorf("collecting metrics: %w", err)
	}

	// Compute derived costs (per-million rates, input/output split).
	qs.calculator.CalculateCosts(metrics)

	set := newInferenceCostSet(win)
	for _, ic := range metrics {
		resp := newInferenceCostResponse(ic, req.CostBasis, win)

		// Apply Phase-1 filter (AND semantics over supported properties).
		if !matchesFilter(resp, req.Filter) {
			continue
		}

		// Derive the natural key for this entry; aggregate() will re-key if
		// aggregateBy is set, but we still need a unique key here.
		key, err := aggKey(resp.Properties, nil) // nil = natural key
		if err != nil {
			return nil, err
		}
		set.InferenceCosts[key] = resp
	}

	// Aggregate into the requested dimensions.
	if err := set.aggregate(req.AggregateBy); err != nil {
		return nil, fmt.Errorf("aggregating results: %w", err)
	}

	return set, nil
}
