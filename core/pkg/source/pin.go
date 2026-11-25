package source

// PinnableMetricsQuerier is optionally implemented by a MetricsQuerier whose underlying data can change
// between queries, for example one that serves from in-memory snapshots replaced on an interval.
//
// Computations that issue many queries for one window (allocation, assets, kube model) pin the querier
// once and issue every query through the pinned view, so that all results come from one consistent
// state of the data rather than a mix of states from before and after an update.
//
// No in-tree data source implements this today: the Prometheus and collector sources are unaffected.
// It is intended for data sources that replace their state wholesale, such as an adapter serving
// immutable snapshots.
type PinnableMetricsQuerier interface {
	MetricsQuerier

	// Pin returns a MetricsQuerier bound to the current state of the data, and a release function.
	//
	// Contract:
	//   - The pinned querier must serve every query from the state current at the time of Pin, and
	//     keep that view valid until release is called, even if the underlying data is updated.
	//   - Callers call release exactly once, after every query issued through the pinned querier has
	//     completed. Implementations must make release safe to call exactly once; the pinned querier
	//     must not be used after release.
	//   - Pin may be called concurrently; each call returns an independent pinned view.
	Pin() (MetricsQuerier, func())
}

// noRelease is the release function for a querier that was not pinned.
func noRelease() {
	// nothing was pinned, so there is nothing to release
}

// PinMetrics pins the querier if it implements PinnableMetricsQuerier. Otherwise it returns the querier
// unchanged and a no-op release function.
func PinMetrics(q MetricsQuerier) (MetricsQuerier, func()) {
	if p, ok := q.(PinnableMetricsQuerier); ok {
		pinned, release := p.Pin()
		if release == nil {
			release = noRelease
		}
		return pinned, release
	}
	return q, noRelease
}

// PinDataSource returns a data source whose Metrics() always returns the same pinned querier, for use
// by a computation that reads metrics through several helpers taking an OpenCostDataSource. If the data
// source's querier does not implement PinnableMetricsQuerier, the data source is returned unchanged.
//
// The returned data source only forwards the OpenCostDataSource methods; optional interfaces
// implemented by the original are not visible through it.
func PinDataSource(ds OpenCostDataSource) (OpenCostDataSource, func()) {
	q := ds.Metrics()
	if _, ok := q.(PinnableMetricsQuerier); !ok {
		return ds, noRelease
	}

	pinned, release := PinMetrics(q)
	return &pinnedDataSource{OpenCostDataSource: ds, metrics: pinned}, release
}

// pinnedDataSource is an OpenCostDataSource whose Metrics() returns a pinned querier.
type pinnedDataSource struct {
	OpenCostDataSource
	metrics MetricsQuerier
}

func (p *pinnedDataSource) Metrics() MetricsQuerier {
	return p.metrics
}
