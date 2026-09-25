package source

// PinnableMetricsQuerier is optionally implemented by a MetricsQuerier whose underlying data can change
// between queries, for example one that serves from in-memory snapshots replaced on an interval.
//
// Computations that issue many queries for one window (allocation, assets, kube model) pin the querier
// once and issue every query through the pinned view, so that all results come from one consistent
// state of the data rather than a mix of states from before and after an update.
type PinnableMetricsQuerier interface {
	MetricsQuerier

	// Pin returns a MetricsQuerier bound to the current state of the data, and a release function
	// which must be called once the pinned querier is no longer used. The pinned querier must remain
	// valid until released, even if the underlying data is updated in the meantime.
	Pin() (MetricsQuerier, func())
}

// PinMetrics pins the querier if it implements PinnableMetricsQuerier. Otherwise it returns the querier
// unchanged and a no-op release function.
func PinMetrics(q MetricsQuerier) (MetricsQuerier, func()) {
	if p, ok := q.(PinnableMetricsQuerier); ok {
		pinned, release := p.Pin()
		if release == nil {
			release = func() {}
		}
		return pinned, release
	}
	return q, func() {}
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
		return ds, func() {}
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
