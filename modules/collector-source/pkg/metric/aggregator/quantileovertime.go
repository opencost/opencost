package aggregator

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

// quantileOverTimeAggregator retains the values observed in the window and
// computes the phi-quantile with linear interpolation between samples,
// matching Prometheus quantile_over_time semantics.
//
// Precondition: the owning MetricCollector must group at series granularity,
// so that exactly one series feeds this aggregator. Update carries a value
// and a timestamp but no series identity (see MetricAggregator), so samples
// arriving from several series under one group key are pooled flat, and the
// quantile of that pool is neither the per-series quantile nor the quantile
// of the summed series.
//
// Worked example, two pods in one group reporting at 0:00 / 0:30 / 1:00 —
// pod1 = 2, 4, 1 and pod2 = 3, 5, 1. The flat pool [1,1,2,3,4,5] gives a p95
// of 4.75. Summing per timestamp first gives the series [5,9,2] and a p95 of
// 8.6.
//
// Neither is a bug to be fixed here, because summing is only meaningful for
// additive quantities. The gauges this aggregator is registered against are
// not all additive: vllm:kv_cache_usage_perc is a ratio in [0, 1], so two
// replicas at 0.4 and 0.5 must not become 0.9. The semantics the Prometheus
// source implements for the same signals is per-series quantile followed by
// an outer combine (`max by (...) (quantile_over_time(...))`), which the flat
// pool coincides with when the precondition holds. The inference collectors
// therefore group by pod_uid, one series per pod. If a sum-then-quantile
// aggregator is ever wanted for an additive metric, model it on the
// accumulate-then-roll-over shape in increase.go rather than changing this
// one.
type quantileOverTimeAggregator struct {
	lock        sync.Mutex
	labelValues []string
	phi         float64
	values      []float64

	// seenTimestamps detects a violation of the series-granularity
	// precondition. Within one series a scrape timestamp appears exactly once,
	// so a repeat is direct evidence that the owning MetricCollector groups
	// coarser than one series and that several series are pooling here. That
	// silently returns a quantile of the pooled samples instead of a quantile
	// of the series, which is a plausible number rather than an obviously
	// wrong one, so it warrants a warning rather than a silent result.
	seenTimestamps map[int64]struct{}
	warned         bool
}

// QuantileOverTime returns a MetricAggregatorFactory that computes the
// phi-quantile (0 <= phi <= 1) of all values observed in the window. Out of
// range phi values are clamped: phi > 1 yields the maximum and phi < 0 the
// minimum.
func QuantileOverTime(phi float64) MetricAggregatorFactory {
	if phi > 1 {
		phi = 1
	}
	if phi < 0 {
		phi = 0
	}
	return func(labelValues []string) MetricAggregator {
		return &quantileOverTimeAggregator{
			labelValues: labelValues,
			phi:         phi,
		}
	}
}

func (a *quantileOverTimeAggregator) AdditionInfo() map[string]string {
	return nil
}

func (a *quantileOverTimeAggregator) LabelValues() []string {
	return a.labelValues
}

func (a *quantileOverTimeAggregator) Update(value float64, timestamp time.Time, additionalInfo map[string]string) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if !a.warned {
		unix := timestamp.Unix()
		if a.seenTimestamps == nil {
			a.seenTimestamps = make(map[int64]struct{})
		}
		if _, repeat := a.seenTimestamps[unix]; repeat {
			// Warn once per aggregator: the grouping is a static property of
			// the collector, so every subsequent sample repeats the same fault.
			log.Warnf("QuantileOverTime: multiple samples share timestamp %d for label values %v; "+
				"the owning MetricCollector is grouping coarser than one series, so this quantile is "+
				"computed over pooled series rather than over one series", unix, a.labelValues)
			a.warned = true
			a.seenTimestamps = nil
		} else {
			a.seenTimestamps[unix] = struct{}{}
		}
	}

	a.values = append(a.values, value)
}

func (a *quantileOverTimeAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()

	if len(a.values) == 0 {
		return []MetricValue{
			{Value: 0},
		}
	}

	sorted := make([]float64, len(a.values))
	copy(sorted, a.values)
	sort.Float64s(sorted)

	// Linear interpolation at rank phi*(n-1), as Prometheus does for
	// quantile_over_time. phi is clamped to [0, 1] at construction, so
	// lower and upper are always valid indices.
	rank := a.phi * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	weight := rank - float64(lower)

	return []MetricValue{
		{Value: sorted[lower] + (sorted[upper]-sorted[lower])*weight},
	}
}
