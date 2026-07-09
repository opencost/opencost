package aggregator

import (
	"math"
	"sort"
	"sync"
	"time"
)

// quantileOverTimeAggregator retains the values observed in the window and
// computes the phi-quantile with linear interpolation between samples,
// matching Prometheus quantile_over_time semantics.
type quantileOverTimeAggregator struct {
	lock        sync.Mutex
	labelValues []string
	phi         float64
	values      []float64
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
