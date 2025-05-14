package aggregator

import (
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

// MetricValue is a resulting data point value with an optional timestamp.
type MetricValue struct {
	Value     float64
	Timestamp *time.Time
}

// MetricResult contains a resulting metric name, the associated labels and label values, and a slice of
// MetricValues.
type MetricResult struct {
	Name         string
	MetricLabels map[string]string
	Values       []MetricValue
}

func (mr *MetricResult) ToQueryResult() *source.QueryResult {
	metrics := map[string]any{}
	for key, value := range mr.MetricLabels {
		metrics[key] = value
	}

	values := make([]*util.Vector, len(mr.Values))
	for i, value := range mr.Values {
		timestamp := 0.0
		if value.Timestamp != nil {
			timestamp = float64(value.Timestamp.Unix())
		}
		values[i] = &util.Vector{
			Timestamp: timestamp,
			Value:     value.Value,
		}
	}

	return source.NewQueryResult(metrics, values, nil)
}

// MetricAggregator is an interface that defines the methods for a metric metric aggregation.
// For example, we have a metric `foo_metric`, and we wish to query and collect the average over time.
// In this case, the `AverageOverTime` component is the MetricAggregator. It is the component responsible
// for routing updates to metric values into their proper condensed form.
type MetricAggregator interface {
	Name() string
	AdditionInfo() map[string]string
	Update(value float64, timestamp time.Time, additionalInfo map[string]string)
	Value() []MetricValue
	LabelValues() []string
}

// MetricAggregatorFactory is a function that accepts a string name and returns a pointer to a MetricAggregator
// implementation.
type MetricAggregatorFactory func(name string, labelValues []string) MetricAggregator
