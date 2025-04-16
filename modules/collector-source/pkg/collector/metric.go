package collector

import (
	"maps"
	"time"

	"github.com/opencost/opencost/modules/collector-source/pkg/aggregator"
)

// MetricCollector is a data structure that represents a specific metric collector instance that contains it's own breakdown
// of stored metrics by a specific label set.
type MetricCollector struct {
	id                MetricCollectorID // ie: RAMUsageAverage
	metricName        string            // ie: container_memory_working_set_bytes
	labels            []string
	aggregatorFactory aggregator.MetricAggregatorFactory
	metrics           map[uint64]aggregator.MetricAggregator // map[hash(labelValues)] = aggregator
	filter            func(map[string]string) bool
}

// NewMetricCollector creates a new MetricCollector instance with a unique identifier. The metric name is the specific
// name of the collected metric that will be used to query the
func NewMetricCollector(id MetricCollectorID, metricName string, labels []string, aggregatorFactory aggregator.MetricAggregatorFactory, fn func(map[string]string) bool) *MetricCollector {
	return &MetricCollector{
		id:                id,
		metricName:        metricName,
		labels:            labels,
		aggregatorFactory: aggregatorFactory,
		metrics:           make(map[uint64]aggregator.MetricAggregator),
		filter:            fn,
	}
}

func (mi *MetricCollector) Update(labels map[string]string, value float64, timestamp *time.Time, additionalInfo map[string]string) {
	if mi.filter != nil && !mi.filter(labels) {
		return
	}

	labelValues := make([]string, len(mi.labels))
	for i, key := range mi.labels {
		labelValues[i] = labels[key]
	}
	key := hash(labelValues)
	if mi.metrics[key] == nil {
		mi.metrics[key] = mi.aggregatorFactory(metricNameFor(mi.metricName, mi.labels, labelValues), labelValues)
	}

	mi.metrics[key].Update(value, timestamp, additionalInfo)
}

func (mi *MetricCollector) Get() []*aggregator.MetricResult {
	results := make([]*aggregator.MetricResult, 0, len(mi.metrics))
	for _, metric := range mi.metrics {
		labels := toMap(mi.labels, metric.LabelValues())
		maps.Copy(labels, metric.AdditionInfo())
		mr := &aggregator.MetricResult{
			Name:         metric.Name(),
			MetricLabels: labels,
			Values:       metric.Value(),
		}

		results = append(results, mr)
	}

	return results
}

func (mi *MetricCollector) Labels() []string {
	return mi.labels
}
