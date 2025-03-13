package collector

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

// Metric names
const (
	ContainerMemoryWorkingSetBytes string = "container_memory_working_set_bytes"
)

// MetricCollectorID is a unique identifier for a specific metric collector instance. We
// use this identifier to register and unregister metric instances from the metrics collector
// instead of the metric name and aggregation type to allow selectable cardinality (via labels)
// across multiple instances of the same aggregation type and metric name.
type MetricCollectorID string

const (
	RAMUsageAverageID MetricCollectorID = "RAMUsageAverage"
	// etc ...
)

// MetricsCollector is an interface that defines an implementation capable of managing a collection
// of metric instances, and exposes helper methods for routing metric updates and queries to the
// proper collector instances.
type MetricsCollector interface {
	// Register accepts a `MetricCollector` instance and registers it for routing updates and querying.
	Register(collector *MetricCollector) error

	// Unregister accepts a `MetricCollectorID` and unregisters the metric collector instance from receiving metrics
	// updates and query availability.
	Unregister(collectorID MetricCollectorID) bool

	// Query accepts a `MetricCollectorID` and returns a slice of `MetricResult` instances for that collector.
	Query(collectorID MetricCollectorID) ([]*MetricResult, error)

	// Update accepts the name of a metric, the label set and values to update the metric, the updated value, and a timestamp.
	// This method does not accept a `MetricCollectorID` because it provides updates across many potential metric collector instances
	// which utilize the same metric.
	Update(metricName string, labels map[string]string, value float64, timestamp *time.Time)
}

// InMemoryMetricsCollector is a thread-safe implementation of the `MetricsCollector` interface that stores metric instances
// in memory.
type InMemoryMetricsCollector struct {
	lock          sync.Mutex
	byMetricName  map[string][]*MetricCollector
	byCollectorID map[MetricCollectorID]*MetricCollector
}

func NewInMemoryMetricsCollector() MetricsCollector {
	return &InMemoryMetricsCollector{
		byMetricName:  make(map[string][]*MetricCollector),
		byCollectorID: make(map[MetricCollectorID]*MetricCollector),
	}
}

func (immc *InMemoryMetricsCollector) Register(collector *MetricCollector) error {
	immc.lock.Lock()
	defer immc.lock.Unlock()

	if _, ok := immc.byCollectorID[collector.id]; ok {
		return fmt.Errorf("collector with ID: %s already exists", collector.id)
	}

	immc.byCollectorID[collector.id] = collector
	immc.byMetricName[collector.metricName] = append(immc.byMetricName[collector.metricName], collector)
	return nil
}

func (immc *InMemoryMetricsCollector) Unregister(collectorID MetricCollectorID) bool {
	immc.lock.Lock()
	defer immc.lock.Unlock()

	if _, ok := immc.byCollectorID[collectorID]; !ok {
		return false
	}

	inst := immc.byCollectorID[collectorID]
	immc.byMetricName[inst.metricName] = slices.DeleteFunc(immc.byMetricName[inst.metricName], func(mc *MetricCollector) bool {
		return mc == nil || mc.id == collectorID
	})

	delete(immc.byCollectorID, collectorID)
	return true
}

func (immc *InMemoryMetricsCollector) Query(collectorID MetricCollectorID) ([]*MetricResult, error) {
	immc.lock.Lock()
	defer immc.lock.Unlock()

	if _, ok := immc.byCollectorID[collectorID]; !ok {
		return nil, fmt.Errorf("collector with ID: %s does not exist", collectorID)
	}

	return immc.byCollectorID[collectorID].Get(), nil
}

func (immc *InMemoryMetricsCollector) Update(metricName string, labels map[string]string, value float64, timestamp *time.Time) {
	immc.lock.Lock()
	defer immc.lock.Unlock()

	for _, collector := range immc.byMetricName[metricName] {
		labelValues := make([]string, 0, len(collector.labels))
		for _, label := range collector.labels {
			labelValues = append(labelValues, labels[label])
		}

		collector.Update(labelValues, value, timestamp)
	}
}
