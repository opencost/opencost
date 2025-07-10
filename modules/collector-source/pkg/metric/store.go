package metric

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/opencost/opencost/modules/collector-source/pkg/metric/aggregator"
)

// MetricStore is an interface that defines an implementation capable of managing a collection
// of metric instances, and exposes helper methods for routing metric updates and queries to the
// proper metric instances.
type MetricStore interface {
	// Register accepts a `MetricCollector` instance and registers it for routing updates and querying.
	Register(collector *MetricCollector) error

	// Unregister accepts a `MetricCollectorID` and unregisters the metric metric instance from receiving metrics
	// updates and query availability.
	Unregister(collectorID MetricCollectorID) bool

	// Query accepts a `MetricCollectorID` and returns a slice of `MetricResult` instances for that metric.
	Query(collectorID MetricCollectorID) ([]*aggregator.MetricResult, error)

	// Update accepts the name of a metric, the label set and values to update the metric, the updated Value, and a Timestamp.
	// This method does not accept a `MetricCollectorID` because it provides updates across many potential MetricCollector instances
	// which utilize the same metric.
	Update(metricName string, labels map[string]string, value float64, timestamp time.Time, additionalInformation map[string]string)
}

type MetricStoreFactory func() MetricStore

// InMemoryMetricStore is a thread-safe implementation of the MetricStore interface that stores MetricCollector instances
// in memory.
type InMemoryMetricStore struct {
	lock          sync.Mutex
	byMetricName  map[string][]*MetricCollector
	byCollectorID map[MetricCollectorID]*MetricCollector
}

func NewInMemoryMetricStore() MetricStore {
	return &InMemoryMetricStore{
		byMetricName:  make(map[string][]*MetricCollector),
		byCollectorID: make(map[MetricCollectorID]*MetricCollector),
	}
}

func (m *InMemoryMetricStore) Register(collector *MetricCollector) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.byCollectorID[collector.id]; ok {
		return fmt.Errorf("metric with ID: %s already exists", collector.id)
	}

	m.byCollectorID[collector.id] = collector
	m.byMetricName[collector.metricName] = append(m.byMetricName[collector.metricName], collector)
	return nil
}

func (m *InMemoryMetricStore) Unregister(collectorID MetricCollectorID) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.byCollectorID[collectorID]; !ok {
		return false
	}

	inst := m.byCollectorID[collectorID]
	m.byMetricName[inst.metricName] = slices.DeleteFunc(m.byMetricName[inst.metricName], func(mc *MetricCollector) bool {
		return mc == nil || mc.id == collectorID
	})

	delete(m.byCollectorID, collectorID)
	return true
}

func (m *InMemoryMetricStore) Query(collectorID MetricCollectorID) ([]*aggregator.MetricResult, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.byCollectorID[collectorID]; !ok {
		return nil, fmt.Errorf("metric with ID: %s does not exist", collectorID)
	}

	return m.byCollectorID[collectorID].Get(), nil
}

func (m *InMemoryMetricStore) Update(
	metricName string,
	labels map[string]string,
	value float64,
	timestamp time.Time,
	additionalInformation map[string]string,
) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, collector := range m.byMetricName[metricName] {
		collector.Update(labels, value, timestamp, additionalInformation)
	}
}
