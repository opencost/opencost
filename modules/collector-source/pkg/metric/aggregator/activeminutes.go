package aggregator

import (
	"sync"
	"time"
)

// activateMinutesAggregator is a MetricAggregator which records the first and last timestamp of updates called on it
type activeMinutesAggregator struct {
	lock        sync.Mutex
	labelValues []string
	start       *time.Time
	end         *time.Time
}

func ActiveMinutes(labelValues []string) MetricAggregator {
	return &activeMinutesAggregator{
		labelValues: labelValues,
	}
}

func (a *activeMinutesAggregator) AdditionInfo() map[string]string {
	return nil
}

func (a *activeMinutesAggregator) LabelValues() []string {
	return a.labelValues
}

func (a *activeMinutesAggregator) Update(value float64, timestamp time.Time, additionalInfo map[string]string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.start == nil {
		a.start = &timestamp
	}
	if !timestamp.Equal(*a.start) {
		a.end = &timestamp
	}
}

func (a *activeMinutesAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()
	metricValues := make([]MetricValue, 0)
	if a.start != nil {
		metricValues = append(metricValues, MetricValue{Value: 1, Timestamp: a.start})
	}
	if a.end != nil {
		metricValues = append(metricValues, MetricValue{Value: 1, Timestamp: a.end})
	}
	return metricValues
}
