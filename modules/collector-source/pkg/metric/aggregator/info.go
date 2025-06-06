package aggregator

import (
	"maps"
	"sync"
	"time"
)

// infoAggregator is MetricAggregator meant to record label values and addition information
type infoAggregator struct {
	lock           sync.RWMutex
	labelValues    []string
	additionalInfo map[string]string
}

func Info(labelValues []string) MetricAggregator {
	return &infoAggregator{
		labelValues: labelValues,
	}
}

func (a *infoAggregator) AdditionInfo() map[string]string {
	a.lock.Lock()
	defer a.lock.Unlock()
	return maps.Clone(a.additionalInfo)
}

func (a *infoAggregator) LabelValues() []string {
	return a.labelValues
}

func (a *infoAggregator) Update(value float64, timestamp time.Time, additionalInfo map[string]string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.additionalInfo = maps.Clone(additionalInfo)
}

func (a *infoAggregator) Value() []MetricValue {
	return []MetricValue{
		{Value: 1},
	}
}
