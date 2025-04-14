package collector

import (
	"maps"
	"time"
)

// InfoAggregator is metric aggregator meant to record label values and addition information
type InfoAggregator struct {
	name           string
	labelValues    []string
	additionalInfo map[string]string
}

func Info(name string, labelValues []string) MetricAggregator {
	return &InfoAggregator{
		name:        name,
		labelValues: labelValues,
	}
}

func (m *InfoAggregator) Name() string {
	return m.name
}

func (m *InfoAggregator) AdditionInfo() map[string]string {
	return m.additionalInfo
}

func (m *InfoAggregator) LabelValues() []string {
	return m.labelValues
}

func (m *InfoAggregator) Update(value float64, timestamp *time.Time, additionalInfo map[string]string) {
	m.additionalInfo = maps.Clone(additionalInfo)
}

func (m *InfoAggregator) Value() []MetricValue {
	return []MetricValue{
		{Value: 1},
	}
}
