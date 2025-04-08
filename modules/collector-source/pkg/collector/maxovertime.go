package collector

import (
	"time"
)

type MaxOverTimeAggregator struct {
	name        string
	labelValues []string
	max         float64
}

func MaxOverTime(name string, labelValues []string) MetricAggregator {
	return &MaxOverTimeAggregator{
		name:        name,
		labelValues: labelValues,
	}
}

func (m *MaxOverTimeAggregator) Name() string {
	return m.name
}

func (m *MaxOverTimeAggregator) AdditionInfo() map[string]string {
	return nil
}

func (m *MaxOverTimeAggregator) LabelValues() []string {
	return m.labelValues
}

func (m *MaxOverTimeAggregator) Update(value float64, timestamp *time.Time, additionalInfo map[string]string) {
	if value > m.max {
		m.max = value
	}
}

func (m *MaxOverTimeAggregator) Value() []MetricValue {
	return []MetricValue{
		{Value: m.max},
	}
}
