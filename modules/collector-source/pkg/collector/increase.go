package collector

import (
	"time"
)

type IncreaseAggregator struct {
	name        string
	labelValues []string
	initiated   bool
	initial     float64
	current     float64
}

func Increase(name string, labelValues []string) MetricAggregator {
	return &IncreaseAggregator{
		name:        name,
		labelValues: labelValues,
	}
}

func (m *IncreaseAggregator) Name() string {
	return m.name
}

func (m *IncreaseAggregator) AdditionInfo() map[string]string {
	return nil
}

func (m *IncreaseAggregator) LabelValues() []string {
	return m.labelValues
}

func (m *IncreaseAggregator) Update(value float64, timestamp *time.Time, additionalInfo map[string]string) {
	if !m.initiated {
		m.initiated = true
		m.initial = value
	}
	m.current = value
}

func (m *IncreaseAggregator) Value() []MetricValue {
	return []MetricValue{
		{Value: m.current - m.initial},
	}
}
