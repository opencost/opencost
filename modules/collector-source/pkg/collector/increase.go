package collector

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

func (m *IncreaseAggregator) LabelValues() []string {
	return m.labelValues
}

func (m *IncreaseAggregator) Update(value float64) {
	if !m.initiated {
		m.initiated = true
		m.initial = value
	}
	m.current = value
}

func (m *IncreaseAggregator) Value() float64 {
	return m.current - m.initial
}
