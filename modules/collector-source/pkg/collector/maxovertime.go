package collector

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

func (m *MaxOverTimeAggregator) LabelValues() []string {
	return m.labelValues
}

func (m *MaxOverTimeAggregator) Update(value float64) {
	if value > m.max {
		m.max = value
	}
}

func (m *MaxOverTimeAggregator) Value() float64 {
	return m.max
}
