package source

import "testing"

type pinnableQuerier struct {
	*MockMetricsQuerier
	pinned   *MockMetricsQuerier
	released int
}

func (p *pinnableQuerier) Pin() (MetricsQuerier, func()) {
	return p.pinned, func() { p.released++ }
}

type metricsOnlyDataSource struct {
	OpenCostDataSource
	metrics MetricsQuerier
}

func (m *metricsOnlyDataSource) Metrics() MetricsQuerier { return m.metrics }

func TestPinMetrics(t *testing.T) {
	plain := NewMockMetricsQuerier()
	got, release := PinMetrics(plain)
	if got != plain {
		t.Errorf("expected a non-pinnable querier to be returned unchanged")
	}
	release()

	p := &pinnableQuerier{MockMetricsQuerier: NewMockMetricsQuerier(), pinned: NewMockMetricsQuerier()}
	got, release = PinMetrics(p)
	if got != p.pinned {
		t.Errorf("expected the pinned querier")
	}
	release()
	if p.released != 1 {
		t.Errorf("expected release to be forwarded, got %d", p.released)
	}
}

func TestPinDataSource(t *testing.T) {
	plain := &metricsOnlyDataSource{metrics: NewMockMetricsQuerier()}
	got, release := PinDataSource(plain)
	if got != plain {
		t.Errorf("expected a data source with a non-pinnable querier to be returned unchanged")
	}
	release()

	p := &pinnableQuerier{MockMetricsQuerier: NewMockMetricsQuerier(), pinned: NewMockMetricsQuerier()}
	ds := &metricsOnlyDataSource{metrics: p}
	got, release = PinDataSource(ds)
	if got.Metrics() != p.pinned || got.Metrics() != p.pinned {
		t.Errorf("expected every Metrics() call to return the pinned querier")
	}
	release()
	if p.released != 1 {
		t.Errorf("expected release to be forwarded, got %d", p.released)
	}
}
