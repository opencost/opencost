package costmodel

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestEmitCarbonCost(t *testing.T) {
	gv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "opencost_carbon_cost",
		Help: "test",
	}, []string{"cluster", "provider", "asset_type", "name"})

	emitter := &CostModelMetricsEmitter{CarbonCostRecorder: gv}
	emitter.EmitCarbonCost("cluster-one", "AWS", "Node", "node-1", 0.042)

	g, err := gv.GetMetricWithLabelValues("cluster-one", "AWS", "Node", "node-1")
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}

	metric := &dto.Metric{}
	if err := g.Write(metric); err != nil {
		t.Fatalf("Write: %v", err)
	}

	const want = 0.042
	const eps = 1e-12
	if got := metric.GetGauge().GetValue(); got < want-eps || got > want+eps {
		t.Errorf("carbon cost value mismatch: got %v, want %v", got, want)
	}
}

func TestEmitCarbonCost_OverwritesPreviousValue(t *testing.T) {
	gv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "opencost_carbon_cost",
		Help: "test",
	}, []string{"cluster", "provider", "asset_type", "name"})

	emitter := &CostModelMetricsEmitter{CarbonCostRecorder: gv}
	emitter.EmitCarbonCost("cluster-one", "AWS", "Node", "node-1", 0.1)
	emitter.EmitCarbonCost("cluster-one", "AWS", "Node", "node-1", 0.2)

	g, err := gv.GetMetricWithLabelValues("cluster-one", "AWS", "Node", "node-1")
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}

	metric := &dto.Metric{}
	if err := g.Write(metric); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if got := metric.GetGauge().GetValue(); got != 0.2 {
		t.Errorf("carbon cost value mismatch: got %v, want 0.2", got)
	}
}
