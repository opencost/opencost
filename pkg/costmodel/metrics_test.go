package costmodel

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type FakeCache struct {
	clustercache.ClusterCache
}

func (f FakeCache) GetAllNodes() []*clustercache.Node {
	return []*clustercache.Node{}
}

type FakeProvider struct {
	models.Provider
}

func (f FakeProvider) GetConfig() (*models.CustomPricing, error) {
	return &models.CustomPricing{}, nil
}

type FakeClusterInfoProvider struct {
	clusters.ClusterInfoProvider
}

func (f FakeClusterInfoProvider) GetClusterInfo() map[string]string {
	return map[string]string{}
}

func TestEmitCarbonCost(t *testing.T) {
	registry := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = registry

	// Initialize metrics with empty config
	initCostModelMetrics(FakeCache{}, FakeProvider{}, FakeClusterInfoProvider{}, &metrics.MetricsConfig{})

	emitter := &CostModelMetricsEmitter{}

	// Emit carbon cost
	emitter.EmitCarbonCost("test-namespace", "test-pod", "test-cluster", 42.0)

	// Get all metrics
	metrics, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Find the carbon cost metric
	var found bool
	for _, m := range metrics {
		if m.GetName() == "opencost_carbon_cost" {
			found = true
			for _, metric := range m.GetMetric() {
				if metric.GetGauge().GetValue() != 42.0 {
					t.Errorf("Carbon cost value mismatch. Expected: 42.0, Got: %v", metric.GetGauge().GetValue())
				}
			}
			break
		}
	}

	if !found {
		t.Error("Carbon cost metric not found")
	}
}
