package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"k8s.io/apimachinery/pkg/types"
)

func TestServiceSelectorLabelsMetric(t *testing.T) {
	service := &clustercache.Service{
		UID:       types.UID("service-test-uid-123"),
		Name:      "test-service",
		Namespace: "test-namespace",
		SpecSelector: map[string]string{
			"app":  "web-app",
			"tier": "frontend",
		},
	}

	sampleServices := []*clustercache.Service{service}
	fc := NewFakeServiceCache(sampleServices)
	mc := MetricsConfig{
		DisabledMetrics: []string{},
	}

	_ = KubecostServiceCollector{
		KubeClusterCache: fc,
		metricsConfig:    mc,
	}

	metric := newServiceSelectorLabelsMetric(
		service.Name,
		service.Namespace,
		string(service.UID),
		"service_selector_labels",
		[]string{"label_app", "label_tier"},
		[]string{"web-app", "frontend"},
	)

	if metric.serviceName != "test-service" {
		t.Errorf("Expected service name 'test-service', got %s", metric.serviceName)
	}
	if metric.namespace != "test-namespace" {
		t.Errorf("Expected namespace 'test-namespace', got %s", metric.namespace)
	}
	if metric.uid != "service-test-uid-123" {
		t.Errorf("Expected UID 'service-test-uid-123', got %s", metric.uid)
	}
	if len(metric.labelNames) != 2 {
		t.Errorf("Expected 2 label names, got %d", len(metric.labelNames))
	}
	if len(metric.labelValues) != 2 {
		t.Errorf("Expected 2 label values, got %d", len(metric.labelValues))
	}
}

type FakeServiceCache struct {
	clustercache.ClusterCache
	services []*clustercache.Service
}

func (f FakeServiceCache) GetAllServices() []*clustercache.Service {
	return f.services
}

func NewFakeServiceCache(services []*clustercache.Service) FakeServiceCache {
	return FakeServiceCache{
		services: services,
	}
}
