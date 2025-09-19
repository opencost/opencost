package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/types"
)

func TestKubecostServiceCollector_Describe(t *testing.T) {
	tests := []struct {
		name            string
		disabledMetrics []string
		expectMetric    bool
	}{
		{
			name:            "service_selector_labels enabled",
			disabledMetrics: []string{},
			expectMetric:    true,
		},
		{
			name:            "service_selector_labels disabled",
			disabledMetrics: []string{"service_selector_labels"},
			expectMetric:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			sc := KubecostServiceCollector{
				KubeClusterCache: NewFakeServiceCache([]*clustercache.Service{}),
				metricsConfig:    mc,
			}

			ch := make(chan *prometheus.Desc, 10)
			sc.Describe(ch)
			close(ch)

			count := 0
			for range ch {
				count++
			}

			if tt.expectMetric && count == 0 {
				t.Error("Expected metric description but got none")
			}
			if !tt.expectMetric && count > 0 {
				t.Error("Expected no metric description but got some")
			}
		})
	}
}

func TestKubecostServiceCollector_Collect(t *testing.T) {
	tests := []struct {
		name            string
		services        []*clustercache.Service
		disabledMetrics []string
		expectedCount   int
	}{
		{
			name: "single service with selector",
			services: []*clustercache.Service{
				{
					UID:          types.UID("test-uid-1"),
					Name:         "test-service",
					Namespace:    "default",
					SpecSelector: map[string]string{"app": "test", "version": "v1"},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   1,
		},
		{
			name: "service without selector",
			services: []*clustercache.Service{
				{
					UID:          types.UID("test-uid-2"),
					Name:         "headless-service",
					Namespace:    "default",
					SpecSelector: map[string]string{},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   0,
		},
		{
			name: "multiple services with selectors",
			services: []*clustercache.Service{
				{
					UID:          types.UID("test-uid-3"),
					Name:         "service1",
					Namespace:    "ns1",
					SpecSelector: map[string]string{"app": "app1"},
				},
				{
					UID:          types.UID("test-uid-4"),
					Name:         "service2",
					Namespace:    "ns2",
					SpecSelector: map[string]string{"component": "frontend"},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   2,
		},
		{
			name: "metric disabled",
			services: []*clustercache.Service{
				{
					UID:          types.UID("test-uid-5"),
					Name:         "test-service",
					Namespace:    "default",
					SpecSelector: map[string]string{"app": "test"},
				},
			},
			disabledMetrics: []string{"service_selector_labels"},
			expectedCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			sc := KubecostServiceCollector{
				KubeClusterCache: NewFakeServiceCache(tt.services),
				metricsConfig:    mc,
			}

			ch := make(chan prometheus.Metric, 10)
			sc.Collect(ch)
			close(ch)

			count := 0
			for range ch {
				count++
			}

			if count != tt.expectedCount {
				t.Errorf("Expected %d metrics, got %d", tt.expectedCount, count)
			}
		})
	}
}

func TestServiceSelectorLabelsMetric(t *testing.T) {
	labelNames := []string{"app", "version"}
	labelValues := []string{"test-app", "v1.0"}
	uid := "test-uid"

	metric := newServiceSelectorLabelsMetric("test-service", "default", "service_selector_labels", labelNames, labelValues, uid)

	// Test Desc method
	desc := metric.Desc()
	if desc == nil {
		t.Error("Expected non-nil descriptor")
	}

	// Test Write method
	var dtoMetric dto.Metric
	err := metric.Write(&dtoMetric)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Error("Expected gauge metric")
	}

	if *dtoMetric.Gauge.Value != 1.0 {
		t.Errorf("Expected gauge value 1.0, got %f", *dtoMetric.Gauge.Value)
	}

	// Verify labels
	expectedLabels := map[string]string{
		"app":       "test-app",
		"version":   "v1.0",
		"service":   "test-service",
		"namespace": "default",
		"uid":       uid,
	}

	actualLabels := make(map[string]string)
	for _, label := range dtoMetric.Label {
		actualLabels[*label.Name] = *label.Value
	}

	for key, expectedValue := range expectedLabels {
		if actualValue, ok := actualLabels[key]; !ok {
			t.Errorf("Missing label %s", key)
		} else if actualValue != expectedValue {
			t.Errorf("Label %s: expected %s, got %s", key, expectedValue, actualValue)
		}
	}
}

func TestServiceSelectorLabelsMetric_EmptyLabels(t *testing.T) {
	metric := newServiceSelectorLabelsMetric("empty-service", "test-ns", "service_selector_labels", []string{}, []string{}, "empty-uid")

	var dtoMetric dto.Metric
	err := metric.Write(&dtoMetric)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Should still have the service metadata labels
	expectedCount := 3 // service, namespace, uid
	if len(dtoMetric.Label) != expectedCount {
		t.Errorf("Expected %d labels, got %d", expectedCount, len(dtoMetric.Label))
	}
}

// FakeServiceCache implements ClusterCache interface for testing
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
