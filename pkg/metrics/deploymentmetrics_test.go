package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/types"
)

func TestKubecostDeploymentCollector_Describe(t *testing.T) {
	tests := []struct {
		name            string
		disabledMetrics []string
		expectMetric    bool
	}{
		{
			name:            "deployment_match_labels enabled",
			disabledMetrics: []string{},
			expectMetric:    true,
		},
		{
			name:            "deployment_match_labels disabled",
			disabledMetrics: []string{"deployment_match_labels"},
			expectMetric:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			kdc := KubecostDeploymentCollector{
				KubeClusterCache: NewFakeDeploymentCache([]*clustercache.Deployment{}),
				metricsConfig:    mc,
			}

			ch := make(chan *prometheus.Desc, 10)
			kdc.Describe(ch)
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

func TestKubecostDeploymentCollector_Collect(t *testing.T) {
	tests := []struct {
		name            string
		deployments     []*clustercache.Deployment
		disabledMetrics []string
		expectedCount   int
	}{
		{
			name: "single deployment with match labels",
			deployments: []*clustercache.Deployment{
				{
					UID:         types.UID("test-uid-1"),
					Name:        "test-deployment",
					Namespace:   "default",
					MatchLabels: map[string]string{"app": "test", "version": "v1"},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   1,
		},
		{
			name: "deployment without match labels",
			deployments: []*clustercache.Deployment{
				{
					UID:         types.UID("test-uid-2"),
					Name:        "empty-deployment",
					Namespace:   "default",
					MatchLabels: map[string]string{},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   0,
		},
		{
			name: "multiple deployments with match labels",
			deployments: []*clustercache.Deployment{
				{
					UID:         types.UID("test-uid-3"),
					Name:        "deployment1",
					Namespace:   "ns1",
					MatchLabels: map[string]string{"app": "app1"},
				},
				{
					UID:         types.UID("test-uid-4"),
					Name:        "deployment2",
					Namespace:   "ns2",
					MatchLabels: map[string]string{"component": "frontend", "tier": "web"},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   2,
		},
		{
			name: "metric disabled",
			deployments: []*clustercache.Deployment{
				{
					UID:         types.UID("test-uid-5"),
					Name:        "test-deployment",
					Namespace:   "default",
					MatchLabels: map[string]string{"app": "test"},
				},
			},
			disabledMetrics: []string{"deployment_match_labels"},
			expectedCount:   0,
		},
		{
			name: "mixed deployments with and without labels",
			deployments: []*clustercache.Deployment{
				{
					UID:         types.UID("test-uid-6"),
					Name:        "with-labels",
					Namespace:   "default",
					MatchLabels: map[string]string{"app": "test"},
				},
				{
					UID:         types.UID("test-uid-7"),
					Name:        "without-labels",
					Namespace:   "default",
					MatchLabels: map[string]string{},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			kdc := KubecostDeploymentCollector{
				KubeClusterCache: NewFakeDeploymentCache(tt.deployments),
				metricsConfig:    mc,
			}

			ch := make(chan prometheus.Metric, 10)
			kdc.Collect(ch)
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

func TestDeploymentMatchLabelsMetric(t *testing.T) {
	labelNames := []string{"app", "version", "tier"}
	labelValues := []string{"myapp", "v2.0", "backend"}
	uid := "test-deployment-uid"

	metric := newDeploymentMatchLabelsMetric("test-deployment", "production", "deployment_match_labels", labelNames, labelValues, uid)

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
		"app":        "myapp",
		"version":    "v2.0",
		"tier":       "backend",
		"deployment": "test-deployment",
		"namespace":  "production",
		"uid":        uid,
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

func TestKubeDeploymentCollector_Describe(t *testing.T) {
	tests := []struct {
		name            string
		disabledMetrics []string
		expectedCount   int
	}{
		{
			name:            "all metrics enabled",
			disabledMetrics: []string{},
			expectedCount:   2,
		},
		{
			name:            "spec replicas disabled",
			disabledMetrics: []string{"kube_deployment_spec_replicas"},
			expectedCount:   1,
		},
		{
			name:            "status replicas disabled",
			disabledMetrics: []string{"kube_deployment_status_replicas_available"},
			expectedCount:   1,
		},
		{
			name:            "all metrics disabled",
			disabledMetrics: []string{"kube_deployment_spec_replicas", "kube_deployment_status_replicas_available"},
			expectedCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			kdc := KubeDeploymentCollector{
				KubeClusterCache: NewFakeDeploymentCache([]*clustercache.Deployment{}),
				metricsConfig:    mc,
			}

			ch := make(chan *prometheus.Desc, 10)
			kdc.Describe(ch)
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

func TestKubeDeploymentCollector_Collect(t *testing.T) {
	replicas3 := int32(3)
	replicas0 := int32(0)

	tests := []struct {
		name            string
		deployments     []*clustercache.Deployment
		disabledMetrics []string
		expectedCount   int
	}{
		{
			name: "deployment with explicit replicas",
			deployments: []*clustercache.Deployment{
				{
					UID:                     types.UID("test-uid-1"),
					Name:                    "test-deployment",
					Namespace:               "default",
					SpecReplicas:            &replicas3,
					StatusAvailableReplicas: 2,
				},
			},
			disabledMetrics: []string{},
			expectedCount:   2, // spec replicas + status available replicas
		},
		{
			name: "deployment with nil replicas defaults to 1",
			deployments: []*clustercache.Deployment{
				{
					UID:                     types.UID("test-uid-2"),
					Name:                    "default-replicas",
					Namespace:               "default",
					SpecReplicas:            nil,
					StatusAvailableReplicas: 1,
				},
			},
			disabledMetrics: []string{},
			expectedCount:   2,
		},
		{
			name: "deployment with zero replicas",
			deployments: []*clustercache.Deployment{
				{
					UID:                     types.UID("test-uid-3"),
					Name:                    "zero-replicas",
					Namespace:               "default",
					SpecReplicas:            &replicas0,
					StatusAvailableReplicas: 0,
				},
			},
			disabledMetrics: []string{},
			expectedCount:   2,
		},
		{
			name: "multiple deployments",
			deployments: []*clustercache.Deployment{
				{
					UID:                     types.UID("test-uid-4"),
					Name:                    "deployment1",
					Namespace:               "ns1",
					SpecReplicas:            &replicas3,
					StatusAvailableReplicas: 3,
				},
				{
					UID:                     types.UID("test-uid-5"),
					Name:                    "deployment2",
					Namespace:               "ns2",
					SpecReplicas:            nil,
					StatusAvailableReplicas: 0,
				},
			},
			disabledMetrics: []string{},
			expectedCount:   4, // 2 metrics per deployment
		},
		{
			name: "spec replicas disabled",
			deployments: []*clustercache.Deployment{
				{
					UID:                     types.UID("test-uid-6"),
					Name:                    "test-deployment",
					Namespace:               "default",
					SpecReplicas:            &replicas3,
					StatusAvailableReplicas: 2,
				},
			},
			disabledMetrics: []string{"kube_deployment_spec_replicas"},
			expectedCount:   1, // only status available replicas
		},
		{
			name: "status replicas disabled",
			deployments: []*clustercache.Deployment{
				{
					UID:                     types.UID("test-uid-7"),
					Name:                    "test-deployment",
					Namespace:               "default",
					SpecReplicas:            &replicas3,
					StatusAvailableReplicas: 2,
				},
			},
			disabledMetrics: []string{"kube_deployment_status_replicas_available"},
			expectedCount:   1, // only spec replicas
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			kdc := KubeDeploymentCollector{
				KubeClusterCache: NewFakeDeploymentCache(tt.deployments),
				metricsConfig:    mc,
			}

			ch := make(chan prometheus.Metric, 10)
			kdc.Collect(ch)
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

func TestKubeDeploymentReplicasMetric(t *testing.T) {
	metric := newKubeDeploymentReplicasMetric("kube_deployment_spec_replicas", "web-app", "production", 5, "deployment-uid")

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

	if *dtoMetric.Gauge.Value != 5.0 {
		t.Errorf("Expected gauge value 5.0, got %f", *dtoMetric.Gauge.Value)
	}

	// Verify labels
	expectedLabels := map[string]string{
		"deployment": "web-app",
		"namespace":  "production",
		"uid":        "deployment-uid",
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

func TestKubeDeploymentStatusAvailableReplicasMetric(t *testing.T) {
	metric := newKubeDeploymentStatusAvailableReplicasMetric("kube_deployment_status_replicas_available", "api-server", "backend", 3, "api-uid")

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

	if *dtoMetric.Gauge.Value != 3.0 {
		t.Errorf("Expected gauge value 3.0, got %f", *dtoMetric.Gauge.Value)
	}

	// Verify labels
	expectedLabels := map[string]string{
		"deployment": "api-server",
		"namespace":  "backend",
		"uid":        "api-uid",
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

func TestKubeDeploymentCollector_DefaultReplicas(t *testing.T) {
	// Test that nil replicas defaults to 1
	deployment := &clustercache.Deployment{
		UID:                     types.UID("test-uid"),
		Name:                    "test-deployment",
		Namespace:               "default",
		SpecReplicas:            nil,
		StatusAvailableReplicas: 0,
	}

	mc := MetricsConfig{
		DisabledMetrics: []string{"kube_deployment_status_replicas_available"}, // Only test spec replicas
	}
	kdc := KubeDeploymentCollector{
		KubeClusterCache: NewFakeDeploymentCache([]*clustercache.Deployment{deployment}),
		metricsConfig:    mc,
	}

	ch := make(chan prometheus.Metric, 10)
	kdc.Collect(ch)
	close(ch)

	for metric := range ch {
		var dtoMetric dto.Metric
		metric.Write(&dtoMetric)
		if *dtoMetric.Gauge.Value != 1.0 {
			t.Errorf("Expected default replicas value 1.0, got %f", *dtoMetric.Gauge.Value)
		}
	}
}

// FakeDeploymentCache implements ClusterCache interface for testing
type FakeDeploymentCache struct {
	clustercache.ClusterCache
	deployments []*clustercache.Deployment
}

func (f FakeDeploymentCache) GetAllDeployments() []*clustercache.Deployment {
	return f.deployments
}

func NewFakeDeploymentCache(deployments []*clustercache.Deployment) FakeDeploymentCache {
	return FakeDeploymentCache{
		deployments: deployments,
	}
}
