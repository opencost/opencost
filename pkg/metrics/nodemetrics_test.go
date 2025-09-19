package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
)

func TestKubeNodeCollector_Describe(t *testing.T) {
	tests := []struct {
		name            string
		disabledMetrics []string
		expectedCount   int
	}{
		{
			name:            "all metrics enabled",
			disabledMetrics: []string{},
			expectedCount:   8,
		},
		{
			name:            "capacity metric disabled",
			disabledMetrics: []string{"kube_node_status_capacity"},
			expectedCount:   7,
		},
		{
			name:            "all metrics disabled",
			disabledMetrics: []string{"kube_node_status_capacity", "kube_node_status_capacity_memory_bytes", "kube_node_status_capacity_cpu_cores", "kube_node_status_allocatable", "kube_node_status_allocatable_cpu_cores", "kube_node_status_allocatable_memory_bytes", "kube_node_labels", "kube_node_status_condition"},
			expectedCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			nc := KubeNodeCollector{
				KubeClusterCache: NewFakeNodeCache([]*clustercache.Node{}),
				metricsConfig:    mc,
			}

			ch := make(chan *prometheus.Desc, 10)
			nc.Describe(ch)
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

func TestKubeNodeCollector_Collect(t *testing.T) {
	tests := []struct {
		name            string
		nodes           []*clustercache.Node
		disabledMetrics []string
		expectedCount   int
	}{
		{
			name: "single node with resources",
			nodes: []*clustercache.Node{
				{
					UID:  types.UID("node-uid-1"),
					Name: "node-1",
					Labels: map[string]string{
						"app": "test",
					},
					Status: v1.NodeStatus{
						Capacity: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("4"),
							v1.ResourceMemory: resource.MustParse("8Gi"),
						},
						Allocatable: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("3.8"),
							v1.ResourceMemory: resource.MustParse("7.5Gi"),
						},
						Conditions: []v1.NodeCondition{
							{
								Type:   v1.NodeReady,
								Status: v1.ConditionTrue,
							},
						},
					},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   12, // 2 capacity + 2 capacity specific + 2 allocatable + 2 allocatable specific + 1 labels + 3 conditions
		},
		{
			name: "multiple_nodes",
			nodes: []*clustercache.Node{
				{
					Name:   "node-1",
					Labels: map[string]string{}, // Empty labels to avoid label metrics
					Status: v1.NodeStatus{
						Capacity: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("4"),
							v1.ResourceMemory: resource.MustParse("8Gi"),
						},
						Allocatable: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("3"),
							v1.ResourceMemory: resource.MustParse("7Gi"),
						},
						Conditions: []v1.NodeCondition{}, // Empty conditions to avoid condition metrics
					},
					UID: types.UID("test-node-1-uid"),
				},
				{
					Name:   "node-2",
					Labels: map[string]string{}, // Empty labels to avoid label metrics
					Status: v1.NodeStatus{
						Capacity: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("4"),
							v1.ResourceMemory: resource.MustParse("8Gi"),
						},
						Allocatable: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("3"),
							v1.ResourceMemory: resource.MustParse("7Gi"),
						},
						Conditions: []v1.NodeCondition{}, // Empty conditions to avoid condition metrics
					},
					UID: types.UID("test-node-2-uid"),
				},
			},

			expectedCount: 18, // 9 metrics per node × 2 nodes
		},
		{
			name:            "no nodes",
			nodes:           []*clustercache.Node{},
			disabledMetrics: []string{},
			expectedCount:   0,
		},
		{
			name: "metrics disabled",
			nodes: []*clustercache.Node{
				{
					UID:  types.UID("node-uid-1"),
					Name: "node-1",
					Status: v1.NodeStatus{
						Capacity: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			disabledMetrics: []string{"kube_node_status_capacity", "kube_node_status_capacity_cpu_cores", "kube_node_labels"},
			expectedCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			nc := KubeNodeCollector{
				KubeClusterCache: NewFakeNodeCache(tt.nodes),
				metricsConfig:    mc,
			}

			ch := make(chan prometheus.Metric, 20)
			nc.Collect(ch)
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

func TestKubeNodeStatusCapacityMetric(t *testing.T) {
	metric := newKubeNodeStatusCapacityMetric("kube_node_status_capacity", "test-node", "cpu", "core", "test-uid", 4.0)

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

	if *dtoMetric.Gauge.Value != 4.0 {
		t.Errorf("Expected gauge value 4.0, got %f", *dtoMetric.Gauge.Value)
	}

	// Verify labels
	expectedLabels := map[string]string{
		"node":     "test-node",
		"resource": "cpu",
		"unit":     "core",
		"uid":      "test-uid",
	}

	actualLabels := make(map[string]string)
	for _, label := range dtoMetric.Label {
		actualLabels[*label.Name] = *label.Value
	}

	for key, expectedValue := range expectedLabels {
		if actualValue, ok := actualLabels[key]; !ok {
			t.Errorf("Missing label %s", key)
		} else if actualValue != expectedValue {
			t.Errorf("Expected label %s=%s, got %s=%s", key, expectedValue, key, actualValue)
		}
	}
}

func TestKubeNodeLabelsMetric(t *testing.T) {
	labelNames := []string{"app", "version"}
	labelValues := []string{"test-app", "v1.0"}
	uid := "test-uid"

	metric := newKubeNodeLabelsMetric("test-node", "kube_node_labels", labelNames, labelValues, uid)

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
		"app":     "test-app",
		"version": "v1.0",
		"node":    "test-node",
		"uid":     uid,
	}

	actualLabels := make(map[string]string)
	for _, label := range dtoMetric.Label {
		actualLabels[*label.Name] = *label.Value
	}

	for key, expectedValue := range expectedLabels {
		if actualValue, ok := actualLabels[key]; !ok {
			t.Errorf("Missing label %s", key)
		} else if actualValue != expectedValue {
			t.Errorf("Expected label %s=%s, got %s=%s", key, expectedValue, key, actualValue)
		}
	}
}

func TestKubeNodeStatusConditionMetric(t *testing.T) {
	metric := newKubeNodeStatusConditionMetric("test-node", "kube_node_status_condition", "Ready", "true", 1.0, "test-uid")

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
		"node":      "test-node",
		"condition": "Ready",
		"status":    "true",
		"uid":       "test-uid",
	}

	actualLabels := make(map[string]string)
	for _, label := range dtoMetric.Label {
		actualLabels[*label.Name] = *label.Value
	}

	for key, expectedValue := range expectedLabels {
		if actualValue, ok := actualLabels[key]; !ok {
			t.Errorf("Missing label %s", key)
		} else if actualValue != expectedValue {
			t.Errorf("Expected label %s=%s, got %s=%s", key, expectedValue, key, actualValue)
		}
	}
}

func TestKubeNodeStatusCapacityMemoryBytesMetric(t *testing.T) {
	metric := newKubeNodeStatusCapacityMemoryBytesMetric("kube_node_status_capacity_memory_bytes", "test-node", "test-uid", 8589934592.0)

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

	if *dtoMetric.Gauge.Value != 8589934592.0 {
		t.Errorf("Expected gauge value 8589934592.0, got %f", *dtoMetric.Gauge.Value)
	}
}

func TestKubeNodeStatusCapacityCPUCoresMetric(t *testing.T) {
	metric := newKubeNodeStatusCapacityCPUCoresMetric("kube_node_status_capacity_cpu_cores", "test-node", "test-uid", 4.0)

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

	if *dtoMetric.Gauge.Value != 4.0 {
		t.Errorf("Expected gauge value 4.0, got %f", *dtoMetric.Gauge.Value)
	}
}

func TestGetConditions(t *testing.T) {
	tests := []struct {
		name           string
		status         v1.ConditionStatus
		expectedValues map[string]float64
	}{
		{
			name:   "condition true",
			status: v1.ConditionTrue,
			expectedValues: map[string]float64{
				"true":    1.0,
				"false":   0.0,
				"unknown": 0.0,
			},
		},
		{
			name:   "condition false",
			status: v1.ConditionFalse,
			expectedValues: map[string]float64{
				"true":    0.0,
				"false":   1.0,
				"unknown": 0.0,
			},
		},
		{
			name:   "condition unknown",
			status: v1.ConditionUnknown,
			expectedValues: map[string]float64{
				"true":    0.0,
				"false":   0.0,
				"unknown": 1.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions := getConditions(tt.status)

			if len(conditions) != 3 {
				t.Errorf("Expected 3 conditions, got %d", len(conditions))
			}

			actualValues := make(map[string]float64)
			for _, cond := range conditions {
				actualValues[cond.status] = cond.value
			}

			for status, expectedValue := range tt.expectedValues {
				if actualValue, ok := actualValues[status]; !ok {
					t.Errorf("Missing status %s", status)
				} else if actualValue != expectedValue {
					t.Errorf("Expected status %s=%f, got %f", status, expectedValue, actualValue)
				}
			}
		})
	}
}

// FakeNodeCache implements ClusterCache interface for testing
type FakeNodeCache struct {
	clustercache.ClusterCache
	nodes []*clustercache.Node
}

func (f FakeNodeCache) GetAllNodes() []*clustercache.Node {
	return f.nodes
}

func NewFakeNodeCache(nodes []*clustercache.Node) FakeNodeCache {
	return FakeNodeCache{
		nodes: nodes,
	}
}
