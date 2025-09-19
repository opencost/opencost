package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestKubecostStatefulsetCollector_Describe(t *testing.T) {
	tests := []struct {
		name            string
		disabledMetrics []string
		expectMetric    bool
	}{
		{
			name:            "statefulSet_match_labels enabled",
			disabledMetrics: []string{},
			expectMetric:    true,
		},
		{
			name:            "statefulSet_match_labels disabled",
			disabledMetrics: []string{"statefulSet_match_labels"},
			expectMetric:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			sc := KubecostStatefulsetCollector{
				KubeClusterCache: NewFakeStatefulsetCache([]*clustercache.StatefulSet{}),
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

func TestKubecostStatefulsetCollector_Collect(t *testing.T) {
	tests := []struct {
		name            string
		statefulsets    []*clustercache.StatefulSet
		disabledMetrics []string
		expectedCount   int
	}{
		{
			name: "single statefulset with match labels",
			statefulsets: []*clustercache.StatefulSet{
				{
					UID:       types.UID("test-uid-1"),
					Name:      "test-statefulset",
					Namespace: "default",
					SpecSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "test", "version": "v1"},
					},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   1,
		},
		{
			name: "statefulset without match labels",
			statefulsets: []*clustercache.StatefulSet{
				{
					UID:       types.UID("test-uid-2"),
					Name:      "empty-statefulset",
					Namespace: "default",
					SpecSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{},
					},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   0,
		},
		{
			name: "statefulset with nil selector",
			statefulsets: []*clustercache.StatefulSet{
				{
					UID:          types.UID("test-uid-3"),
					Name:         "nil-selector-statefulset",
					Namespace:    "default",
					SpecSelector: nil,
				},
			},
			disabledMetrics: []string{},
			expectedCount:   0,
		},
		{
			name: "multiple statefulsets with match labels",
			statefulsets: []*clustercache.StatefulSet{
				{
					UID:       types.UID("test-uid-4"),
					Name:      "statefulset1",
					Namespace: "ns1",
					SpecSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "app1"},
					},
				},
				{
					UID:       types.UID("test-uid-5"),
					Name:      "statefulset2",
					Namespace: "ns2",
					SpecSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"component": "database"},
					},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   2,
		},
		{
			name: "metric disabled",
			statefulsets: []*clustercache.StatefulSet{
				{
					UID:       types.UID("test-uid-6"),
					Name:      "test-statefulset",
					Namespace: "default",
					SpecSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "test"},
					},
				},
			},
			disabledMetrics: []string{"statefulSet_match_labels"},
			expectedCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			sc := KubecostStatefulsetCollector{
				KubeClusterCache: NewFakeStatefulsetCache(tt.statefulsets),
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

func TestStatefulsetMatchLabelsMetric(t *testing.T) {
	labelNames := []string{"app", "version"}
	labelValues := []string{"test-app", "v1.0"}
	uid := "test-uid"

	metric := newStatefulsetMatchLabelsMetric("test-statefulset", "default", "statefulSet_match_labels", labelNames, labelValues, uid)

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
		"app":         "test-app",
		"version":     "v1.0",
		"statefulSet": "test-statefulset",
		"namespace":   "default",
		"uid":         uid,
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

func TestStatefulsetMatchLabelsMetric_EmptyLabels(t *testing.T) {
	metric := newStatefulsetMatchLabelsMetric("empty-statefulset", "test-ns", "statefulSet_match_labels", []string{}, []string{}, "empty-uid")

	var dtoMetric dto.Metric
	err := metric.Write(&dtoMetric)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Should still have the statefulset metadata labels
	expectedCount := 3 // statefulSet, namespace, uid
	if len(dtoMetric.Label) != expectedCount {
		t.Errorf("Expected %d labels, got %d", expectedCount, len(dtoMetric.Label))
	}
}

func TestStatefulsetMatchLabelsMetric_MissingFields(t *testing.T) {
	tests := []struct {
		name            string
		statefulsetName string
		namespace       string
		uid             string
	}{
		{
			name:            "empty statefulset name",
			statefulsetName: "",
			namespace:       "test-ns",
			uid:             "test-uid",
		},
		{
			name:            "empty namespace",
			statefulsetName: "test-statefulset",
			namespace:       "",
			uid:             "test-uid",
		},
		{
			name:            "empty uid",
			statefulsetName: "test-statefulset",
			namespace:       "test-ns",
			uid:             "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric := newStatefulsetMatchLabelsMetric(tt.statefulsetName, tt.namespace, "statefulSet_match_labels", []string{}, []string{}, tt.uid)

			var dtoMetric dto.Metric
			err := metric.Write(&dtoMetric)
			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			// Should still create the metric with empty values
			if len(dtoMetric.Label) != 3 {
				t.Errorf("Expected 3 labels, got %d", len(dtoMetric.Label))
			}
		})
	}
}

// FakeStatefulsetCache implements ClusterCache interface for testing
type FakeStatefulsetCache struct {
	clustercache.ClusterCache
	statefulsets []*clustercache.StatefulSet
}

func (f FakeStatefulsetCache) GetAllStatefulSets() []*clustercache.StatefulSet {
	return f.statefulsets
}

func NewFakeStatefulsetCache(statefulsets []*clustercache.StatefulSet) FakeStatefulsetCache {
	return FakeStatefulsetCache{
		statefulsets: statefulsets,
	}
}
