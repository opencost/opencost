package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestKubecostPodCollector_Describe(t *testing.T) {
	tests := []struct {
		name            string
		disabledMetrics []string
		expectMetric    bool
	}{
		{
			name:            "annotations enabled",
			disabledMetrics: []string{},
			expectMetric:    true,
		},
		{
			name:            "annotations disabled",
			disabledMetrics: []string{"kube_pod_annotations"},
			expectMetric:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			kpc := KubecostPodCollector{
				KubeClusterCache: NewFakePodCache([]*clustercache.Pod{}),
				metricsConfig:    mc,
			}

			ch := make(chan *prometheus.Desc, 10)
			kpc.Describe(ch)
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

func TestKubecostPodCollector_Collect(t *testing.T) {
	tests := []struct {
		name            string
		pods            []*clustercache.Pod
		disabledMetrics []string
		expectedCount   int
	}{
		{
			name: "pod with annotations",
			pods: []*clustercache.Pod{
				{
					UID:       types.UID("pod-uid-1"),
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "8080",
					},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   1,
		},
		{
			name: "pod without annotations",
			pods: []*clustercache.Pod{
				{
					UID:         types.UID("pod-uid-2"),
					Name:        "empty-pod",
					Namespace:   "default",
					Annotations: map[string]string{},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   0,
		},
		{
			name: "multiple pods with mixed annotations",
			pods: []*clustercache.Pod{
				{
					UID:         types.UID("pod-uid-3"),
					Name:        "pod1",
					Namespace:   "ns1",
					Annotations: map[string]string{"key": "value"},
				},
				{
					UID:         types.UID("pod-uid-4"),
					Name:        "pod2",
					Namespace:   "ns1",
					Annotations: map[string]string{},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   1,
		},
		{
			name: "metric disabled",
			pods: []*clustercache.Pod{
				{
					UID:         types.UID("pod-uid-5"),
					Name:        "test-pod",
					Namespace:   "default",
					Annotations: map[string]string{"test": "annotation"},
				},
			},
			disabledMetrics: []string{"kube_pod_annotations"},
			expectedCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			kpc := KubecostPodCollector{
				KubeClusterCache: NewFakePodCache(tt.pods),
				metricsConfig:    mc,
			}

			ch := make(chan prometheus.Metric, 10)
			kpc.Collect(ch)
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

func TestPodAnnotationMetric(t *testing.T) {
	labelNames := []string{"annotation_key1", "annotation_key2"}
	labelValues := []string{"value1", "value2"}

	metric := newPodAnnotationMetric("kube_pod_annotations", "test-ns", "test-pod", "test-uid", labelNames, labelValues)

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
		"annotation_key1": "value1",
		"annotation_key2": "value2",
		"namespace":       "test-ns",
		"pod":             "test-pod",
		"uid":             "test-uid",
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

func TestKubePodCollector_Describe(t *testing.T) {
	tests := []struct {
		name            string
		disabledMetrics []string
		expectedCount   int
	}{
		{
			name:            "all metrics enabled",
			disabledMetrics: []string{},
			expectedCount:   10,
		},
		{
			name: "some metrics disabled",
			disabledMetrics: []string{
				"kube_pod_labels",
				"kube_pod_owner",
				"kube_pod_container_status_running",
			},
			expectedCount: 7,
		},
		{
			name: "all metrics disabled",
			disabledMetrics: []string{
				"kube_pod_labels",
				"kube_pod_owner",
				"kube_pod_container_status_running",
				"kube_pod_container_status_terminated_reason",
				"kube_pod_container_status_restarts_total",
				"kube_pod_container_resource_requests",
				"kube_pod_container_resource_limits",
				"kube_pod_container_resource_limits_cpu_cores",
				"kube_pod_container_resource_limits_memory_bytes",
				"kube_pod_status_phase",
			},
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			kpc := KubePodCollector{
				KubeClusterCache: NewFakePodCache([]*clustercache.Pod{}),
				metricsConfig:    mc,
			}

			ch := make(chan *prometheus.Desc, 15)
			kpc.Describe(ch)
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

func TestKubePodCollector_Collect(t *testing.T) {
	boolTrue := true
	tests := []struct {
		name            string
		pods            []*clustercache.Pod
		disabledMetrics []string
		expectedCount   int
	}{
		{
			name: "pod with all features",
			pods: []*clustercache.Pod{
				{
					UID:       types.UID("pod-uid-1"),
					Name:      "test-pod",
					Namespace: "default",
					Labels: map[string]string{
						"app":     "test",
						"version": "v1",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							Name:       "test-deployment",
							Kind:       "Deployment",
							Controller: &boolTrue,
						},
					},
					Status: clustercache.PodStatus{
						Phase: v1.PodRunning,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name:         "container1",
								RestartCount: 2,
								State: v1.ContainerState{
									Running: &v1.ContainerStateRunning{},
								},
							},
						},
					},
					Spec: clustercache.PodSpec{
						NodeName: "node1",
						Containers: []clustercache.Container{
							{
								Name: "container1",
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("100m"),
										v1.ResourceMemory: resource.MustParse("128Mi"),
									},
									Limits: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("200m"),
										v1.ResourceMemory: resource.MustParse("256Mi"),
									},
								},
							},
						},
					},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   15, // 5 phases + 1 labels + 1 owner + 1 restarts + 1 running + 2 requests + 4 limits
		},
		{
			name: "pod without containers",
			pods: []*clustercache.Pod{
				{
					UID:       types.UID("pod-uid-2"),
					Name:      "empty-pod",
					Namespace: "default",
					Labels:    map[string]string{"test": "label"},
					Status: clustercache.PodStatus{
						Phase: v1.PodPending,
					},
					Spec: clustercache.PodSpec{
						Containers: []clustercache.Container{},
					},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   6, // 5 phases + 1 labels
		},
		{
			name: "pod with terminated container",
			pods: []*clustercache.Pod{
				{
					UID:       types.UID("pod-uid-3"),
					Name:      "terminated-pod",
					Namespace: "default",
					Labels:    map[string]string{},
					Status: clustercache.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name:         "failed-container",
								RestartCount: 5,
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										Reason: "OOMKilled",
									},
								},
							},
						},
					},
					Spec: clustercache.PodSpec{
						Containers: []clustercache.Container{
							{
								Name:      "failed-container",
								Resources: v1.ResourceRequirements{},
							},
						},
					},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   8, // 5 phases + 1 labels + 1 restarts + 1 terminated reason
		},
		{
			name: "pod without phase",
			pods: []*clustercache.Pod{
				{
					UID:       types.UID("pod-uid-4"),
					Name:      "no-phase-pod",
					Namespace: "default",
					Labels:    map[string]string{"app": "test"},
					Status: clustercache.PodStatus{
						Phase: "", // Empty phase
					},
					Spec: clustercache.PodSpec{},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   1, // Only labels
		},
		{
			name: "multiple containers",
			pods: []*clustercache.Pod{
				{
					UID:       types.UID("pod-uid-5"),
					Name:      "multi-container-pod",
					Namespace: "default",
					Labels:    map[string]string{},
					Status: clustercache.PodStatus{
						Phase: v1.PodRunning,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name:         "container1",
								RestartCount: 0,
								State: v1.ContainerState{
									Running: &v1.ContainerStateRunning{},
								},
							},
							{
								Name:         "container2",
								RestartCount: 1,
								State: v1.ContainerState{
									Running: &v1.ContainerStateRunning{},
								},
							},
						},
					},
					Spec: clustercache.PodSpec{
						NodeName: "node2",
						Containers: []clustercache.Container{
							{
								Name: "container1",
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU: resource.MustParse("50m"),
									},
									Limits: v1.ResourceList{
										v1.ResourceCPU: resource.MustParse("100m"),
									},
								},
							},
							{
								Name: "container2",
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceMemory: resource.MustParse("64Mi"),
									},
									Limits: v1.ResourceList{
										v1.ResourceMemory: resource.MustParse("128Mi"),
									},
								},
							},
						},
					},
				},
			},
			disabledMetrics: []string{},
			expectedCount:   16, // 5 phases + 1 labels + 2 restarts + 2 running + 2 requests + 4 limits
		},
		{
			name: "metrics disabled",
			pods: []*clustercache.Pod{
				{
					UID:       types.UID("pod-uid-6"),
					Name:      "test-pod",
					Namespace: "default",
					Labels:    map[string]string{"app": "test"},
					Status: clustercache.PodStatus{
						Phase: v1.PodRunning,
					},
					Spec: clustercache.PodSpec{},
				},
			},
			disabledMetrics: []string{"kube_pod_labels", "kube_pod_status_phase"},
			expectedCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := MetricsConfig{
				DisabledMetrics: tt.disabledMetrics,
			}
			kpc := KubePodCollector{
				KubeClusterCache: NewFakePodCache(tt.pods),
				metricsConfig:    mc,
			}

			ch := make(chan prometheus.Metric, 30)
			kpc.Collect(ch)
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

func TestKubePodLabelsMetric(t *testing.T) {
	labelNames := []string{"label_app", "label_env"}
	labelValues := []string{"webapp", "production"}

	metric := newKubePodLabelsMetric("kube_pod_labels", "prod", "web-pod", "pod-uid", labelNames, labelValues)

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
		"label_app": "webapp",
		"label_env": "production",
		"namespace": "prod",
		"pod":       "web-pod",
		"uid":       "pod-uid",
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

func TestKubePodContainerStatusRestartsTotalMetric(t *testing.T) {
	metric := newKubePodContainerStatusRestartsTotalMetric("kube_pod_container_status_restarts_total", "default", "test-pod", "pod-uid", "app-container", 3.0)

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

	if dtoMetric.Counter == nil {
		t.Error("Expected counter metric")
	}

	if *dtoMetric.Counter.Value != 3.0 {
		t.Errorf("Expected counter value 3.0, got %f", *dtoMetric.Counter.Value)
	}
}

func TestKubePodContainerStatusTerminatedReasonMetric(t *testing.T) {
	metric := newKubePodContainerStatusTerminatedReasonMetric("kube_pod_container_status_terminated_reason", "default", "crashed-pod", "pod-uid", "failing-container", "Error")

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

	// Check for reason label
	hasReason := false
	for _, label := range dtoMetric.Label {
		if *label.Name == "reason" && *label.Value == "Error" {
			hasReason = true
			break
		}
	}
	if !hasReason {
		t.Error("Expected reason label with value 'Error'")
	}
}

func TestKubePodStatusPhaseMetric(t *testing.T) {
	metric := newKubePodStatusPhaseMetric("kube_pod_status_phase", "default", "test-pod", "pod-uid", "Running", 1.0)

	var dtoMetric dto.Metric
	err := metric.Write(&dtoMetric)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Error("Expected gauge metric")
	}

	// Check phase label
	hasPhase := false
	for _, label := range dtoMetric.Label {
		if *label.Name == "phase" && *label.Value == "Running" {
			hasPhase = true
			break
		}
	}
	if !hasPhase {
		t.Error("Expected phase label with value 'Running'")
	}
}

func TestKubePodContainerStatusRunningMetric(t *testing.T) {
	metric := newKubePodContainerStatusRunningMetric("kube_pod_container_status_running", "default", "running-pod", "pod-uid", "web-container")

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
}

func TestKubePodContainerResourceRequestsMetric(t *testing.T) {
	metric := newKubePodContainerResourceRequestsMetric("kube_pod_container_resource_requests", "default", "test-pod", "pod-uid", "container1", "node1", "cpu", "core", 0.1)

	var dtoMetric dto.Metric
	err := metric.Write(&dtoMetric)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Error("Expected gauge metric")
	}

	if *dtoMetric.Gauge.Value != 0.1 {
		t.Errorf("Expected gauge value 0.1, got %f", *dtoMetric.Gauge.Value)
	}

	// Verify all labels
	expectedLabels := map[string]string{
		"namespace": "default",
		"pod":       "test-pod",
		"container": "container1",
		"uid":       "pod-uid",
		"node":      "node1",
		"resource":  "cpu",
		"unit":      "core",
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

func TestKubePodContainerResourceLimitsMetric(t *testing.T) {
	metric := newKubePodContainerResourceLimitsMetric("kube_pod_container_resource_limits", "default", "test-pod", "pod-uid", "container1", "node1", "memory", "byte", 268435456)

	var dtoMetric dto.Metric
	err := metric.Write(&dtoMetric)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Error("Expected gauge metric")
	}

	if *dtoMetric.Gauge.Value != 268435456 {
		t.Errorf("Expected gauge value 268435456, got %f", *dtoMetric.Gauge.Value)
	}
}

func TestKubePodContainerResourceLimitsCPUCoresMetric(t *testing.T) {
	metric := newKubePodContainerResourceLimitsCPUCoresMetric("kube_pod_container_resource_limits_cpu_cores", "default", "test-pod", "pod-uid", "container1", "node1", 2.0)

	var dtoMetric dto.Metric
	err := metric.Write(&dtoMetric)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Error("Expected gauge metric")
	}

	if *dtoMetric.Gauge.Value != 2.0 {
		t.Errorf("Expected gauge value 2.0, got %f", *dtoMetric.Gauge.Value)
	}
}

func TestKubePodContainerResourceLimitsMemoryBytesMetric(t *testing.T) {
	metric := newKubePodContainerResourceLimitsMemoryBytesMetric("kube_pod_container_resource_limits_memory_bytes", "default", "test-pod", "pod-uid", "container1", "node1", 536870912)

	var dtoMetric dto.Metric
	err := metric.Write(&dtoMetric)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Error("Expected gauge metric")
	}

	if *dtoMetric.Gauge.Value != 536870912 {
		t.Errorf("Expected gauge value 536870912, got %f", *dtoMetric.Gauge.Value)
	}
}

func TestKubePodOwnerMetric(t *testing.T) {
	metric := newKubePodOwnerMetric("kube_pod_owner", "default", "test-pod", "test-uid", "test-replicaset", "ReplicaSet", true)

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

	// Verify owner-specific labels
	expectedLabels := map[string]string{
		"namespace":           "default",
		"pod":                 "test-pod",
		"uid":                 "test-uid",
		"owner_name":          "test-replicaset",
		"owner_kind":          "ReplicaSet",
		"owner_is_controller": "true",
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

func TestPodPhaseMetrics(t *testing.T) {
	// Test that all pod phases generate correct metrics
	pod := &clustercache.Pod{
		UID:       types.UID("phase-test-uid"),
		Name:      "phase-test-pod",
		Namespace: "default",
		Labels:    map[string]string{},
		Status: clustercache.PodStatus{
			Phase: v1.PodRunning,
		},
		Spec: clustercache.PodSpec{},
	}

	mc := MetricsConfig{
		DisabledMetrics: []string{"kube_pod_labels"}, // Only test phase metrics
	}
	kpc := KubePodCollector{
		KubeClusterCache: NewFakePodCache([]*clustercache.Pod{pod}),
		metricsConfig:    mc,
	}

	ch := make(chan prometheus.Metric, 10)
	kpc.Collect(ch)
	close(ch)

	phaseMetrics := make(map[string]float64)
	for metric := range ch {
		var dtoMetric dto.Metric
		metric.Write(&dtoMetric)

		for _, label := range dtoMetric.Label {
			if *label.Name == "phase" {
				phaseMetrics[*label.Value] = *dtoMetric.Gauge.Value
			}
		}
	}

	// Verify all phases are emitted
	expectedPhases := map[string]float64{
		"Pending":   0.0,
		"Succeeded": 0.0,
		"Failed":    0.0,
		"Unknown":   0.0,
		"Running":   1.0, // Only Running should be 1
	}

	for phase, expectedValue := range expectedPhases {
		if actualValue, ok := phaseMetrics[phase]; !ok {
			t.Errorf("Missing phase metric for %s", phase)
		} else if actualValue != expectedValue {
			t.Errorf("Phase %s: expected value %f, got %f", phase, expectedValue, actualValue)
		}
	}
}

// FakePodCache implements ClusterCache interface for testing
type FakePodCache struct {
	clustercache.ClusterCache
	pods []*clustercache.Pod
}

func (f FakePodCache) GetAllPods() []*clustercache.Pod {
	return f.pods
}

func NewFakePodCache(pods []*clustercache.Pod) FakePodCache {
	return FakePodCache{
		pods: pods,
	}
}
