package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/types"
)

type mockNamespaceCache struct {
	clustercache.ClusterCache
	namespaces []*clustercache.Namespace
}

func (m mockNamespaceCache) GetAllNamespaces() []*clustercache.Namespace {
	return m.namespaces
}

func TestKubecostNamespaceCollector_Collect(t *testing.T) {
	// Test with namespace that has annotations
	cache := mockNamespaceCache{
		namespaces: []*clustercache.Namespace{
			{
				Name:        "test-ns",
				UID:         types.UID("test-uid"),
				Annotations: map[string]string{"team": "backend"},
			},
		},
	}

	collector := KubecostNamespaceCollector{
		KubeClusterCache: cache,
		metricsConfig:    MetricsConfig{},
	}

	ch := make(chan prometheus.Metric, 10)
	go func() {
		collector.Collect(ch)
		close(ch)
	}()

	count := 0
	for range ch {
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 metric, got %d", count)
	}
}

func TestKubeNamespaceCollector_Collect(t *testing.T) {
	// Test with namespace that has labels
	cache := mockNamespaceCache{
		namespaces: []*clustercache.Namespace{
			{
				Name:   "test-ns",
				UID:    types.UID("test-uid"),
				Labels: map[string]string{"env": "prod"},
			},
		},
	}

	collector := KubeNamespaceCollector{
		KubeClusterCache: cache,
		metricsConfig:    MetricsConfig{},
	}

	ch := make(chan prometheus.Metric, 10)
	go func() {
		collector.Collect(ch)
		close(ch)
	}()

	count := 0
	for range ch {
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 metric, got %d", count)
	}
}

func TestNamespaceAnnotationsMetric_Write(t *testing.T) {
	metric := newNamespaceAnnotationsMetric(
		"test_metric",
		"test-ns",
		"test-uid",
		[]string{"team"},
		[]string{"backend"},
	)

	pbMetric := &dto.Metric{}
	err := metric.Write(pbMetric)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if pbMetric.Gauge == nil || *pbMetric.Gauge.Value != 1.0 {
		t.Error("Expected gauge value 1.0")
	}

	if len(pbMetric.Label) != 3 { // team + namespace + uid
		t.Errorf("Expected 3 labels, got %d", len(pbMetric.Label))
	}

	// Verify UID label exists and has correct value
	foundUID := false
	for _, label := range pbMetric.Label {
		if *label.Name == "uid" && *label.Value == "test-uid" {
			foundUID = true
			break
		}
	}
	if !foundUID {
		t.Error("Expected uid label with value 'test-uid' not found")
	}
}

func TestKubeNamespaceLabelsMetric_Write(t *testing.T) {
	metric := newKubeNamespaceLabelsMetric(
		"test_metric",
		"test-ns",
		"test-uid",
		[]string{"env"},
		[]string{"prod"},
	)

	pbMetric := &dto.Metric{}
	err := metric.Write(pbMetric)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if pbMetric.Gauge == nil || *pbMetric.Gauge.Value != 1.0 {
		t.Error("Expected gauge value 1.0")
	}

	if len(pbMetric.Label) != 3 { // env + namespace + uid
		t.Errorf("Expected 3 labels, got %d", len(pbMetric.Label))
	}

	// Verify UID label exists and has correct value
	foundUID := false
	for _, label := range pbMetric.Label {
		if *label.Name == "uid" && *label.Value == "test-uid" {
			foundUID = true
			break
		}
	}
	if !foundUID {
		t.Error("Expected uid label with value 'test-uid' not found")
	}
}

func TestKubecostNamespaceCollector_Describe(t *testing.T) {
	collector := KubecostNamespaceCollector{metricsConfig: MetricsConfig{}}

	ch := make(chan *prometheus.Desc, 1)
	go func() {
		collector.Describe(ch)
		close(ch)
	}()

	count := 0
	for range ch {
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 descriptor, got %d", count)
	}
}

func TestKubeNamespaceCollector_Describe(t *testing.T) {
	collector := KubeNamespaceCollector{metricsConfig: MetricsConfig{}}

	ch := make(chan *prometheus.Desc, 1)
	go func() {
		collector.Describe(ch)
		close(ch)
	}()

	count := 0
	for range ch {
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 descriptor, got %d", count)
	}
}
