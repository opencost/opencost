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

func collectPVMetrics(collector KubePVCollector) []prometheus.Metric {
	ch := make(chan prometheus.Metric, 10)
	go func() {
		defer close(ch)
		collector.Collect(ch)
	}()

	var metrics []prometheus.Metric
	for metric := range ch {
		metrics = append(metrics, metric)
	}
	return metrics
}

func TestKubePVCollector_Describe(t *testing.T) {
	collector := KubePVCollector{metricsConfig: MetricsConfig{}}
	ch := make(chan *prometheus.Desc, 5)
	go func() {
		defer close(ch)
		collector.Describe(ch)
	}()

	count := 0
	for range ch {
		count++
	}

	if count != 3 {
		t.Errorf("Expected 3 metrics described, got %d", count)
	}
}

func TestKubePVCollector_Collect(t *testing.T) {
	storageSize := resource.MustParse("10Gi")
	pv := &clustercache.PersistentVolume{
		UID:  types.UID("test-pv-uid"),
		Name: "test-pv",
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceStorage: storageSize,
			},
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeBound,
		},
	}

	cache := NewFakePVCache([]*clustercache.PersistentVolume{pv})
	collector := KubePVCollector{
		KubeClusterCache: cache,
		metricsConfig:    MetricsConfig{},
	}

	metrics := collectPVMetrics(collector)
	if len(metrics) != 7 { // 1 capacity + 5 phase + 1 info
		t.Errorf("Expected 7 metrics, got %d", len(metrics))
	}

	// Verify UID label exists in metrics
	for _, metric := range metrics {
		var m dto.Metric
		if err := metric.Write(&m); err != nil {
			t.Errorf("Error writing metric: %v", err)
		}

		hasUID := false
		for _, label := range m.Label {
			if *label.Name == "uid" && *label.Value == "test-pv-uid" {
				hasUID = true
				break
			}
		}
		if !hasUID {
			t.Error("Metric missing UID label")
		}
	}
}

func TestKubePVMetrics_UIDLabel(t *testing.T) {
	metric := newKubePVCapacityBytesMetric(
		"test_metric", "test-pv", "test-uid", 1000.0,
	)

	var m dto.Metric
	if err := metric.Write(&m); err != nil {
		t.Fatalf("Error writing metric: %v", err)
	}

	// Verify UID label exists
	for _, label := range m.Label {
		if *label.Name == "uid" && *label.Value == "test-uid" {
			return
		}
	}
	t.Error("UID label not found in metric")
}

type FakePVCache struct {
	clustercache.ClusterCache
	pvs []*clustercache.PersistentVolume
}

func (f FakePVCache) GetAllPersistentVolumes() []*clustercache.PersistentVolume {
	return f.pvs
}

func NewFakePVCache(pvs []*clustercache.PersistentVolume) FakePVCache {
	return FakePVCache{
		pvs: pvs,
	}
}
