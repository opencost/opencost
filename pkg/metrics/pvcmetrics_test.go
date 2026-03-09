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

func collectMetrics(collector KubePVCCollector) []prometheus.Metric {
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

func TestKubePVCCollector_Describe(t *testing.T) {
	collector := KubePVCCollector{metricsConfig: MetricsConfig{}}
	ch := make(chan *prometheus.Desc, 5)
	go func() {
		defer close(ch)
		collector.Describe(ch)
	}()

	count := 0
	for range ch {
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 metrics described, got %d", count)
	}
}

func TestKubePVCCollector_Collect(t *testing.T) {
	storageSize := resource.MustParse("1Gi")
	pvc := &clustercache.PersistentVolumeClaim{
		UID:       types.UID("test-uid"),
		Name:      "test-pvc",
		Namespace: "default",
		Spec: v1.PersistentVolumeClaimSpec{
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceStorage: storageSize},
			},
		},
	}

	cache := NewFakePVCCache([]*clustercache.PersistentVolumeClaim{pvc})
	collector := KubePVCCollector{
		KubeClusterCache: cache,
		metricsConfig:    MetricsConfig{},
	}

	metrics := collectMetrics(collector)
	if len(metrics) != 2 {
		t.Errorf("Expected 2 metrics, got %d", len(metrics))
	}

	// Verify UID label exists in metrics
	for _, metric := range metrics {
		var m dto.Metric
		if err := metric.Write(&m); err != nil {
			t.Errorf("Error writing metric: %v", err)
		}

		hasUID := false
		for _, label := range m.Label {
			if *label.Name == "uid" && *label.Value == "test-uid" {
				hasUID = true
				break
			}
		}
		if !hasUID {
			t.Error("Metric missing UID label")
		}
	}
}

func TestKubePVCMetrics_UIDLabel(t *testing.T) {
	metric := newKubePVCResourceRequestsStorageBytesMetric(
		"test_metric", "test-pvc", "test-namespace", "test-uid", 1000.0,
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

type FakePVCCache struct {
	clustercache.ClusterCache
	pvcs []*clustercache.PersistentVolumeClaim
}

func (f FakePVCCache) GetAllPersistentVolumeClaims() []*clustercache.PersistentVolumeClaim {
	return f.pvcs
}

func NewFakePVCCache(pvcs []*clustercache.PersistentVolumeClaim) FakePVCCache {
	return FakePVCCache{
		pvcs: pvcs,
	}
}
