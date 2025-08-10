package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
)

func TestKubePVCapacityBytesMetric(t *testing.T) {
	metric := newKubePVCapacityBytesMetric(
		"kube_persistentvolume_capacity_bytes",
		"test-pv",
		"pv-test-uid-123",
		107374182400,
	)

	if metric.pv != "test-pv" {
		t.Errorf("Expected pv 'test-pv', got %s", metric.pv)
	}
	if metric.uid != "pv-test-uid-123" {
		t.Errorf("Expected UID 'pv-test-uid-123', got %s", metric.uid)
	}
	if metric.value != 107374182400 {
		t.Errorf("Expected value 107374182400, got %f", metric.value)
	}
}

func TestKubePVStatusPhaseMetric(t *testing.T) {
	metric := newKubePVStatusPhaseMetric(
		"kube_persistentvolume_status_phase",
		"test-pv",
		"pv-test-uid-456",
		"Bound",
		1.0,
	)

	if metric.pv != "test-pv" {
		t.Errorf("Expected pv 'test-pv', got %s", metric.pv)
	}
	if metric.uid != "pv-test-uid-456" {
		t.Errorf("Expected UID 'pv-test-uid-456', got %s", metric.uid)
	}
	if metric.phase != "Bound" {
		t.Errorf("Expected phase 'Bound', got %s", metric.phase)
	}
	if metric.value != 1.0 {
		t.Errorf("Expected value 1.0, got %f", metric.value)
	}
}

func TestKubecostPVInfoMetric(t *testing.T) {
	metric := newKubecostPVInfoMetric(
		"kubecost_pv_info",
		"test-pv",
		"gp2",
		"vol-123456789",
		"pv-test-uid-789",
		1.0,
	)

	if metric.pv != "test-pv" {
		t.Errorf("Expected pv 'test-pv', got %s", metric.pv)
	}
	if metric.uid != "pv-test-uid-789" {
		t.Errorf("Expected UID 'pv-test-uid-789', got %s", metric.uid)
	}
	if metric.storageClass != "gp2" {
		t.Errorf("Expected storageClass 'gp2', got %s", metric.storageClass)
	}
	if metric.providerId != "vol-123456789" {
		t.Errorf("Expected providerId 'vol-123456789', got %s", metric.providerId)
	}
	if metric.value != 1.0 {
		t.Errorf("Expected value 1.0, got %f", metric.value)
	}
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
