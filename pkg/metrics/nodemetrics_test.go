package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
)

func TestKubeNodeStatusCapacityMemoryBytesMetric(t *testing.T) {
	metric := newKubeNodeStatusCapacityMemoryBytesMetric(
		"kube_node_status_capacity_memory_bytes",
		"test-node",
		"node-test-uid-123",
		8589934592,
	)

	if metric.node != "test-node" {
		t.Errorf("Expected node 'test-node', got %s", metric.node)
	}
	if metric.uid != "node-test-uid-123" {
		t.Errorf("Expected UID 'node-test-uid-123', got %s", metric.uid)
	}
	if metric.bytes != 8589934592 {
		t.Errorf("Expected bytes 8589934592, got %f", metric.bytes)
	}
}

func TestKubeNodeStatusCapacityCPUCoresMetric(t *testing.T) {
	metric := newKubeNodeStatusCapacityCPUCoresMetric(
		"kube_node_status_capacity_cpu_cores",
		"test-node",
		"node-test-uid-456",
		4.0,
	)

	if metric.node != "test-node" {
		t.Errorf("Expected node 'test-node', got %s", metric.node)
	}
	if metric.uid != "node-test-uid-456" {
		t.Errorf("Expected UID 'node-test-uid-456', got %s", metric.uid)
	}
	if metric.cores != 4.0 {
		t.Errorf("Expected cores 4.0, got %f", metric.cores)
	}
}

func TestKubeNodeLabelsMetric(t *testing.T) {
	metric := newKubeNodeLabelsMetric(
		"test-node",
		"node-test-uid-789",
		"kube_node_labels",
		[]string{"label_kubernetes_io_arch", "label_zone"},
		[]string{"amd64", "us-east-1a"},
	)

	if metric.node != "test-node" {
		t.Errorf("Expected node 'test-node', got %s", metric.node)
	}
	if metric.uid != "node-test-uid-789" {
		t.Errorf("Expected UID 'node-test-uid-789', got %s", metric.uid)
	}
	if len(metric.labelNames) != 2 {
		t.Errorf("Expected 2 label names, got %d", len(metric.labelNames))
	}
	if len(metric.labelValues) != 2 {
		t.Errorf("Expected 2 label values, got %d", len(metric.labelValues))
	}
}

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
