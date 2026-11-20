package costmodel

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

// A container series can reach GetContainerMetricVector with an empty node
// name: DecodeContainerMetricResult ignores the error from GetNode, falls back
// to the instance label, and ignores the error from that too, so a series
// carrying neither label decodes to Node == "". ComputeCostData feeds those
// results straight into GetContainerMetricVector, which returns on the first
// error, so one such series must not discard the whole batch.
func TestGetContainerMetricVector_EmptyNodeNameKeepsBatch(t *testing.T) {
	qrs := []*source.ContainerMetricResult{
		{
			Cluster:   "cluster-one",
			Node:      "node-1",
			Namespace: "kube-system",
			Pod:       "pod-a",
			Container: "container-a",
			Data:      []*util.Vector{{Timestamp: 1, Value: 1}},
		},
		{
			Cluster:   "cluster-one",
			Node:      "",
			Namespace: "kube-system",
			Pod:       "pod-b",
			Container: "container-b",
			Data:      []*util.Vector{{Timestamp: 1, Value: 2}},
		},
	}

	got, err := GetContainerMetricVector(qrs, "cluster-one")
	if err != nil {
		t.Fatalf("GetContainerMetricVector returned an error for a series with no node name: %s", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 container entries, got %d", len(got))
	}

	withNode := containerMetricKey("kube-system", "pod-a", "container-a", "node-1", "cluster-one")
	if _, ok := got[withNode]; !ok {
		t.Errorf("entry for the node-bearing container is missing: %q", withNode)
	}

	noNode := containerMetricKey("kube-system", "pod-b", "container-b", "", "cluster-one")
	if _, ok := got[noNode]; !ok {
		t.Errorf("entry for the container with no node name is missing: %q", noNode)
	}
}

// NewContainerMetricFrom is the sink GetContainerMetricVector calls. Its
// sibling NewContainerMetricFromResult keeps a result whose node label is
// absent, recording an empty node name, and this one carries a dead
// `if nodeName == ""` branch saying it means to do the same.
func TestNewContainerMetricFrom_EmptyNodeNameIsKept(t *testing.T) {
	cm, err := NewContainerMetricFrom(&source.ContainerMetricResult{
		Cluster:   "cluster-one",
		Node:      "",
		Namespace: "kube-system",
		Pod:       "pod-b",
		Container: "container-b",
	}, "cluster-one")
	if err != nil {
		t.Fatalf("NewContainerMetricFrom rejected an empty node name: %s", err)
	}
	if cm.NodeName != "" {
		t.Errorf("NodeName = %q, want empty", cm.NodeName)
	}
}

// A genuinely malformed node name must still be rejected, so the fix cannot be
// "stop validating".
func TestNewContainerMetricFrom_InvalidNodeNameStillRejected(t *testing.T) {
	_, err := NewContainerMetricFrom(&source.ContainerMetricResult{
		Cluster:   "cluster-one",
		Node:      "10.0.0.1:443",
		Namespace: "kube-system",
		Pod:       "pod-c",
		Container: "container-c",
	}, "cluster-one")
	if err == nil {
		t.Fatal("expected an error for a node name set from the instance label, got nil")
	}
}
