package prometheus

import (
	"context"
	"testing"
	"time"

	modelpb "github.com/opencost/opencost/core/pkg/model/pb"
	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewSourceValidation(t *testing.T) {
	if _, err := NewSource(Config{}); err == nil {
		t.Fatalf("expected error when metrics client is nil")
	}

	if _, err := NewSource(Config{
		Metrics: &fakeMetrics{},
	}); err == nil {
		t.Fatalf("expected error when cluster identifiers are missing")
	}
}

func TestSourceComputeModel(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	window := &modelpb.Window{
		Resolution: modelpb.Resolution_RESOLUTION_1H,
		Start:      timestamppb.New(start),
	}

	metrics := &fakeMetrics{
		nodeLabels: []*source.NodeLabelsResult{
			{
				UID:     "node-uid",
				Cluster: "",
				Node:    "node-a",
				Labels: map[string]string{
					"kubernetes_io/hostname": "node-a",
				},
			},
		},
		namespaceLabels: []*source.NamespaceLabelsResult{
			{
				UID:       "ns-uid",
				Cluster:   "",
				Namespace: "ns-a",
				Labels: map[string]string{
					"team": "platform",
				},
			},
		},
		podLabels: []*source.PodLabelsResult{
			{
				UID:       "pod-uid",
				Cluster:   "",
				Namespace: "ns-a",
				Pod:       "pod-a",
				Labels: map[string]string{
					"app": "demo",
				},
			},
		},
		podAnnotations: []*source.PodAnnotationsResult{
			{
				UID:         "pod-uid",
				Cluster:     "",
				Namespace:   "ns-a",
				Pod:         "pod-a",
				Annotations: map[string]string{"annotation": "value"},
			},
		},
	}

	src, err := NewSource(Config{
		Metrics:     metrics,
		ClusterID:   "cluster-1",
		ClusterName: "cluster-name",
		Account:     "acct",
		Provider:    kubepb.Provider_PROVIDER_GCP,
	})
	if err != nil {
		t.Fatalf("unexpected error creating source: %v", err)
	}

	model, err := src.ComputeModel(context.Background(), window)
	if err != nil {
		t.Fatalf("unexpected compute error: %v", err)
	}

	if model.Window == window {
		t.Fatalf("expected model window to be cloned")
	}

	if model.Cluster == nil {
		t.Fatalf("expected cluster information")
	}
	if model.Cluster.ID != "cluster-1" {
		t.Fatalf("unexpected cluster ID: %s", model.Cluster.ID)
	}
	if model.Cluster.Window == window || model.Cluster.Window == model.Window {
		t.Fatalf("cluster window should be independently cloned")
	}
	if !proto.Equal(model.Cluster.Window, window) {
		t.Fatalf("cluster window should match requested window")
	}

	node, ok := model.Nodes["node-uid"]
	if !ok {
		t.Fatalf("expected node data to be populated")
	}
	if node.ClusterID != "cluster-1" || node.Name != "node-a" {
		t.Fatalf("unexpected node data: %#v", node)
	}

	namespace, ok := model.Namespaces["ns-uid"]
	if !ok {
		t.Fatalf("expected namespace to be populated")
	}
	if namespace.ClusterID != "cluster-1" || namespace.Name != "ns-a" {
		t.Fatalf("unexpected namespace data: %#v", namespace)
	}

	pod, ok := model.Pods["pod-uid"]
	if !ok {
		t.Fatalf("expected pod to be populated")
	}
	if pod.NamespaceID != "ns-uid" {
		t.Fatalf("expected pod namespace ID to map to ns-uid, got %q", pod.NamespaceID)
	}
	if pod.Name != "pod-a" {
		t.Fatalf("unexpected pod name: %s", pod.Name)
	}
	if got := pod.Labels["app"]; got != "demo" {
		t.Fatalf("expected pod label 'app=demo', got %q", got)
	}
	if got := pod.Annotations["annotation"]; got != "value" {
		t.Fatalf("expected pod annotation value, got %q", got)
	}

	if len(model.Containers) != 0 {
		t.Fatalf("containers should not be populated yet")
	}

	if len(metrics.starts) == 0 || !metrics.starts[0].Equal(start) {
		t.Fatalf("expected metrics queries to use start time %v, got %v", start, metrics.starts)
	}
	if len(metrics.ends) == 0 || !metrics.ends[0].Equal(start.Add(time.Hour)) {
		t.Fatalf("expected metrics queries to use end time %v, got %v", start.Add(time.Hour), metrics.ends)
	}
}

type fakeMetrics struct {
	nodeLabels      []*source.NodeLabelsResult
	namespaceLabels []*source.NamespaceLabelsResult
	podLabels       []*source.PodLabelsResult
	podAnnotations  []*source.PodAnnotationsResult

	starts []time.Time
	ends   []time.Time
}

func (f *fakeMetrics) record(start, end time.Time) {
	f.starts = append(f.starts, start)
	f.ends = append(f.ends, end)
}

func (f *fakeMetrics) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	f.record(start, end)
	return source.NewFutureFrom(f.nodeLabels)
}

func (f *fakeMetrics) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	f.record(start, end)
	return source.NewFutureFrom(f.namespaceLabels)
}

func (f *fakeMetrics) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	f.record(start, end)
	return source.NewFutureFrom(f.podLabels)
}

func (f *fakeMetrics) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	f.record(start, end)
	return source.NewFutureFrom(f.podAnnotations)
}

var _ MetricsClient = (*fakeMetrics)(nil)
