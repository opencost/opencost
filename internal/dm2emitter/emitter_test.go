//go:build dm2emitter

package dm2emitter

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	dm2pb "github.com/opencost/opencost/protos/dm2"
	"google.golang.org/protobuf/proto"
)

// fakeInv is a mock implementation of Inventory for testing
type fakeInv struct{}

func (f *fakeInv) ListNamespaces(context.Context) []Namespace {
	return []Namespace{
		{UID: "ns-1", Name: "default"},
		{UID: "ns-2", Name: "kube-system"},
	}
}

func (f *fakeInv) ListWorkloadsByNamespace(ctx context.Context, nsUID string) []Workload {
	if nsUID == "ns-1" {
		return []Workload{
			{UID: "wl-1", Name: "web", Kind: "Deployment", NamespaceUID: nsUID},
			{UID: "wl-2", Name: "db", Kind: "StatefulSet", NamespaceUID: nsUID},
		}
	}
	return []Workload{
		{UID: "wl-3", Name: "coredns", Kind: "Deployment", NamespaceUID: nsUID},
	}
}

func (f *fakeInv) ListPodsByWorkload(ctx context.Context, wlUID string) []Pod {
	switch wlUID {
	case "wl-1":
		return []Pod{
			{UID: "pod-1", Name: "web-abc", NodeUID: "node-1", WorkloadUID: wlUID},
			{UID: "pod-2", Name: "web-def", NodeUID: "node-2", WorkloadUID: wlUID},
		}
	case "wl-2":
		return []Pod{
			{UID: "pod-3", Name: "db-0", NodeUID: "node-1", WorkloadUID: wlUID},
		}
	case "wl-3":
		return []Pod{
			{UID: "pod-4", Name: "coredns-xyz", NodeUID: "node-1", WorkloadUID: wlUID},
		}
	}
	return nil
}

func (f *fakeInv) ListContainersByPod(ctx context.Context, podUID string) []Container {
	switch podUID {
	case "pod-1":
		return []Container{
			{UID: "pod-1/web", Name: "web", Image: "nginx:latest", PodUID: podUID},
			{UID: "pod-1/sidecar", Name: "sidecar", Image: "envoy:latest", PodUID: podUID},
		}
	case "pod-2":
		return []Container{
			{UID: "pod-2/web", Name: "web", Image: "nginx:latest", PodUID: podUID},
		}
	case "pod-3":
		return []Container{
			{UID: "pod-3/postgres", Name: "postgres", Image: "postgres:13", PodUID: podUID},
		}
	case "pod-4":
		return []Container{
			{UID: "pod-4/coredns", Name: "coredns", Image: "coredns/coredns:1.8.6", PodUID: podUID},
		}
	}
	return nil
}

func (f *fakeInv) ClusterUID(context.Context) string  { return "cluster-1" }
func (f *fakeInv) ClusterName(context.Context) string { return "test-cluster" }

func TestEmitterWritesFile(t *testing.T) {
	tmp := t.TempDir()
	e := New(&fakeInv{}, tmp, 10*time.Second, true)
	
	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	
	// Check that a file was written
	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("no dm2 file written")
	}
	
	// Read and decompress the file
	filename := filepath.Join(tmp, entries[0].Name())
	bz, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	
	gr, err := gzip.NewReader(bytes.NewReader(bz))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	raw, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("read gzip: %v", err)
	}
	
	// Unmarshal and verify the protobuf content
	var cluster dm2pb.Cluster
	if err := proto.Unmarshal(raw, &cluster); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	
	// Verify cluster info
	if cluster.GetClusterUid() != "cluster-1" {
		t.Errorf("expected cluster UID 'cluster-1', got %s", cluster.GetClusterUid())
	}
	if cluster.GetClusterName() != "test-cluster" {
		t.Errorf("expected cluster name 'test-cluster', got %s", cluster.GetClusterName())
	}
	
	// Verify namespaces
	if len(cluster.GetNamespaces()) != 2 {
		t.Fatalf("expected 2 namespaces, got %d", len(cluster.GetNamespaces()))
	}
	
	// Count all entities
	totalWorkloads := 0
	totalPods := 0
	totalContainers := 0
	
	for _, ns := range cluster.GetNamespaces() {
		totalWorkloads += len(ns.GetWorkloads())
		for _, wl := range ns.GetWorkloads() {
			totalPods += len(wl.GetPods())
			for _, pod := range wl.GetPods() {
				totalContainers += len(pod.GetContainers())
			}
		}
	}
	
	if totalWorkloads != 3 {
		t.Errorf("expected 3 workloads total, got %d", totalWorkloads)
	}
	if totalPods != 4 {
		t.Errorf("expected 4 pods total, got %d", totalPods)
	}
	if totalContainers != 5 {
		t.Errorf("expected 5 containers total, got %d", totalContainers)
	}
}

func TestEmitterCreatesDirectory(t *testing.T) {
	tmp := t.TempDir()
	subdir := filepath.Join(tmp, "subdir", "dm2output")
	
	e := New(&fakeInv{}, subdir, 10*time.Second, true)
	
	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	
	// Check that directory was created
	if _, err := os.Stat(subdir); os.IsNotExist(err) {
		t.Fatalf("expected directory %s to be created", subdir)
	}
	
	// Check that a file was written
	entries, err := os.ReadDir(subdir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("no dm2 file written")
	}
}

// Benchmark to compare JSON vs Protobuf size
func BenchmarkProtobufVsJSON(b *testing.B) {
	ctx := context.Background()
	inv := &fakeInv{}
	
	// Build the cluster structure
	cluster := &dm2pb.Cluster{
		ClusterUid:  inv.ClusterUID(ctx),
		ClusterName: inv.ClusterName(ctx),
	}
	
	for _, ns := range inv.ListNamespaces(ctx) {
		pbNs := &dm2pb.Namespace{Uid: ns.UID, Name: ns.Name}
		cluster.Namespaces = append(cluster.Namespaces, pbNs)
		
		for _, wl := range inv.ListWorkloadsByNamespace(ctx, ns.UID) {
			pbWl := &dm2pb.Workload{Uid: wl.UID, Name: wl.Name, Kind: wl.Kind}
			pbNs.Workloads = append(pbNs.Workloads, pbWl)
			
			for _, pod := range inv.ListPodsByWorkload(ctx, wl.UID) {
				pbPod := &dm2pb.Pod{Uid: pod.UID, Name: pod.Name, NodeUid: pod.NodeUID}
				pbWl.Pods = append(pbWl.Pods, pbPod)
				
				for _, ctr := range inv.ListContainersByPod(ctx, pod.UID) {
					pbCtr := &dm2pb.Container{Uid: ctr.UID, Name: ctr.Name, Image: ctr.Image}
					pbPod.Containers = append(pbPod.Containers, pbCtr)
				}
			}
		}
	}
	
	// Measure protobuf size
	pbData, err := proto.Marshal(cluster)
	if err != nil {
		b.Fatalf("marshal protobuf: %v", err)
	}
	
	var pbGz bytes.Buffer
	pbZw := gzip.NewWriter(&pbGz)
	if _, err := pbZw.Write(pbData); err != nil {
		b.Fatalf("gzip protobuf: %v", err)
	}
	if err := pbZw.Close(); err != nil {
		b.Fatalf("close gzip: %v", err)
	}
	
	b.Logf("Protobuf raw size: %d bytes", len(pbData))
	b.Logf("Protobuf gzipped size: %d bytes", pbGz.Len())
	
	// Note: JSON comparison would require a JSON representation of the same data
	// For now, we're just demonstrating the protobuf sizes
}