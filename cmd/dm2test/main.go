//go:build dm2emitter

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/opencost/opencost/internal/dm2emitter"
)

// testInventory provides mock data for testing
type testInventory struct{}

func (t *testInventory) ListNamespaces(ctx context.Context) []dm2emitter.Namespace {
	fmt.Println("ListNamespaces called")
	return []dm2emitter.Namespace{
		{UID: "ns-uid-1", Name: "default"},
		{UID: "ns-uid-2", Name: "kube-system"},
	}
}

func (t *testInventory) ListWorkloadsByNamespace(ctx context.Context, nsUID string) []dm2emitter.Workload {
	fmt.Printf("ListWorkloadsByNamespace called for %s\n", nsUID)
	if nsUID == "ns-uid-1" {
		return []dm2emitter.Workload{
			{UID: "wl-uid-1", Name: "nginx-deployment", Kind: "Deployment", NamespaceUID: nsUID},
		}
	}
	return nil
}

func (t *testInventory) ListPodsByWorkload(ctx context.Context, wlUID string) []dm2emitter.Pod {
	fmt.Printf("ListPodsByWorkload called for %s\n", wlUID)
	if wlUID == "wl-uid-1" {
		return []dm2emitter.Pod{
			{UID: "pod-uid-1", Name: "nginx-pod-abc123", NodeUID: "node-uid-1", WorkloadUID: wlUID},
		}
	}
	return nil
}

func (t *testInventory) ListContainersByPod(ctx context.Context, podUID string) []dm2emitter.Container {
	fmt.Printf("ListContainersByPod called for %s\n", podUID)
	if podUID == "pod-uid-1" {
		return []dm2emitter.Container{
			{UID: "pod-uid-1/nginx", Name: "nginx", Image: "nginx:latest", PodUID: podUID},
		}
	}
	return nil
}

func (t *testInventory) ClusterUID(ctx context.Context) string {
	return "test-cluster-uid"
}

func (t *testInventory) ClusterName(ctx context.Context) string {
	return "test-cluster"
}

func main() {
	fmt.Println("DM2 Emitter Test Starting...")
	
	// Set output directory
	outDir := "./dm2test_output"
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Fatalf("Failed to create output dir: %v", err)
	}
	
	// Create test inventory
	inv := &testInventory{}
	
	// Create emitter with once=true for single emission
	emitter := dm2emitter.New(inv, outDir, 5*time.Second, true)
	
	// Run emitter
	ctx := context.Background()
	fmt.Printf("Starting emitter, output will be in %s\n", outDir)
	if err := emitter.Start(ctx); err != nil {
		log.Fatalf("Emitter failed: %v", err)
	}
	
	fmt.Println("Emitter completed successfully")
	fmt.Printf("Check output: ls -la %s/\n", outDir)
}