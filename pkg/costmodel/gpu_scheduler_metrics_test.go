package costmodel

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestIsGPUResourceName(t *testing.T) {
	cases := map[v1.ResourceName]bool{
		"nvidia.com/gpu":            true,
		"nvidia.com/gpu.shared":     true,
		"nvidia.com/mig-1g.5gb":     true,
		"nvidia.com/mig-3g.20gb":    true,
		"cpu":                       false,
		"memory":                    false,
		"amd.com/gpu":               false,
		"nvidia.com/gpu-misc":       false,
		"example.com/nvidia.com":    false,
		"nvidia.com/gpufake.shared": false,
	}
	for name, want := range cases {
		if got := isGPUResourceName(name); got != want {
			t.Errorf("isGPUResourceName(%q) = %v, want %v", name, got, want)
		}
	}
}

func gpuPod(phase v1.PodPhase, resourceName v1.ResourceName, quantity string) *clustercache.Pod {
	return &clustercache.Pod{
		Status: clustercache.PodStatus{Phase: phase},
		Spec: clustercache.PodSpec{
			Containers: []clustercache.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							resourceName: resource.MustParse(quantity),
						},
					},
				},
			},
		},
	}
}

func gpuNode(resourceName v1.ResourceName, quantity string) *clustercache.Node {
	return &clustercache.Node{
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				resourceName: resource.MustParse(quantity),
			},
		},
	}
}

func TestComputeGPUSchedulerStats(t *testing.T) {
	pods := []*clustercache.Pod{
		gpuPod(v1.PodPending, "nvidia.com/gpu", "2"),
		gpuPod(v1.PodPending, "nvidia.com/gpu", "1"),
		gpuPod(v1.PodRunning, "nvidia.com/gpu", "4"),
		gpuPod(v1.PodPending, "nvidia.com/gpu.shared", "3"),
		gpuPod(v1.PodRunning, "nvidia.com/gpu.shared", "5"),
		// terminated pods must not count
		gpuPod(v1.PodSucceeded, "nvidia.com/gpu", "8"),
		gpuPod(v1.PodFailed, "nvidia.com/gpu.shared", "8"),
		// non-GPU pods must not appear at all
		gpuPod(v1.PodPending, "cpu", "1"),
	}
	nodes := []*clustercache.Node{
		gpuNode("nvidia.com/gpu", "4"),
		gpuNode("nvidia.com/gpu", "4"),
		gpuNode("nvidia.com/gpu.shared", "8"),
	}

	stats := computeGPUSchedulerStats(pods, nodes)

	if len(stats) != 2 {
		t.Fatalf("expected stats for 2 resources, got %d: %v", len(stats), stats)
	}

	gpu := stats["nvidia.com/gpu"]
	if gpu == nil {
		t.Fatalf("missing stats for nvidia.com/gpu")
	}
	if gpu.PendingPodCount != 2 || gpu.PendingRequestTotal != 3 {
		t.Errorf("nvidia.com/gpu pending = (%v pods, %v units), want (2, 3)", gpu.PendingPodCount, gpu.PendingRequestTotal)
	}
	if gpu.Allocatable != 8 || gpu.ActiveRequested != 7 {
		t.Errorf("nvidia.com/gpu capacity = (%v allocatable, %v requested), want (8, 7)", gpu.Allocatable, gpu.ActiveRequested)
	}

	shared := stats["nvidia.com/gpu.shared"]
	if shared == nil {
		t.Fatalf("missing stats for nvidia.com/gpu.shared")
	}
	if shared.PendingPodCount != 1 || shared.PendingRequestTotal != 3 {
		t.Errorf("shared pending = (%v pods, %v units), want (1, 3)", shared.PendingPodCount, shared.PendingRequestTotal)
	}
	if shared.Allocatable != 8 || shared.ActiveRequested != 8 {
		t.Errorf("shared capacity = (%v allocatable, %v requested), want (8, 8)", shared.Allocatable, shared.ActiveRequested)
	}
}

func TestComputeGPUSchedulerStats_Empty(t *testing.T) {
	if stats := computeGPUSchedulerStats(nil, nil); len(stats) != 0 {
		t.Errorf("expected no stats for empty cluster, got %v", stats)
	}

	// GPU resource requested but no allocatable capacity anywhere: stats
	// exist (pending is real) but allocatable stays zero so the ratio is
	// not emitted
	pods := []*clustercache.Pod{gpuPod(v1.PodPending, "nvidia.com/gpu", "1")}
	stats := computeGPUSchedulerStats(pods, nil)
	if len(stats) != 1 {
		t.Fatalf("expected stats for 1 resource, got %d", len(stats))
	}
	if gpu := stats["nvidia.com/gpu"]; gpu.Allocatable != 0 || gpu.PendingPodCount != 1 {
		t.Errorf("unexpected stats: %+v", gpu)
	}
}

func TestComputeGPUSchedulerStats_MultiContainerPod(t *testing.T) {
	pod := &clustercache.Pod{
		Status: clustercache.PodStatus{Phase: v1.PodPending},
		Spec: clustercache.PodSpec{
			Containers: []clustercache.Container{
				{Resources: v1.ResourceRequirements{Requests: v1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}},
				{Resources: v1.ResourceRequirements{Requests: v1.ResourceList{"nvidia.com/gpu": resource.MustParse("2")}}},
			},
		},
	}

	stats := computeGPUSchedulerStats([]*clustercache.Pod{pod}, nil)
	gpu := stats["nvidia.com/gpu"]
	if gpu.PendingPodCount != 1 {
		t.Errorf("multi-container pod counted %v times, want once", gpu.PendingPodCount)
	}
	if gpu.PendingRequestTotal != 3 {
		t.Errorf("PendingRequestTotal = %v, want 3 (sum across containers)", gpu.PendingRequestTotal)
	}
}
