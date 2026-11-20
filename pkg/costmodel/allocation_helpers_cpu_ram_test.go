package costmodel

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

// cpuRAMTestHelpers lists the CPU and RAM apply helpers with the value each
// one writes to an Allocation. Their result types are all aliases of
// source.ContainerMetricResult, so they share one signature. setsNode is false
// for the usage max helpers, which do not read the node from the result.
var cpuRAMTestHelpers = []struct {
	name     string
	apply    func(podMap map[podKey]*pod, results []*source.ContainerMetricResult, podUIDKeyMap map[podKey][]podKey)
	read     func(alloc *opencost.Allocation) float64
	setsNode bool
}{
	{"applyCPUCoresAllocated", applyCPUCoresAllocated, func(a *opencost.Allocation) float64 { return a.CPUCores() }, true},
	{"applyCPUCoresRequested", applyCPUCoresRequested, func(a *opencost.Allocation) float64 { return a.CPUCoreRequestAverage }, true},
	{"applyCPUCoresLimits", applyCPUCoresLimits, func(a *opencost.Allocation) float64 { return a.CPUCoreLimitAverage }, true},
	{"applyCPUCoresUsedAvg", applyCPUCoresUsedAvg, func(a *opencost.Allocation) float64 { return a.CPUCoreUsageAverage }, true},
	{"applyCPUCoresUsedMax", applyCPUCoresUsedMax, func(a *opencost.Allocation) float64 { return cpuRAMTestRaw(a).CPUCoreUsageMax }, false},
	{"applyRAMBytesAllocated", applyRAMBytesAllocated, func(a *opencost.Allocation) float64 { return a.RAMBytes() }, true},
	{"applyRAMBytesRequested", applyRAMBytesRequested, func(a *opencost.Allocation) float64 { return a.RAMBytesRequestAverage }, true},
	{"applyRAMBytesLimits", applyRAMBytesLimits, func(a *opencost.Allocation) float64 { return a.RAMBytesLimitAverage }, true},
	{"applyRAMBytesUsedAvg", applyRAMBytesUsedAvg, func(a *opencost.Allocation) float64 { return a.RAMBytesUsageAverage }, true},
	{"applyRAMBytesUsedMax", applyRAMBytesUsedMax, func(a *opencost.Allocation) float64 { return cpuRAMTestRaw(a).RAMBytesUsageMax }, false},
}

// cpuRAMTestRaw returns the Allocation's RawAllocationOnly data, or an empty
// value if it has none.
func cpuRAMTestRaw(alloc *opencost.Allocation) *opencost.RawAllocationOnlyData {
	if alloc.RawAllocationOnly == nil {
		return &opencost.RawAllocationOnlyData{}
	}
	return alloc.RawAllocationOnly
}

// cpuRAMTestResult returns a single-sample result, the way an instant query
// returns it, for the given container of pod1 on node1.
func cpuRAMTestResult(container string, value float64) *source.ContainerMetricResult {
	return &source.ContainerMetricResult{
		Cluster:   "cluster1",
		Namespace: "namespace1",
		Pod:       "pod1",
		Container: container,
		Node:      "node1",
		Data:      []*util.Vector{{Timestamp: endFloat, Value: value}},
	}
}

func assertCPURAMFloat(t *testing.T, name string, got, want float64) {
	t.Helper()
	if !util.IsApproximately(got, want) {
		t.Errorf("%s: expected %f, got %f", name, want, got)
	}
}

// TestApplyCPURAMHelpers_UnmatchedResults checks that the CPU and RAM apply
// helpers ignore results they cannot attach to a known pod and container. The
// pod map also has pods keyed with an empty namespace and an empty pod name, so
// a result missing either one would find a pod if its key were not rejected.
func TestApplyCPURAMHelpers_UnmatchedResults(t *testing.T) {
	emptyNamespaceKey := newPodKey("cluster1", "", "pod1")
	emptyPodKey := newPodKey("cluster1", "namespace1", "")

	cases := map[string]func(res *source.ContainerMetricResult){
		"unknown pod":       func(res *source.ContainerMetricResult) { res.Pod = "unknown" },
		"missing namespace": func(res *source.ContainerMetricResult) { res.Namespace = "" },
		"missing pod":       func(res *source.ContainerMetricResult) { res.Pod = "" },
		"missing container": func(res *source.ContainerMetricResult) { res.Container = "" },
	}

	for _, helper := range cpuRAMTestHelpers {
		for caseName, change := range cases {
			t.Run(helper.name+"/"+caseName, func(t *testing.T) {
				podMap := map[podKey]*pod{
					podKey1:           newGPUTestPod(podKey1, 24*time.Hour),
					emptyNamespaceKey: newGPUTestPod(emptyNamespaceKey, 24*time.Hour),
					emptyPodKey:       newGPUTestPod(emptyPodKey, 24*time.Hour),
				}
				res := cpuRAMTestResult("container1", 2)
				change(res)

				helper.apply(podMap, []*source.ContainerMetricResult{res}, nil)

				if len(podMap) != 3 {
					t.Errorf("expected podMap to keep 3 pods, got %d", len(podMap))
				}
				for key, p := range podMap {
					if n := len(p.Allocations); n != 0 {
						t.Errorf("%s: expected no allocations to be added, got %d", key, n)
					}
				}
			})
		}
	}
}

// TestApplyCPURAMHelpers_PodUIDKeys checks that a result keyed by pod name is
// applied to every pod with that name when pods are keyed by UID. The results
// here carry no UID, and the helpers don't read one today, so this pins the
// name-only fallback.
func TestApplyCPURAMHelpers_PodUIDKeys(t *testing.T) {
	uidKeyA := newPodKey("cluster1", "namespace1", "pod1 uid-a")
	uidKeyB := newPodKey("cluster1", "namespace1", "pod1 uid-b")

	for _, helper := range cpuRAMTestHelpers {
		t.Run(helper.name, func(t *testing.T) {
			podMap := map[podKey]*pod{
				uidKeyA: newGPUTestPod(uidKeyA, 24*time.Hour),
				uidKeyB: newGPUTestPod(uidKeyB, 12*time.Hour),
			}
			podUIDKeyMap := map[podKey][]podKey{podKey1: {uidKeyA, uidKeyB}}

			helper.apply(podMap, []*source.ContainerMetricResult{cpuRAMTestResult("container1", 2)}, podUIDKeyMap)

			for _, key := range []podKey{uidKeyA, uidKeyB} {
				alloc, ok := podMap[key].Allocations["container1"]
				if !ok {
					t.Errorf("%s: expected allocation for container1", key)
					continue
				}
				assertCPURAMFloat(t, key.String(), helper.read(alloc), 2)
			}
		})
	}
}

// TestApplyCPURAMHelpers_Node checks that the helpers which read the node
// from the result set it on the allocation and on the pod, and that a result
// without a node is still applied and leaves the node as it was.
func TestApplyCPURAMHelpers_Node(t *testing.T) {
	for _, helper := range cpuRAMTestHelpers {
		if !helper.setsNode {
			continue
		}

		t.Run(helper.name+"/with node", func(t *testing.T) {
			podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}

			helper.apply(podMap, []*source.ContainerMetricResult{cpuRAMTestResult("container1", 2)}, nil)

			alloc := podMap[podKey1].Allocations["container1"]
			if alloc == nil {
				t.Fatalf("expected allocation for container1")
			}
			assertCPURAMFloat(t, "value", helper.read(alloc), 2)
			if alloc.Properties.Node != "node1" {
				t.Errorf("expected allocation node node1, got %q", alloc.Properties.Node)
			}
			if podMap[podKey1].Node != "node1" {
				t.Errorf("expected pod node node1, got %q", podMap[podKey1].Node)
			}
		})

		t.Run(helper.name+"/without node", func(t *testing.T) {
			thisPod := newGPUTestPod(podKey1, 24*time.Hour)
			thisPod.Node = "node0"
			thisPod.appendContainer("container1")
			thisPod.Allocations["container1"].Properties.Node = "node0"
			podMap := map[podKey]*pod{podKey1: thisPod}
			res := cpuRAMTestResult("container1", 2)
			res.Node = ""

			helper.apply(podMap, []*source.ContainerMetricResult{res}, nil)

			alloc := podMap[podKey1].Allocations["container1"]
			if alloc == nil {
				t.Fatalf("expected allocation for container1")
			}
			assertCPURAMFloat(t, "value", helper.read(alloc), 2)
			if alloc.Properties.Node != "node0" {
				t.Errorf("expected allocation node node0, got %q", alloc.Properties.Node)
			}
			if thisPod.Node != "node0" {
				t.Errorf("expected pod node node0, got %q", thisPod.Node)
			}
		})
	}
}

func TestApplyCPUCoresAllocated(t *testing.T) {
	cases := map[string]struct {
		runtime   time.Duration
		cores     float64
		wantHours float64
	}{
		"two cores for 24 hours":            {runtime: 24 * time.Hour, cores: 2, wantHours: 48},
		"half a core for 12 hours":          {runtime: 12 * time.Hour, cores: 0.5, wantHours: 6},
		"above the sanity limit is dropped": {runtime: 24 * time.Hour, cores: CPU_SANITY_LIMIT + 1, wantHours: 0},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, tc.runtime)}

			applyCPUCoresAllocated(podMap, []*source.CPUCoresAllocatedResult{cpuRAMTestResult("container1", tc.cores)}, nil)

			alloc := podMap[podKey1].Allocations["container1"]
			if alloc == nil {
				t.Fatalf("expected allocation for container1")
			}
			if alloc.Name != "cluster1/namespace1/pod1/container1" {
				t.Errorf("expected allocation name cluster1/namespace1/pod1/container1, got %s", alloc.Name)
			}
			assertCPURAMFloat(t, "CPUCoreHours", alloc.CPUCoreHours, tc.wantHours)
		})
	}
}

// TestApplyCPUCoresRequested checks that the request is recorded and that
// CPUCoreHours ends up at max(allocation, request) x hours.
func TestApplyCPUCoresRequested(t *testing.T) {
	cases := map[string]struct {
		allocated []*source.CPUCoresAllocatedResult
		request   float64
		wantHours float64
	}{
		"no allocation result": {
			request:   2,
			wantHours: 48,
		},
		"allocation below the request is raised to the request": {
			allocated: []*source.CPUCoresAllocatedResult{cpuRAMTestResult("container1", 1)},
			request:   2,
			wantHours: 48,
		},
		"allocation above the request is kept": {
			allocated: []*source.CPUCoresAllocatedResult{cpuRAMTestResult("container1", 3)},
			request:   2,
			wantHours: 72,
		},
		"allocation dropped by the sanity limit is raised to the request": {
			allocated: []*source.CPUCoresAllocatedResult{cpuRAMTestResult("container1", CPU_SANITY_LIMIT+1)},
			request:   2,
			wantHours: 48,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}

			applyCPUCoresAllocated(podMap, tc.allocated, nil)
			applyCPUCoresRequested(podMap, []*source.CPURequestsResult{cpuRAMTestResult("container1", tc.request)}, nil)

			alloc := podMap[podKey1].Allocations["container1"]
			if alloc == nil {
				t.Fatalf("expected allocation for container1")
			}
			assertCPURAMFloat(t, "CPUCoreRequestAverage", alloc.CPUCoreRequestAverage, tc.request)
			assertCPURAMFloat(t, "CPUCoreHours", alloc.CPUCoreHours, tc.wantHours)
		})
	}
}

// TestApplyCPUCoresUsedAvg checks the recorded usage and the outlier guard.
// Usage above CPU_SANITY_LIMIT is stored as 0, which reads the same as a
// container measured at zero or one with no usage row, so that case may
// change if #4179 changes how missing usage is stored. A container with no row
// in the usage result is left out until that is decided. Several rows for one
// container, which the query returns after a pod restart, are left out too:
// #3756 changes how the usage max helpers handle them.
func TestApplyCPUCoresUsedAvg(t *testing.T) {
	cases := map[string]struct {
		usage     float64
		wantUsage float64
	}{
		"usage below the request":           {usage: 0.5, wantUsage: 0.5},
		"usage above the request":           {usage: 3, wantUsage: 3},
		"above the sanity limit is dropped": {usage: CPU_SANITY_LIMIT + 1, wantUsage: 0},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}
			applyCPUCoresRequested(podMap, []*source.CPURequestsResult{cpuRAMTestResult("container1", 2)}, nil)

			applyCPUCoresUsedAvg(podMap, []*source.CPUUsageAvgResult{cpuRAMTestResult("container1", tc.usage)}, nil)

			alloc := podMap[podKey1].Allocations["container1"]
			if alloc == nil {
				t.Fatalf("expected allocation for container1")
			}
			assertCPURAMFloat(t, "CPUCoreUsageAverage", alloc.CPUCoreUsageAverage, tc.wantUsage)
			// Usage is recorded next to the request and does not change it
			// or the allocated hours.
			assertCPURAMFloat(t, "CPUCoreRequestAverage", alloc.CPUCoreRequestAverage, 2)
			assertCPURAMFloat(t, "CPUCoreHours", alloc.CPUCoreHours, 48)
		})
	}
}

func TestApplyRAMBytesAllocated(t *testing.T) {
	cases := map[string]struct {
		runtime   time.Duration
		bytes     float64
		wantHours float64
	}{
		"4 GiB for 24 hours":   {runtime: 24 * time.Hour, bytes: 4 * Gi, wantHours: 96 * Gi},
		"512 MiB for 12 hours": {runtime: 12 * time.Hour, bytes: 512 * Mi, wantHours: 6 * Gi},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, tc.runtime)}

			applyRAMBytesAllocated(podMap, []*source.RAMBytesAllocatedResult{cpuRAMTestResult("container1", tc.bytes)}, nil)

			alloc := podMap[podKey1].Allocations["container1"]
			if alloc == nil {
				t.Fatalf("expected allocation for container1")
			}
			assertCPURAMFloat(t, "RAMByteHours", alloc.RAMByteHours, tc.wantHours)
		})
	}
}

// TestApplyRAMBytesRequested checks that the request is recorded and that
// RAMByteHours ends up at max(allocation, request) x hours.
func TestApplyRAMBytesRequested(t *testing.T) {
	cases := map[string]struct {
		allocated []*source.RAMBytesAllocatedResult
		request   float64
		wantHours float64
	}{
		"no allocation result": {
			request:   2 * Gi,
			wantHours: 48 * Gi,
		},
		"allocation below the request is raised to the request": {
			allocated: []*source.RAMBytesAllocatedResult{cpuRAMTestResult("container1", 1*Gi)},
			request:   2 * Gi,
			wantHours: 48 * Gi,
		},
		"allocation above the request is kept": {
			allocated: []*source.RAMBytesAllocatedResult{cpuRAMTestResult("container1", 3*Gi)},
			request:   2 * Gi,
			wantHours: 72 * Gi,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}

			applyRAMBytesAllocated(podMap, tc.allocated, nil)
			applyRAMBytesRequested(podMap, []*source.RAMRequestsResult{cpuRAMTestResult("container1", tc.request)}, nil)

			alloc := podMap[podKey1].Allocations["container1"]
			if alloc == nil {
				t.Fatalf("expected allocation for container1")
			}
			assertCPURAMFloat(t, "RAMBytesRequestAverage", alloc.RAMBytesRequestAverage, tc.request)
			assertCPURAMFloat(t, "RAMByteHours", alloc.RAMByteHours, tc.wantHours)
		})
	}
}

// TestApplyCPURAMUsedMax checks that the usage max helpers create
// RawAllocationOnly when it is missing and keep its other fields when it is
// not. Several rows for one container are left out, see #3756.
func TestApplyCPURAMUsedMax(t *testing.T) {
	t.Run("new container", func(t *testing.T) {
		podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}

		applyCPUCoresUsedMax(podMap, []*source.CPUUsageMaxResult{cpuRAMTestResult("container1", 1.5)}, nil)
		applyRAMBytesUsedMax(podMap, []*source.RAMUsageMaxResult{cpuRAMTestResult("container2", 2*Gi)}, nil)

		allocs := podMap[podKey1].Allocations
		if len(allocs) != 2 {
			t.Fatalf("expected 2 allocations, got %d", len(allocs))
		}
		for _, container := range []string{"container1", "container2"} {
			if allocs[container] == nil || allocs[container].RawAllocationOnly == nil {
				t.Fatalf("expected %s with RawAllocationOnly, got %+v", container, allocs[container])
			}
		}
		assertCPURAMFloat(t, "container1 CPUCoreUsageMax", allocs["container1"].RawAllocationOnly.CPUCoreUsageMax, 1.5)
		assertCPURAMFloat(t, "container2 RAMBytesUsageMax", allocs["container2"].RawAllocationOnly.RAMBytesUsageMax, 2*Gi)
	})

	t.Run("existing RawAllocationOnly keeps other fields", func(t *testing.T) {
		gpuUsageMax := 0.9
		thisPod := newGPUTestPod(podKey1, 24*time.Hour)
		thisPod.appendContainer("container1")
		thisPod.Allocations["container1"].RawAllocationOnly = &opencost.RawAllocationOnlyData{GPUUsageMax: &gpuUsageMax}
		podMap := map[podKey]*pod{podKey1: thisPod}

		applyCPUCoresUsedMax(podMap, []*source.CPUUsageMaxResult{cpuRAMTestResult("container1", 1.5)}, nil)
		applyRAMBytesUsedMax(podMap, []*source.RAMUsageMaxResult{cpuRAMTestResult("container1", 2*Gi)}, nil)

		alloc := podMap[podKey1].Allocations["container1"]
		if alloc == nil || alloc.RawAllocationOnly == nil {
			t.Fatalf("expected container1 with RawAllocationOnly, got %+v", alloc)
		}
		raw := alloc.RawAllocationOnly
		assertCPURAMFloat(t, "CPUCoreUsageMax", raw.CPUCoreUsageMax, 1.5)
		assertCPURAMFloat(t, "RAMBytesUsageMax", raw.RAMBytesUsageMax, 2*Gi)
		assertFloatPtr(t, "GPUUsageMax", raw.GPUUsageMax, 0.9)
	})
}

// TestApplyCPURAMHelpers_RequestAndUsage runs the CPU and RAM helpers in the
// order computeAllocation calls them, for one pod that ran 24 hours with two
// containers. The allocated values are what the container_cpu_allocation and
// container_memory_allocation_bytes metrics hold, max(request, usage):
//
//	container  request         usage avg       allocated
//	under      2 cores, 4 GiB  0.5 core, 1 GiB 2 cores, 4 GiB
//	over       1 core, 1 GiB   3 cores, 2 GiB  3 cores, 2 GiB
func TestApplyCPURAMHelpers_RequestAndUsage(t *testing.T) {
	podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}
	results := func(under, over float64) []*source.ContainerMetricResult {
		return []*source.ContainerMetricResult{cpuRAMTestResult("under", under), cpuRAMTestResult("over", over)}
	}

	applyCPUCoresAllocated(podMap, results(2, 3), nil)
	applyCPUCoresRequested(podMap, results(2, 1), nil)
	applyCPUCoresLimits(podMap, results(4, 4), nil)
	applyCPUCoresUsedAvg(podMap, results(0.5, 3), nil)
	applyCPUCoresUsedMax(podMap, results(1.5, 3.5), nil)
	applyRAMBytesAllocated(podMap, results(4*Gi, 2*Gi), nil)
	applyRAMBytesRequested(podMap, results(4*Gi, 1*Gi), nil)
	applyRAMBytesLimits(podMap, results(8*Gi, 4*Gi), nil)
	applyRAMBytesUsedAvg(podMap, results(1*Gi, 2*Gi), nil)
	applyRAMBytesUsedMax(podMap, results(2*Gi, 3*Gi), nil)

	cases := map[string]struct {
		cpuHours, cpuRequest, cpuLimit, cpuUsage, cpuMax, cpuEfficiency float64
		ramHours, ramRequest, ramLimit, ramUsage, ramMax, ramEfficiency float64
	}{
		"under": {
			cpuHours: 48, cpuRequest: 2, cpuLimit: 4, cpuUsage: 0.5, cpuMax: 1.5, cpuEfficiency: 0.25,
			ramHours: 96 * Gi, ramRequest: 4 * Gi, ramLimit: 8 * Gi, ramUsage: 1 * Gi, ramMax: 2 * Gi, ramEfficiency: 0.25,
		},
		"over": {
			cpuHours: 72, cpuRequest: 1, cpuLimit: 4, cpuUsage: 3, cpuMax: 3.5, cpuEfficiency: 3,
			ramHours: 48 * Gi, ramRequest: 1 * Gi, ramLimit: 4 * Gi, ramUsage: 2 * Gi, ramMax: 3 * Gi, ramEfficiency: 2,
		},
	}

	if n := len(podMap[podKey1].Allocations); n != len(cases) {
		t.Fatalf("expected %d allocations, got %d", len(cases), n)
	}
	for container, want := range cases {
		t.Run(container, func(t *testing.T) {
			alloc := podMap[podKey1].Allocations[container]
			if alloc == nil || alloc.RawAllocationOnly == nil {
				t.Fatalf("expected allocation with RawAllocationOnly, got %+v", alloc)
			}
			assertCPURAMFloat(t, "CPUCoreHours", alloc.CPUCoreHours, want.cpuHours)
			assertCPURAMFloat(t, "CPUCoreRequestAverage", alloc.CPUCoreRequestAverage, want.cpuRequest)
			assertCPURAMFloat(t, "CPUCoreLimitAverage", alloc.CPUCoreLimitAverage, want.cpuLimit)
			assertCPURAMFloat(t, "CPUCoreUsageAverage", alloc.CPUCoreUsageAverage, want.cpuUsage)
			assertCPURAMFloat(t, "CPUCoreUsageMax", alloc.RawAllocationOnly.CPUCoreUsageMax, want.cpuMax)
			assertCPURAMFloat(t, "CPUEfficiency", alloc.CPUEfficiency(), want.cpuEfficiency)
			assertCPURAMFloat(t, "RAMByteHours", alloc.RAMByteHours, want.ramHours)
			assertCPURAMFloat(t, "RAMBytesRequestAverage", alloc.RAMBytesRequestAverage, want.ramRequest)
			assertCPURAMFloat(t, "RAMBytesLimitAverage", alloc.RAMBytesLimitAverage, want.ramLimit)
			assertCPURAMFloat(t, "RAMBytesUsageAverage", alloc.RAMBytesUsageAverage, want.ramUsage)
			assertCPURAMFloat(t, "RAMBytesUsageMax", alloc.RawAllocationOnly.RAMBytesUsageMax, want.ramMax)
			assertCPURAMFloat(t, "RAMEfficiency", alloc.RAMEfficiency(), want.ramEfficiency)
			if alloc.Properties.Node != "node1" {
				t.Errorf("expected node node1, got %q", alloc.Properties.Node)
			}
		})
	}
}
