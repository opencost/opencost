package costmodel

import (
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

// gpuTestLabels identifies the pod and container a GPU query result refers to.
type gpuTestLabels struct {
	cluster, namespace, pod, container string
}

var gpuTestContainer = gpuTestLabels{cluster: "cluster1", namespace: "namespace1", pod: "pod1", container: "container1"}

// newGPUTestPod returns a pod without containers that started at windowStart
// and ran for the given duration.
func newGPUTestPod(key podKey, runtime time.Duration) *pod {
	return &pod{
		Window:      window.Clone(),
		Start:       windowStart,
		End:         windowStart.Add(runtime),
		Key:         key,
		Allocations: map[string]*opencost.Allocation{},
	}
}

// gpuTestData wraps a single sample the way an instant query result does.
func gpuTestData(value float64) []*util.Vector {
	return []*util.Vector{{Timestamp: endFloat, Value: value}}
}

func gpuTestPtr[T any](v T) *T {
	return &v
}

func assertFloatPtr(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Errorf("%s: expected %f, got nil", name, want)
		return
	}
	if !util.IsApproximately(*got, want) {
		t.Errorf("%s: expected %f, got %f", name, want, *got)
	}
}

// gpuTestApplyFuncs calls each GPU apply helper with a single result for the
// given labels. The value is chosen so that every helper writes something.
var gpuTestApplyFuncs = map[string]func(podMap map[podKey]*pod, podUIDKeyMap map[podKey][]podKey, l gpuTestLabels){
	"applyGPUUsageAvg": func(podMap map[podKey]*pod, podUIDKeyMap map[podKey][]podKey, l gpuTestLabels) {
		applyGPUUsageAvg(podMap, []*source.GPUsUsageAvgResult{
			{Cluster: l.cluster, Namespace: l.namespace, Pod: l.pod, Container: l.container, Data: gpuTestData(0.5)},
		}, podUIDKeyMap)
	},
	"applyGPUUsageMax": func(podMap map[podKey]*pod, podUIDKeyMap map[podKey][]podKey, l gpuTestLabels) {
		applyGPUUsageMax(podMap, []*source.GPUsUsageMaxResult{
			{Cluster: l.cluster, Namespace: l.namespace, Pod: l.pod, Container: l.container, Data: gpuTestData(0.9)},
		}, podUIDKeyMap)
	},
	"applyGPUUsageShared": func(podMap map[podKey]*pod, podUIDKeyMap map[podKey][]podKey, l gpuTestLabels) {
		applyGPUUsageShared(podMap, []*source.IsGPUSharedResult{
			{Cluster: l.cluster, Namespace: l.namespace, Pod: l.pod, Container: l.container, Resource: "nvidia_com_gpu", Data: gpuTestData(1)},
		}, podUIDKeyMap)
	},
	"applyGPUInfo": func(podMap map[podKey]*pod, podUIDKeyMap map[podKey][]podKey, l gpuTestLabels) {
		applyGPUInfo(podMap, []*source.GPUInfoResult{
			{Cluster: l.cluster, Namespace: l.namespace, Pod: l.pod, Container: l.container, Device: "nvidia0", ModelName: "Tesla T4", UUID: "GPU-1", Data: gpuTestData(1)},
		}, podUIDKeyMap)
	},
	"applyGPUsAllocated": func(podMap map[podKey]*pod, podUIDKeyMap map[podKey][]podKey, l gpuTestLabels) {
		applyGPUsAllocated(podMap, nil, []*source.GPUsAllocatedResult{
			{Cluster: l.cluster, Namespace: l.namespace, Pod: l.pod, Container: l.container, Data: gpuTestData(1)},
		}, podUIDKeyMap)
	},
}

// TestApplyGPUHelpers_UnmatchedResults checks that the GPU apply helpers
// ignore results they cannot attach to a known pod and container.
func TestApplyGPUHelpers_UnmatchedResults(t *testing.T) {
	cases := map[string]gpuTestLabels{
		"unknown pod":       {cluster: "cluster1", namespace: "namespace1", pod: "unknown", container: "container1"},
		"missing namespace": {cluster: "cluster1", namespace: "", pod: "pod1", container: "container1"},
		"missing pod":       {cluster: "cluster1", namespace: "namespace1", pod: "", container: "container1"},
		"missing container": {cluster: "cluster1", namespace: "namespace1", pod: "pod1", container: ""},
	}

	for helperName, apply := range gpuTestApplyFuncs {
		for caseName, labels := range cases {
			t.Run(helperName+"/"+caseName, func(t *testing.T) {
				podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}

				apply(podMap, nil, labels)

				if len(podMap) != 1 {
					t.Errorf("expected podMap to keep 1 pod, got %d", len(podMap))
				}
				if n := len(podMap[podKey1].Allocations); n != 0 {
					t.Errorf("expected no allocations to be added, got %d", n)
				}
			})
		}
	}
}

// TestApplyGPUHelpers_PodUIDKeys checks that a result keyed by pod name is
// applied to every pod with that name when pods are keyed by UID.
func TestApplyGPUHelpers_PodUIDKeys(t *testing.T) {
	uidKeyA := newPodKey("cluster1", "namespace1", "pod1 uid-a")
	uidKeyB := newPodKey("cluster1", "namespace1", "pod1 uid-b")

	for helperName, apply := range gpuTestApplyFuncs {
		t.Run(helperName, func(t *testing.T) {
			podMap := map[podKey]*pod{
				uidKeyA: newGPUTestPod(uidKeyA, 24*time.Hour),
				uidKeyB: newGPUTestPod(uidKeyB, 12*time.Hour),
			}
			podUIDKeyMap := map[podKey][]podKey{podKey1: {uidKeyA, uidKeyB}}

			apply(podMap, podUIDKeyMap, gpuTestContainer)

			for _, key := range []podKey{uidKeyA, uidKeyB} {
				alloc, ok := podMap[key].Allocations[gpuTestContainer.container]
				if !ok {
					t.Errorf("%s: expected allocation for %s", key, gpuTestContainer.container)
					continue
				}
				if alloc.GPUAllocation == nil && alloc.RawAllocationOnly == nil {
					t.Errorf("%s: expected GPU data on allocation, got none", key)
				}
			}
		})
	}
}

func TestApplyGPUUsageAvg(t *testing.T) {
	t.Run("new container", func(t *testing.T) {
		podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}

		applyGPUUsageAvg(podMap, []*source.GPUsUsageAvgResult{
			{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Data: gpuTestData(0.25)},
			{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container2", Data: gpuTestData(0.75)},
		}, nil)

		allocs := podMap[podKey1].Allocations
		if len(allocs) != 2 {
			t.Fatalf("expected 2 allocations, got %d", len(allocs))
		}
		alloc := allocs["container1"]
		if alloc.Name != "cluster1/namespace1/pod1/container1" {
			t.Errorf("expected allocation name cluster1/namespace1/pod1/container1, got %s", alloc.Name)
		}
		if alloc.GPUAllocation == nil {
			t.Fatalf("expected GPUAllocation, got nil")
		}
		assertFloatPtr(t, "container1 GPUUsageAverage", alloc.GPUAllocation.GPUUsageAverage, 0.25)
		if alloc.GPUAllocation.GPURequestAverage != nil {
			t.Errorf("expected nil GPURequestAverage, got %f", *alloc.GPUAllocation.GPURequestAverage)
		}
		if alloc.GPUAllocation.IsGPUShared != nil {
			t.Errorf("expected nil IsGPUShared, got %t", *alloc.GPUAllocation.IsGPUShared)
		}
		if allocs["container2"].GPUAllocation == nil {
			t.Fatalf("expected GPUAllocation on container2, got nil")
		}
		assertFloatPtr(t, "container2 GPUUsageAverage", allocs["container2"].GPUAllocation.GPUUsageAverage, 0.75)
	})

	t.Run("existing GPUAllocation keeps other fields", func(t *testing.T) {
		thisPod := newGPUTestPod(podKey1, 24*time.Hour)
		thisPod.appendContainer("container1")
		thisPod.Allocations["container1"].GPUAllocation = &opencost.GPUAllocation{
			GPUUUID:           "GPU-1",
			IsGPUShared:       gpuTestPtr(false),
			GPURequestAverage: gpuTestPtr(1.0),
		}
		podMap := map[podKey]*pod{podKey1: thisPod}

		applyGPUUsageAvg(podMap, []*source.GPUsUsageAvgResult{
			{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Data: gpuTestData(0.25)},
		}, nil)

		got := podMap[podKey1].Allocations["container1"].GPUAllocation
		assertFloatPtr(t, "GPUUsageAverage", got.GPUUsageAverage, 0.25)
		assertFloatPtr(t, "GPURequestAverage", got.GPURequestAverage, 1.0)
		if got.GPUUUID != "GPU-1" {
			t.Errorf("expected GPUUUID GPU-1, got %s", got.GPUUUID)
		}
		if got.IsGPUShared == nil || *got.IsGPUShared {
			t.Errorf("expected IsGPUShared false, got %v", got.IsGPUShared)
		}
	})
}

func TestApplyGPUUsageMax(t *testing.T) {
	t.Run("new container", func(t *testing.T) {
		podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}

		applyGPUUsageMax(podMap, []*source.GPUsUsageMaxResult{
			{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Data: gpuTestData(0.9)},
		}, nil)

		alloc := podMap[podKey1].Allocations["container1"]
		if alloc == nil || alloc.RawAllocationOnly == nil {
			t.Fatalf("expected allocation with RawAllocationOnly, got %+v", alloc)
		}
		assertFloatPtr(t, "GPUUsageMax", alloc.RawAllocationOnly.GPUUsageMax, 0.9)
		if alloc.GPUAllocation != nil {
			t.Errorf("expected nil GPUAllocation, got %+v", alloc.GPUAllocation)
		}
	})

	t.Run("existing RawAllocationOnly keeps other fields", func(t *testing.T) {
		thisPod := newGPUTestPod(podKey1, 24*time.Hour)
		thisPod.appendContainer("container1")
		thisPod.Allocations["container1"].RawAllocationOnly = &opencost.RawAllocationOnlyData{
			CPUCoreUsageMax:  2.0,
			RAMBytesUsageMax: 4 * Gi,
		}
		podMap := map[podKey]*pod{podKey1: thisPod}

		applyGPUUsageMax(podMap, []*source.GPUsUsageMaxResult{
			{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Data: gpuTestData(0.9)},
		}, nil)

		raw := podMap[podKey1].Allocations["container1"].RawAllocationOnly
		assertFloatPtr(t, "GPUUsageMax", raw.GPUUsageMax, 0.9)
		if raw.CPUCoreUsageMax != 2.0 {
			t.Errorf("expected CPUCoreUsageMax 2.0, got %f", raw.CPUCoreUsageMax)
		}
		if raw.RAMBytesUsageMax != 4*Gi {
			t.Errorf("expected RAMBytesUsageMax %d, got %f", 4*Gi, raw.RAMBytesUsageMax)
		}
	})
}

// TestApplyGPUUsageShared covers values of 0 and 1 only. QueryIsGPUShared
// returns the averaged integer request, so a container requesting more than
// one GPU or shared replica gets a value above 1, for which the helper
// currently leaves the flag nil. That case is left out until its intended
// result is agreed. See #4118.
func TestApplyGPUUsageShared(t *testing.T) {
	cases := map[string]struct {
		resource string
		value    float64
		want     *bool
	}{
		"shared resource":                {resource: "nvidia_com_gpu_shared", value: 1, want: gpuTestPtr(true)},
		"exclusive resource":             {resource: "nvidia_com_gpu", value: 1, want: gpuTestPtr(false)},
		"shared resource, zero value":    {resource: "nvidia_com_gpu_shared", value: 0, want: nil},
		"exclusive resource, zero value": {resource: "nvidia_com_gpu", value: 0, want: nil},
		"other resource":                 {resource: "cpu", value: 1, want: nil},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}

			applyGPUUsageShared(podMap, []*source.IsGPUSharedResult{
				{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Resource: tc.resource, Data: gpuTestData(tc.value)},
			}, nil)

			alloc := podMap[podKey1].Allocations["container1"]
			if tc.want == nil {
				if alloc != nil && alloc.GPUAllocation != nil {
					t.Errorf("expected no GPUAllocation, got %+v", alloc.GPUAllocation)
				}
				return
			}
			if alloc == nil || alloc.GPUAllocation == nil || alloc.GPUAllocation.IsGPUShared == nil {
				t.Fatalf("expected IsGPUShared %t, got nil", *tc.want)
			}
			if *alloc.GPUAllocation.IsGPUShared != *tc.want {
				t.Errorf("expected IsGPUShared %t, got %t", *tc.want, *alloc.GPUAllocation.IsGPUShared)
			}
		})
	}

	t.Run("existing GPUAllocation keeps other fields", func(t *testing.T) {
		thisPod := newGPUTestPod(podKey1, 24*time.Hour)
		thisPod.appendContainer("container1")
		thisPod.Allocations["container1"].GPUAllocation = &opencost.GPUAllocation{
			GPUUsageAverage: gpuTestPtr(0.5),
		}
		podMap := map[podKey]*pod{podKey1: thisPod}

		applyGPUUsageShared(podMap, []*source.IsGPUSharedResult{
			{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Resource: "nvidia_com_gpu_shared", Data: gpuTestData(1)},
		}, nil)

		got := podMap[podKey1].Allocations["container1"].GPUAllocation
		if got.IsGPUShared == nil || !*got.IsGPUShared {
			t.Errorf("expected IsGPUShared true, got %v", got.IsGPUShared)
		}
		assertFloatPtr(t, "GPUUsageAverage", got.GPUUsageAverage, 0.5)
	})
}

func TestApplyGPUInfo(t *testing.T) {
	cases := map[string]struct {
		device     string
		wantDevice string
	}{
		"NVIDIA device name is sanitized": {device: "nvidia0", wantDevice: "nvidia"},
		"other device name is kept":       {device: "accel0", wantDevice: "accel0"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}

			applyGPUInfo(podMap, []*source.GPUInfoResult{
				{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Device: tc.device, ModelName: "Tesla T4", UUID: "GPU-1", Data: gpuTestData(1)},
			}, nil)

			alloc := podMap[podKey1].Allocations["container1"]
			if alloc == nil || alloc.GPUAllocation == nil {
				t.Fatalf("expected allocation with GPUAllocation, got %+v", alloc)
			}
			want := &opencost.GPUAllocation{GPUDevice: tc.wantDevice, GPUModel: "Tesla T4", GPUUUID: "GPU-1"}
			if !alloc.GPUAllocation.Equal(want) {
				t.Errorf("expected %+v, got %+v", want, alloc.GPUAllocation)
			}
		})
	}

	t.Run("existing GPUAllocation keeps other fields", func(t *testing.T) {
		thisPod := newGPUTestPod(podKey1, 24*time.Hour)
		thisPod.appendContainer("container1")
		thisPod.Allocations["container1"].GPUAllocation = &opencost.GPUAllocation{
			IsGPUShared:       gpuTestPtr(false),
			GPUUsageAverage:   gpuTestPtr(0.5),
			GPURequestAverage: gpuTestPtr(1.0),
		}
		podMap := map[podKey]*pod{podKey1: thisPod}

		applyGPUInfo(podMap, []*source.GPUInfoResult{
			{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Device: "nvidia0", ModelName: "Tesla T4", UUID: "GPU-1", Data: gpuTestData(1)},
		}, nil)

		want := &opencost.GPUAllocation{
			GPUDevice:         "nvidia",
			GPUModel:          "Tesla T4",
			GPUUUID:           "GPU-1",
			IsGPUShared:       gpuTestPtr(false),
			GPUUsageAverage:   gpuTestPtr(0.5),
			GPURequestAverage: gpuTestPtr(1.0),
		}
		if got := podMap[podKey1].Allocations["container1"].GPUAllocation; !got.Equal(want) {
			t.Errorf("expected %+v, got %+v", want, got)
		}
	})
}

func TestApplyGPUsAllocated(t *testing.T) {
	result := func(value float64) *source.GPUsAllocatedResult {
		return &source.GPUsAllocatedResult{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Data: gpuTestData(value)}
	}

	cases := map[string]struct {
		runtime     time.Duration
		requested   []*source.GPUsRequestedResult
		allocated   []*source.GPUsAllocatedResult
		wantRequest float64
		wantHours   float64
	}{
		"one GPU for 12 hours": {
			runtime:     12 * time.Hour,
			allocated:   []*source.GPUsAllocatedResult{result(1)},
			wantRequest: 1,
			wantHours:   12,
		},
		"four GPUs for 24 hours": {
			runtime:     24 * time.Hour,
			allocated:   []*source.GPUsAllocatedResult{result(4)},
			wantRequest: 4,
			wantHours:   96,
		},
		"allocated results take precedence over requested": {
			runtime:     24 * time.Hour,
			requested:   []*source.GPUsRequestedResult{result(2)},
			allocated:   []*source.GPUsAllocatedResult{result(1)},
			wantRequest: 1,
			wantHours:   24,
		},
		"requested results are used when there are no allocated results": {
			runtime:     6 * time.Hour,
			requested:   []*source.GPUsRequestedResult{result(2)},
			wantRequest: 2,
			wantHours:   12,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, tc.runtime)}

			applyGPUsAllocated(podMap, tc.requested, tc.allocated, nil)

			alloc := podMap[podKey1].Allocations["container1"]
			if alloc == nil || alloc.GPUAllocation == nil {
				t.Fatalf("expected allocation with GPUAllocation, got %+v", alloc)
			}
			if !util.IsApproximately(alloc.GPUHours, tc.wantHours) {
				t.Errorf("expected GPUHours %f, got %f", tc.wantHours, alloc.GPUHours)
			}
			assertFloatPtr(t, "GPURequestAverage", alloc.GPUAllocation.GPURequestAverage, tc.wantRequest)
			if alloc.GPUAllocation.GPUUsageAverage != nil {
				t.Errorf("expected nil GPUUsageAverage, got %f", *alloc.GPUAllocation.GPUUsageAverage)
			}
		})
	}

	t.Run("existing GPUAllocation keeps other fields", func(t *testing.T) {
		thisPod := newGPUTestPod(podKey1, 24*time.Hour)
		thisPod.appendContainer("container1")
		thisPod.Allocations["container1"].GPUAllocation = &opencost.GPUAllocation{
			GPUUUID:         "GPU-1",
			GPUUsageAverage: gpuTestPtr(0.5),
		}
		podMap := map[podKey]*pod{podKey1: thisPod}

		applyGPUsAllocated(podMap, nil, []*source.GPUsAllocatedResult{result(1)}, nil)

		got := podMap[podKey1].Allocations["container1"].GPUAllocation
		assertFloatPtr(t, "GPURequestAverage", got.GPURequestAverage, 1)
		assertFloatPtr(t, "GPUUsageAverage", got.GPUUsageAverage, 0.5)
		if got.GPUUUID != "GPU-1" {
			t.Errorf("expected GPUUUID GPU-1, got %s", got.GPUUUID)
		}
	})
}

// TestApplyGPUHelpers_MultiGPUContainer runs the GPU helpers in the order
// ComputeAllocation calls them, for one container holding four GPUs, with the
// results the data sources return for it: a request of 4 from the shared and
// allocated queries, and one GPU info row per device.
//
// It checks GPUHours, GPURequestAverage, the device and model, and
// GPUUsageMax. It deliberately does not assert:
//   - GPUUsageAverage or GPUEfficiency: whether GPUUsageAverage is a
//     per-device fraction or a number of GPU-equivalents for multi-GPU
//     containers depends on the data source and is not settled yet.
//   - IsGPUShared: applyGPUUsageShared sets it only for a value of exactly 1,
//     so it stays nil for this container.
//   - GPUUUID: one container has one UUID field, and the last info row wins.
//
// See #4118.
func TestApplyGPUHelpers_MultiGPUContainer(t *testing.T) {
	podMap := map[podKey]*pod{podKey1: newGPUTestPod(podKey1, 24*time.Hour)}

	applyGPUUsageAvg(podMap, []*source.GPUsUsageAvgResult{
		{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Data: gpuTestData(0.5)},
	}, nil)
	applyGPUUsageMax(podMap, []*source.GPUsUsageMaxResult{
		{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Data: gpuTestData(0.9)},
	}, nil)
	applyGPUUsageShared(podMap, []*source.IsGPUSharedResult{
		{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Resource: "nvidia_com_gpu", Data: gpuTestData(4)},
	}, nil)
	var gpuInfo []*source.GPUInfoResult
	for i := range 4 {
		gpuInfo = append(gpuInfo, &source.GPUInfoResult{
			Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1",
			Device: fmt.Sprintf("nvidia%d", i), ModelName: "Tesla T4", UUID: fmt.Sprintf("GPU-%d", i), Data: gpuTestData(1),
		})
	}
	applyGPUInfo(podMap, gpuInfo, nil)
	applyGPUsAllocated(podMap, nil, []*source.GPUsAllocatedResult{
		{Cluster: "cluster1", Namespace: "namespace1", Pod: "pod1", Container: "container1", Data: gpuTestData(4)},
	}, nil)

	alloc := podMap[podKey1].Allocations["container1"]
	if alloc == nil || alloc.GPUAllocation == nil || alloc.RawAllocationOnly == nil {
		t.Fatalf("expected allocation with GPUAllocation and RawAllocationOnly, got %+v", alloc)
	}
	if !util.IsApproximately(alloc.GPUHours, 96) {
		t.Errorf("expected GPUHours 96, got %f", alloc.GPUHours)
	}
	if !util.IsApproximately(alloc.GPUs(), 4) {
		t.Errorf("expected GPUs() 4, got %f", alloc.GPUs())
	}
	assertFloatPtr(t, "GPURequestAverage", alloc.GPUAllocation.GPURequestAverage, 4)
	if alloc.GPUAllocation.GPUDevice != "nvidia" {
		t.Errorf("expected GPUDevice nvidia, got %s", alloc.GPUAllocation.GPUDevice)
	}
	if alloc.GPUAllocation.GPUModel != "Tesla T4" {
		t.Errorf("expected GPUModel Tesla T4, got %s", alloc.GPUAllocation.GPUModel)
	}
	assertFloatPtr(t, "GPUUsageMax", alloc.RawAllocationOnly.GPUUsageMax, 0.9)
}
