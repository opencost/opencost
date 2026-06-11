package costmodel

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

func newSaturationTestPodMap(t *testing.T) (map[podKey]*pod, podKey) {
	t.Helper()
	t.Setenv("CLUSTER_ID", "cluster1")

	key, err := newResultPodKey("cluster1", "namespace1", "pod1")
	if err != nil {
		t.Fatalf("newResultPodKey: %v", err)
	}

	window := opencost.NewWindow(nil, nil)
	thisPod := &pod{
		Window:      window.Clone(),
		Start:       time.Time{},
		End:         time.Time{},
		Key:         key,
		Allocations: map[string]*opencost.Allocation{},
	}
	thisPod.appendContainer("container1")

	return map[podKey]*pod{key: thisPod}, key
}

func saturationResult(reason string, value float64) *source.GPUSaturationResult {
	return &source.GPUSaturationResult{
		Cluster:   "cluster1",
		Namespace: "namespace1",
		Pod:       "pod1",
		Container: "container1",
		UUID:      "GPU-1",
		Reason:    reason,
		Data:      []*util.Vector{{Value: value}},
	}
}

func TestApplyGPUThrottleRatios(t *testing.T) {
	podMap, key := newSaturationTestPodMap(t)

	applyGPUThrottleViolationRatios(podMap, []*source.GPUSaturationResult{
		saturationResult(opencost.GPUThrottleViolationPower, 0.25),
		saturationResult(opencost.GPUThrottleViolationThermal, 0.1),
		saturationResult("", 0.5), // missing reason must be dropped
	}, nil)
	applyGPUThrottleReasonRatios(podMap, []*source.GPUSaturationResult{
		saturationResult(opencost.GPUThrottleReasonSwPowerCap, 0.2),
	}, nil)

	sat := podMap[key].Allocations["container1"].GPUAllocation.Saturation
	if sat == nil {
		t.Fatalf("expected saturation to be populated")
	}
	if got := sat.ThrottleViolationRatios; len(got) != 2 || got["power"] != 0.25 || got["thermal"] != 0.1 {
		t.Errorf("ThrottleViolationRatios = %v", got)
	}
	if got := sat.ThrottleReasonRatios; len(got) != 1 || got["sw_power_cap"] != 0.2 {
		t.Errorf("ThrottleReasonRatios = %v", got)
	}
}

func TestApplyGPUSaturationScalar(t *testing.T) {
	podMap, key := newSaturationTestPodMap(t)

	applyGPUSaturationScalar(podMap, []*source.GPUSaturationResult{saturationResult("", 0.85)}, nil,
		"memory used ratio avg", func(sat *opencost.GPUSaturation, v float64) { sat.MemoryUsedRatioAvg = &v })
	applyGPUSaturationScalar(podMap, []*source.GPUSaturationResult{saturationResult("", 2)}, nil,
		"xid error count", func(sat *opencost.GPUSaturation, v float64) { sat.XIDErrorCount = &v })

	sat := podMap[key].Allocations["container1"].GPUAllocation.Saturation
	if sat == nil {
		t.Fatalf("expected saturation to be populated")
	}
	if sat.MemoryUsedRatioAvg == nil || *sat.MemoryUsedRatioAvg != 0.85 {
		t.Errorf("MemoryUsedRatioAvg = %v", sat.MemoryUsedRatioAvg)
	}
	if sat.XIDErrorCount == nil || *sat.XIDErrorCount != 2 {
		t.Errorf("XIDErrorCount = %v", sat.XIDErrorCount)
	}
	// untouched signals stay nil: absence is never zero
	if sat.SMActiveAvg != nil || sat.MemoryPressureRatio != nil {
		t.Errorf("expected unqueried signals to remain nil, got %+v", sat)
	}
}

func TestApplyGPUSaturationNoResultsLeavesAllocationUntouched(t *testing.T) {
	podMap, key := newSaturationTestPodMap(t)

	applyGPUThrottleViolationRatios(podMap, nil, nil)
	applyGPUSaturationScalar(podMap, nil, nil, "sm active avg",
		func(sat *opencost.GPUSaturation, v float64) { sat.SMActiveAvg = &v })

	if alloc := podMap[key].Allocations["container1"]; alloc.GPUAllocation != nil {
		t.Errorf("expected no GPUAllocation to be created without results, got %+v", alloc.GPUAllocation)
	}
}

func TestApplyGPUSaturationSkipsMalformedResults(t *testing.T) {
	podMap, key := newSaturationTestPodMap(t)

	noData := saturationResult("", 0)
	noData.Data = nil
	unknownPod := saturationResult("", 0.5)
	unknownPod.Pod = "missing-pod"
	noContainer := saturationResult("", 0.5)
	noContainer.Container = ""

	applyGPUSaturationScalar(podMap, []*source.GPUSaturationResult{noData, unknownPod, noContainer}, nil,
		"dram active avg", func(sat *opencost.GPUSaturation, v float64) { sat.DRAMActiveAvg = &v })

	if alloc := podMap[key].Allocations["container1"]; alloc.GPUAllocation != nil {
		t.Errorf("expected malformed results to be skipped, got %+v", alloc.GPUAllocation)
	}
}

func TestApplyGPUSaturationPreservesExistingGPUAllocation(t *testing.T) {
	podMap, key := newSaturationTestPodMap(t)

	usage := 0.4
	podMap[key].Allocations["container1"].GPUAllocation = &opencost.GPUAllocation{
		GPUUUID:         "GPU-1",
		GPUUsageAverage: &usage,
	}

	applyGPUSaturationScalar(podMap, []*source.GPUSaturationResult{saturationResult("", 0.7)}, nil,
		"dram active avg", func(sat *opencost.GPUSaturation, v float64) { sat.DRAMActiveAvg = &v })

	gpuAlloc := podMap[key].Allocations["container1"].GPUAllocation
	if gpuAlloc.GPUUUID != "GPU-1" || gpuAlloc.GPUUsageAverage == nil || *gpuAlloc.GPUUsageAverage != 0.4 {
		t.Errorf("existing GPUAllocation fields were clobbered: %+v", gpuAlloc)
	}
	if gpuAlloc.Saturation == nil || gpuAlloc.Saturation.DRAMActiveAvg == nil || *gpuAlloc.Saturation.DRAMActiveAvg != 0.7 {
		t.Errorf("saturation not applied alongside existing GPUAllocation: %+v", gpuAlloc.Saturation)
	}
}

func TestApplyGPUSaturationUIDFallback(t *testing.T) {
	podMap, key := newSaturationTestPodMap(t)

	// query results keyed by pod UID resolve through podUIDKeyMap
	uidKey, err := newResultPodKey("cluster1", "namespace1", "pod-uid-1")
	if err != nil {
		t.Fatalf("newResultPodKey: %v", err)
	}
	podUIDKeyMap := map[podKey][]podKey{uidKey: {key}}

	res := saturationResult("", 0.9)
	res.Pod = "pod-uid-1"
	applyGPUSaturationScalar(podMap, []*source.GPUSaturationResult{res}, podUIDKeyMap,
		"memory used ratio max", func(sat *opencost.GPUSaturation, v float64) { sat.MemoryUsedRatioMax = &v })

	sat := podMap[key].Allocations["container1"].GPUAllocation.Saturation
	if sat == nil || sat.MemoryUsedRatioMax == nil || *sat.MemoryUsedRatioMax != 0.9 {
		t.Errorf("UID fallback did not apply saturation: %+v", sat)
	}
}
