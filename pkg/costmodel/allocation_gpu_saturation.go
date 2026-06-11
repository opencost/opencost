package costmodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
)

// GPU saturation wiring for ComputeAllocation. Each saturation signal is an
// independent query; a signal whose underlying DCGM metrics are unavailable
// returns no results and leaves the corresponding GPUSaturation field nil.
// The GPUSaturation struct itself is only created for containers that
// produced at least one signal.

// gpuSaturationFutures holds the in-flight saturation queries for one
// ComputeAllocation pass.
type gpuSaturationFutures struct {
	throttleViolation *source.QueryGroupFuture[source.GPUSaturationResult]
	throttleReason    *source.QueryGroupFuture[source.GPUSaturationResult]
	memoryUsedAvg     *source.QueryGroupFuture[source.GPUSaturationResult]
	memoryUsedMax     *source.QueryGroupFuture[source.GPUSaturationResult]
	memoryPressure    *source.QueryGroupFuture[source.GPUSaturationResult]
	xidErrorCount     *source.QueryGroupFuture[source.GPUSaturationResult]
	dramActiveAvg     *source.QueryGroupFuture[source.GPUSaturationResult]
	dramActiveMax     *source.QueryGroupFuture[source.GPUSaturationResult]
	smActiveAvg       *source.QueryGroupFuture[source.GPUSaturationResult]
	smOccupancyAvg    *source.QueryGroupFuture[source.GPUSaturationResult]
	pcieTxBytesAvg    *source.QueryGroupFuture[source.GPUSaturationResult]
	pcieRxBytesAvg    *source.QueryGroupFuture[source.GPUSaturationResult]
	nvlinkTxBytesAvg  *source.QueryGroupFuture[source.GPUSaturationResult]
	nvlinkRxBytesAvg  *source.QueryGroupFuture[source.GPUSaturationResult]
}

// startGPUSaturationQueries launches every saturation query concurrently
// within the allocation query group.
func startGPUSaturationQueries(grp *source.QueryGroup, ds source.MetricsQuerier, start, end time.Time) *gpuSaturationFutures {
	return &gpuSaturationFutures{
		throttleViolation: source.WithGroup(grp, ds.QueryGPUThrottleViolationRatio(start, end)),
		throttleReason:    source.WithGroup(grp, ds.QueryGPUThrottleReasonRatio(start, end)),
		memoryUsedAvg:     source.WithGroup(grp, ds.QueryGPUMemoryUsedRatioAvg(start, end)),
		memoryUsedMax:     source.WithGroup(grp, ds.QueryGPUMemoryUsedRatioMax(start, end)),
		memoryPressure:    source.WithGroup(grp, ds.QueryGPUMemoryPressureRatio(start, end)),
		xidErrorCount:     source.WithGroup(grp, ds.QueryGPUXIDErrorCount(start, end)),
		dramActiveAvg:     source.WithGroup(grp, ds.QueryGPUDRAMActiveAvg(start, end)),
		dramActiveMax:     source.WithGroup(grp, ds.QueryGPUDRAMActiveMax(start, end)),
		smActiveAvg:       source.WithGroup(grp, ds.QueryGPUSMActiveAvg(start, end)),
		smOccupancyAvg:    source.WithGroup(grp, ds.QueryGPUSMOccupancyAvg(start, end)),
		pcieTxBytesAvg:    source.WithGroup(grp, ds.QueryGPUPCIeTxBytesAvg(start, end)),
		pcieRxBytesAvg:    source.WithGroup(grp, ds.QueryGPUPCIeRxBytesAvg(start, end)),
		nvlinkTxBytesAvg:  source.WithGroup(grp, ds.QueryGPUNVLinkTxBytesAvg(start, end)),
		nvlinkRxBytesAvg:  source.WithGroup(grp, ds.QueryGPUNVLinkRxBytesAvg(start, end)),
	}
}

// awaitAndApply awaits every saturation query and applies the results to the
// pod map.
func (f *gpuSaturationFutures) awaitAndApply(podMap map[podKey]*pod, podUIDKeyMap map[podKey][]podKey) {
	if f == nil {
		return
	}

	resThrottleViolation, _ := f.throttleViolation.Await()
	resThrottleReason, _ := f.throttleReason.Await()
	resMemoryUsedAvg, _ := f.memoryUsedAvg.Await()
	resMemoryUsedMax, _ := f.memoryUsedMax.Await()
	resMemoryPressure, _ := f.memoryPressure.Await()
	resXIDErrorCount, _ := f.xidErrorCount.Await()
	resDRAMActiveAvg, _ := f.dramActiveAvg.Await()
	resDRAMActiveMax, _ := f.dramActiveMax.Await()
	resSMActiveAvg, _ := f.smActiveAvg.Await()
	resSMOccupancyAvg, _ := f.smOccupancyAvg.Await()
	resPCIeTxBytesAvg, _ := f.pcieTxBytesAvg.Await()
	resPCIeRxBytesAvg, _ := f.pcieRxBytesAvg.Await()
	resNVLinkTxBytesAvg, _ := f.nvlinkTxBytesAvg.Await()
	resNVLinkRxBytesAvg, _ := f.nvlinkRxBytesAvg.Await()

	applyGPUThrottleViolationRatios(podMap, resThrottleViolation, podUIDKeyMap)
	applyGPUThrottleReasonRatios(podMap, resThrottleReason, podUIDKeyMap)
	applyGPUSaturationScalar(podMap, resMemoryUsedAvg, podUIDKeyMap, "memory used ratio avg", func(sat *opencost.GPUSaturation, v float64) { sat.MemoryUsedRatioAvg = &v })
	applyGPUSaturationScalar(podMap, resMemoryUsedMax, podUIDKeyMap, "memory used ratio max", func(sat *opencost.GPUSaturation, v float64) { sat.MemoryUsedRatioMax = &v })
	applyGPUSaturationScalar(podMap, resMemoryPressure, podUIDKeyMap, "memory pressure ratio", func(sat *opencost.GPUSaturation, v float64) { sat.MemoryPressureRatio = &v })
	applyGPUSaturationScalar(podMap, resXIDErrorCount, podUIDKeyMap, "xid error count", func(sat *opencost.GPUSaturation, v float64) { sat.XIDErrorCount = &v })
	applyGPUSaturationScalar(podMap, resDRAMActiveAvg, podUIDKeyMap, "dram active avg", func(sat *opencost.GPUSaturation, v float64) { sat.DRAMActiveAvg = &v })
	applyGPUSaturationScalar(podMap, resDRAMActiveMax, podUIDKeyMap, "dram active max", func(sat *opencost.GPUSaturation, v float64) { sat.DRAMActiveMax = &v })
	applyGPUSaturationScalar(podMap, resSMActiveAvg, podUIDKeyMap, "sm active avg", func(sat *opencost.GPUSaturation, v float64) { sat.SMActiveAvg = &v })
	applyGPUSaturationScalar(podMap, resSMOccupancyAvg, podUIDKeyMap, "sm occupancy avg", func(sat *opencost.GPUSaturation, v float64) { sat.SMOccupancyAvg = &v })
	applyGPUSaturationScalar(podMap, resPCIeTxBytesAvg, podUIDKeyMap, "pcie tx bytes avg", func(sat *opencost.GPUSaturation, v float64) { sat.PCIeTxBytesAvg = &v })
	applyGPUSaturationScalar(podMap, resPCIeRxBytesAvg, podUIDKeyMap, "pcie rx bytes avg", func(sat *opencost.GPUSaturation, v float64) { sat.PCIeRxBytesAvg = &v })
	applyGPUSaturationScalar(podMap, resNVLinkTxBytesAvg, podUIDKeyMap, "nvlink tx bytes avg", func(sat *opencost.GPUSaturation, v float64) { sat.NVLinkTxBytesAvg = &v })
	applyGPUSaturationScalar(podMap, resNVLinkRxBytesAvg, podUIDKeyMap, "nvlink rx bytes avg", func(sat *opencost.GPUSaturation, v float64) { sat.NVLinkRxBytesAvg = &v })
}

// forEachGPUSaturationContainer resolves each saturation result to its pod
// containers, ensures the container has a GPUAllocation with a Saturation,
// and invokes apply with the result value.
//
// The lookup (podMap by key, podUIDKeyMap fallback, appendContainer on
// miss) deliberately mirrors the existing applyGPU* helpers in
// allocation_helpers.go so saturation attaches under exactly the same
// conditions as utilization — including lazily creating a GPUAllocation
// without device identity when the GPU info query returned nothing, which
// is the established behavior of applyGPUUsageAvg. The ~15 pre-existing
// applyX helpers each hand-roll this same loop; this is the one shared copy
// for all fourteen saturation signals. Extracting a repo-wide helper is
// worthwhile but belongs in its own refactor, not this feature.
func forEachGPUSaturationContainer(podMap map[podKey]*pod, results []*source.GPUSaturationResult, podUIDKeyMap map[podKey][]podKey, signal string, apply func(sat *opencost.GPUSaturation, res *source.GPUSaturationResult)) {
	for _, res := range results {
		if len(res.Data) == 0 {
			continue
		}

		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU %s query result missing field: %s", signal, err)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {
			container := res.Container
			if container == "" {
				log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU %s query result missing 'container': %s", signal, key)
				continue
			}
			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			alloc := thisPod.Allocations[container]
			if alloc.GPUAllocation == nil {
				alloc.GPUAllocation = &opencost.GPUAllocation{}
			}
			if alloc.GPUAllocation.Saturation == nil {
				alloc.GPUAllocation.Saturation = &opencost.GPUSaturation{}
			}
			apply(alloc.GPUAllocation.Saturation, res)
		}
	}
}

// applyGPUThrottleViolationRatios applies per-reason throttle time ratios
// derived from the DCGM violation counters.
func applyGPUThrottleViolationRatios(podMap map[podKey]*pod, results []*source.GPUSaturationResult, podUIDKeyMap map[podKey][]podKey) {
	forEachGPUSaturationContainer(podMap, results, podUIDKeyMap, "throttle violation ratio", func(sat *opencost.GPUSaturation, res *source.GPUSaturationResult) {
		if res.Reason == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU throttle violation result missing 'reason'")
			return
		}
		if sat.ThrottleViolationRatios == nil {
			sat.ThrottleViolationRatios = make(map[string]float64)
		}
		sat.ThrottleViolationRatios[res.Reason] = res.Data[0].Value
	})
}

// applyGPUThrottleReasonRatios applies per-reason bit ratios derived from
// the DCGM clock throttle reasons bitmask.
func applyGPUThrottleReasonRatios(podMap map[podKey]*pod, results []*source.GPUSaturationResult, podUIDKeyMap map[podKey][]podKey) {
	forEachGPUSaturationContainer(podMap, results, podUIDKeyMap, "throttle reason ratio", func(sat *opencost.GPUSaturation, res *source.GPUSaturationResult) {
		if res.Reason == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU throttle reason result missing 'reason'")
			return
		}
		if sat.ThrottleReasonRatios == nil {
			sat.ThrottleReasonRatios = make(map[string]float64)
		}
		sat.ThrottleReasonRatios[res.Reason] = res.Data[0].Value
	})
}

// applyGPUSaturationScalar applies a single-valued saturation signal via the
// provided setter.
func applyGPUSaturationScalar(podMap map[podKey]*pod, results []*source.GPUSaturationResult, podUIDKeyMap map[podKey][]podKey, signal string, set func(sat *opencost.GPUSaturation, value float64)) {
	forEachGPUSaturationContainer(podMap, results, podUIDKeyMap, signal, func(sat *opencost.GPUSaturation, res *source.GPUSaturationResult) {
		set(sat, res.Data[0].Value)
	})
}
