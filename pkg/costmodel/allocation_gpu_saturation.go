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

// gpuSaturationResults holds the awaited saturation query results, decoupled
// from application so ComputeAllocation can await (and thereby surface query
// errors into the query group) before its single HasErrors() gate, and apply
// afterwards alongside the other apply helpers.
type gpuSaturationResults struct {
	throttleViolation []*source.GPUSaturationResult
	throttleReason    []*source.GPUSaturationResult
	memoryUsedAvg     []*source.GPUSaturationResult
	memoryUsedMax     []*source.GPUSaturationResult
	memoryPressure    []*source.GPUSaturationResult
	xidErrorCount     []*source.GPUSaturationResult
	dramActiveAvg     []*source.GPUSaturationResult
	dramActiveMax     []*source.GPUSaturationResult
	smActiveAvg       []*source.GPUSaturationResult
	smOccupancyAvg    []*source.GPUSaturationResult
	pcieTxBytesAvg    []*source.GPUSaturationResult
	pcieRxBytesAvg    []*source.GPUSaturationResult
	nvlinkTxBytesAvg  []*source.GPUSaturationResult
	nvlinkRxBytesAvg  []*source.GPUSaturationResult
}

// await collects every saturation query result. Like every other awaited
// query in ComputeAllocation, per-future errors are discarded here because
// Await records them in the query group, which the caller checks once via
// grp.HasErrors() after all futures (saturation included) are awaited.
func (f *gpuSaturationFutures) await() *gpuSaturationResults {
	if f == nil {
		return nil
	}

	results := &gpuSaturationResults{}
	results.throttleViolation, _ = f.throttleViolation.Await()
	results.throttleReason, _ = f.throttleReason.Await()
	results.memoryUsedAvg, _ = f.memoryUsedAvg.Await()
	results.memoryUsedMax, _ = f.memoryUsedMax.Await()
	results.memoryPressure, _ = f.memoryPressure.Await()
	results.xidErrorCount, _ = f.xidErrorCount.Await()
	results.dramActiveAvg, _ = f.dramActiveAvg.Await()
	results.dramActiveMax, _ = f.dramActiveMax.Await()
	results.smActiveAvg, _ = f.smActiveAvg.Await()
	results.smOccupancyAvg, _ = f.smOccupancyAvg.Await()
	results.pcieTxBytesAvg, _ = f.pcieTxBytesAvg.Await()
	results.pcieRxBytesAvg, _ = f.pcieRxBytesAvg.Await()
	results.nvlinkTxBytesAvg, _ = f.nvlinkTxBytesAvg.Await()
	results.nvlinkRxBytesAvg, _ = f.nvlinkRxBytesAvg.Await()
	return results
}

// apply attaches every awaited saturation signal to the pod map.
func (r *gpuSaturationResults) apply(podMap map[podKey]*pod, podUIDKeyMap map[podKey][]podKey) {
	if r == nil {
		return
	}

	applyGPUThrottleViolationRatios(podMap, r.throttleViolation, podUIDKeyMap)
	applyGPUThrottleReasonRatios(podMap, r.throttleReason, podUIDKeyMap)
	applyGPUSaturationScalar(podMap, r.memoryUsedAvg, podUIDKeyMap, "memory used ratio avg", func(sat *opencost.GPUSaturation, v float64) { sat.MemoryUsedRatioAvg = &v })
	applyGPUSaturationScalar(podMap, r.memoryUsedMax, podUIDKeyMap, "memory used ratio max", func(sat *opencost.GPUSaturation, v float64) { sat.MemoryUsedRatioMax = &v })
	applyGPUSaturationScalar(podMap, r.memoryPressure, podUIDKeyMap, "memory pressure ratio", func(sat *opencost.GPUSaturation, v float64) { sat.MemoryPressureRatio = &v })
	applyGPUSaturationScalar(podMap, r.xidErrorCount, podUIDKeyMap, "xid error count", func(sat *opencost.GPUSaturation, v float64) { sat.XIDErrorCount = &v })
	applyGPUSaturationScalar(podMap, r.dramActiveAvg, podUIDKeyMap, "dram active avg", func(sat *opencost.GPUSaturation, v float64) { sat.DRAMActiveAvg = &v })
	applyGPUSaturationScalar(podMap, r.dramActiveMax, podUIDKeyMap, "dram active max", func(sat *opencost.GPUSaturation, v float64) { sat.DRAMActiveMax = &v })
	applyGPUSaturationScalar(podMap, r.smActiveAvg, podUIDKeyMap, "sm active avg", func(sat *opencost.GPUSaturation, v float64) { sat.SMActiveAvg = &v })
	applyGPUSaturationScalar(podMap, r.smOccupancyAvg, podUIDKeyMap, "sm occupancy avg", func(sat *opencost.GPUSaturation, v float64) { sat.SMOccupancyAvg = &v })
	applyGPUSaturationScalar(podMap, r.pcieTxBytesAvg, podUIDKeyMap, "pcie tx bytes avg", func(sat *opencost.GPUSaturation, v float64) { sat.PCIeTxBytesAvg = &v })
	applyGPUSaturationScalar(podMap, r.pcieRxBytesAvg, podUIDKeyMap, "pcie rx bytes avg", func(sat *opencost.GPUSaturation, v float64) { sat.PCIeRxBytesAvg = &v })
	applyGPUSaturationScalar(podMap, r.nvlinkTxBytesAvg, podUIDKeyMap, "nvlink tx bytes avg", func(sat *opencost.GPUSaturation, v float64) { sat.NVLinkTxBytesAvg = &v })
	applyGPUSaturationScalar(podMap, r.nvlinkRxBytesAvg, podUIDKeyMap, "nvlink rx bytes avg", func(sat *opencost.GPUSaturation, v float64) { sat.NVLinkRxBytesAvg = &v })
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

// filterReasonedResults drops results missing a reason label before they
// reach forEachGPUSaturationContainer, which creates the GPUSaturation
// struct ahead of the apply callback: validating inside the callback would
// leave an empty Saturation attached for malformed results, breaking the
// "only present when at least one signal exists" semantics.
func filterReasonedResults(results []*source.GPUSaturationResult, signal string) []*source.GPUSaturationResult {
	reasoned := make([]*source.GPUSaturationResult, 0, len(results))
	for _, res := range results {
		if res.Reason == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU %s result missing 'reason'", signal)
			continue
		}
		reasoned = append(reasoned, res)
	}
	return reasoned
}

// applyGPUThrottleViolationRatios applies per-reason throttle time ratios
// derived from the DCGM violation counters.
func applyGPUThrottleViolationRatios(podMap map[podKey]*pod, results []*source.GPUSaturationResult, podUIDKeyMap map[podKey][]podKey) {
	results = filterReasonedResults(results, "throttle violation ratio")
	forEachGPUSaturationContainer(podMap, results, podUIDKeyMap, "throttle violation ratio", func(sat *opencost.GPUSaturation, res *source.GPUSaturationResult) {
		if sat.ThrottleViolationRatios == nil {
			sat.ThrottleViolationRatios = make(map[string]float64)
		}
		sat.ThrottleViolationRatios[res.Reason] = res.Data[0].Value
	})
}

// applyGPUThrottleReasonRatios applies per-reason bit ratios derived from
// the DCGM clock throttle reasons bitmask.
func applyGPUThrottleReasonRatios(podMap map[podKey]*pod, results []*source.GPUSaturationResult, podUIDKeyMap map[podKey][]podKey) {
	results = filterReasonedResults(results, "throttle reason ratio")
	forEachGPUSaturationContainer(podMap, results, podUIDKeyMap, "throttle reason ratio", func(sat *opencost.GPUSaturation, res *source.GPUSaturationResult) {
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
