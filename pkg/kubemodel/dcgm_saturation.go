package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

// DCGM device saturation hydration. The saturation queries are
// container-attributed series (dcgm-exporter duplicates each device-level
// value onto every pod sharing the device), so hydration reduces them to
// one value per device UUID. MIG instances surface as their own UUIDs when
// the exporter runs in MIG mode, so slices hydrate as distinct devices and
// PodUsages preserves the container-to-slice association.

// dcgmSaturationFutures holds the in-flight saturation queries for one
// computeDCGMDevices pass, mirroring the bundle used by ComputeAllocation.
type dcgmSaturationFutures struct {
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

func startDCGMSaturationQueries(grp *source.QueryGroup, metrics source.MetricsQuerier, start, end time.Time) *dcgmSaturationFutures {
	return &dcgmSaturationFutures{
		throttleViolation: source.WithGroup(grp, metrics.QueryGPUThrottleViolationRatio(start, end)),
		throttleReason:    source.WithGroup(grp, metrics.QueryGPUThrottleReasonRatio(start, end)),
		memoryUsedAvg:     source.WithGroup(grp, metrics.QueryGPUMemoryUsedRatioAvg(start, end)),
		memoryUsedMax:     source.WithGroup(grp, metrics.QueryGPUMemoryUsedRatioMax(start, end)),
		memoryPressure:    source.WithGroup(grp, metrics.QueryGPUMemoryPressureRatio(start, end)),
		xidErrorCount:     source.WithGroup(grp, metrics.QueryGPUXIDErrorCount(start, end)),
		dramActiveAvg:     source.WithGroup(grp, metrics.QueryGPUDRAMActiveAvg(start, end)),
		dramActiveMax:     source.WithGroup(grp, metrics.QueryGPUDRAMActiveMax(start, end)),
		smActiveAvg:       source.WithGroup(grp, metrics.QueryGPUSMActiveAvg(start, end)),
		smOccupancyAvg:    source.WithGroup(grp, metrics.QueryGPUSMOccupancyAvg(start, end)),
		pcieTxBytesAvg:    source.WithGroup(grp, metrics.QueryGPUPCIeTxBytesAvg(start, end)),
		pcieRxBytesAvg:    source.WithGroup(grp, metrics.QueryGPUPCIeRxBytesAvg(start, end)),
		nvlinkTxBytesAvg:  source.WithGroup(grp, metrics.QueryGPUNVLinkTxBytesAvg(start, end)),
		nvlinkRxBytesAvg:  source.WithGroup(grp, metrics.QueryGPUNVLinkRxBytesAvg(start, end)),
	}
}

// awaitAndApply awaits every saturation query and reduces the results onto
// the device map. Per-future errors are recorded in the query group by
// Await, matching how the rest of computeDCGMDevices treats its queries.
func (f *dcgmSaturationFutures) awaitAndApply(deviceMap map[string]*kubemodel.DCGMDevice) {
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

	applyDeviceThrottleRatios(deviceMap, resThrottleViolation, func(sat *kubemodel.DCGMDeviceSaturation) map[string]float64 {
		if sat.ThrottleViolationRatios == nil {
			sat.ThrottleViolationRatios = make(map[string]float64)
		}
		return sat.ThrottleViolationRatios
	})
	applyDeviceThrottleRatios(deviceMap, resThrottleReason, func(sat *kubemodel.DCGMDeviceSaturation) map[string]float64 {
		if sat.ThrottleReasonRatios == nil {
			sat.ThrottleReasonRatios = make(map[string]float64)
		}
		return sat.ThrottleReasonRatios
	})
	applyDeviceSaturationScalar(deviceMap, resMemoryUsedAvg, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.MemoryUsedRatioAvg = &v })
	applyDeviceSaturationScalar(deviceMap, resMemoryUsedMax, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.MemoryUsedRatioMax = &v })
	applyDeviceSaturationScalar(deviceMap, resMemoryPressure, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.MemoryPressureRatio = &v })
	applyDeviceSaturationScalar(deviceMap, resXIDErrorCount, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.XIDErrorCount = &v })
	applyDeviceSaturationScalar(deviceMap, resDRAMActiveAvg, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.DRAMActiveAvg = &v })
	applyDeviceSaturationScalar(deviceMap, resDRAMActiveMax, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.DRAMActiveMax = &v })
	applyDeviceSaturationScalar(deviceMap, resSMActiveAvg, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.SMActiveAvg = &v })
	applyDeviceSaturationScalar(deviceMap, resSMOccupancyAvg, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.SMOccupancyAvg = &v })
	applyDeviceSaturationScalar(deviceMap, resPCIeTxBytesAvg, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.PCIeTxBytesAvg = &v })
	applyDeviceSaturationScalar(deviceMap, resPCIeRxBytesAvg, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.PCIeRxBytesAvg = &v })
	applyDeviceSaturationScalar(deviceMap, resNVLinkTxBytesAvg, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.NVLinkTxBytesAvg = &v })
	applyDeviceSaturationScalar(deviceMap, resNVLinkRxBytesAvg, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.NVLinkRxBytesAvg = &v })
}

// ensureDeviceSaturation lazily creates the saturation struct, so devices
// with no signals keep Saturation nil (absence is never zero).
func ensureDeviceSaturation(device *kubemodel.DCGMDevice) *kubemodel.DCGMDeviceSaturation {
	if device.Saturation == nil {
		device.Saturation = &kubemodel.DCGMDeviceSaturation{}
	}
	return device.Saturation
}

// applyDeviceSaturationScalar reduces container-attributed results to one
// value per device UUID. Sharing containers carry duplicates of the same
// device-level value, so last-write-wins is exact; results for UUIDs the
// info query did not report are skipped.
func applyDeviceSaturationScalar(deviceMap map[string]*kubemodel.DCGMDevice, results []*source.GPUSaturationResult, set func(sat *kubemodel.DCGMDeviceSaturation, value float64)) {
	for _, res := range results {
		device, ok := deviceMap[res.UUID]
		if !ok || len(res.Data) == 0 {
			continue
		}
		set(ensureDeviceSaturation(device), res.Data[0].Value)
	}
}

// applyDeviceThrottleRatios reduces reason-labeled throttle results onto the
// device's reason map.
func applyDeviceThrottleRatios(deviceMap map[string]*kubemodel.DCGMDevice, results []*source.GPUSaturationResult, ratios func(sat *kubemodel.DCGMDeviceSaturation) map[string]float64) {
	for _, res := range results {
		device, ok := deviceMap[res.UUID]
		if !ok || len(res.Data) == 0 || res.Reason == "" {
			continue
		}
		ratios(ensureDeviceSaturation(device))[res.Reason] = res.Data[0].Value
	}
}
