package kubemodel

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

func saturationDeviceMap() map[string]*kubemodel.DCGMDevice {
	return map[string]*kubemodel.DCGMDevice{
		"GPU-1": {UUID: "GPU-1", Device: "nvidia0", ModelName: "Tesla T4"},
		"GPU-2": {UUID: "GPU-2", Device: "nvidia1", ModelName: "Tesla T4"},
	}
}

func deviceSaturationResult(uuid, container, reason string, value float64) *source.GPUSaturationResult {
	return &source.GPUSaturationResult{
		UUID:      uuid,
		Container: container,
		Pod:       "pod-" + container,
		Namespace: "ns",
		Reason:    reason,
		Data:      []*util.Vector{{Value: value}},
	}
}

func TestApplyDeviceSaturationScalar(t *testing.T) {
	deviceMap := saturationDeviceMap()

	results := []*source.GPUSaturationResult{
		// dcgm-exporter duplicates the device-level value onto every
		// container sharing the device: both rows carry the same value
		deviceSaturationResult("GPU-1", "container-a", "", 0.85),
		deviceSaturationResult("GPU-1", "container-b", "", 0.85),
		// unknown device must be skipped
		deviceSaturationResult("GPU-9", "container-c", "", 0.5),
	}
	noData := deviceSaturationResult("GPU-2", "container-d", "", 0)
	noData.Data = nil
	results = append(results, noData)

	applyDeviceSaturationScalar(deviceMap, results, func(sat *kubemodel.DCGMDeviceSaturation, v float64) { sat.MemoryUsedRatioAvg = &v })

	sat := deviceMap["GPU-1"].Saturation
	if sat == nil || sat.MemoryUsedRatioAvg == nil || *sat.MemoryUsedRatioAvg != 0.85 {
		t.Errorf("GPU-1 saturation = %+v, want MemoryUsedRatioAvg 0.85", sat)
	}
	// device with only an empty-data result must keep Saturation nil
	if deviceMap["GPU-2"].Saturation != nil {
		t.Errorf("GPU-2 saturation should stay nil for empty results, got %+v", deviceMap["GPU-2"].Saturation)
	}
}

func TestApplyDeviceThrottleRatios(t *testing.T) {
	deviceMap := saturationDeviceMap()

	applyDeviceThrottleRatios(deviceMap, []*source.GPUSaturationResult{
		deviceSaturationResult("GPU-1", "container-a", "power", 0.25),
		deviceSaturationResult("GPU-1", "container-a", "thermal", 0.1),
		// duplicate attribution from a sharing container, same value
		deviceSaturationResult("GPU-1", "container-b", "power", 0.25),
		// missing reason must be dropped without creating saturation
		deviceSaturationResult("GPU-2", "container-c", "", 0.5),
	}, func(sat *kubemodel.DCGMDeviceSaturation) map[string]float64 {
		if sat.ThrottleViolationRatios == nil {
			sat.ThrottleViolationRatios = make(map[string]float64)
		}
		return sat.ThrottleViolationRatios
	})

	sat := deviceMap["GPU-1"].Saturation
	if sat == nil {
		t.Fatalf("expected saturation on GPU-1")
	}
	if len(sat.ThrottleViolationRatios) != 2 || sat.ThrottleViolationRatios["power"] != 0.25 || sat.ThrottleViolationRatios["thermal"] != 0.1 {
		t.Errorf("ThrottleViolationRatios = %v", sat.ThrottleViolationRatios)
	}
	if deviceMap["GPU-2"].Saturation != nil {
		t.Errorf("reasonless result must not create saturation, got %+v", deviceMap["GPU-2"].Saturation)
	}
}

// TestDCGMSaturationAwaitAndApply runs the full reduction over a fabricated
// future bundle, verifying every signal lands on the right device and that
// devices without signals keep Saturation nil.
func TestDCGMSaturationAwaitAndApply(t *testing.T) {
	makeFuture := func(results ...*source.GPUSaturationResult) *source.QueryGroupFuture[source.GPUSaturationResult] {
		queryResults := source.NewQueryResults("test")
		for _, res := range results {
			metrics := map[string]any{
				"UUID":      res.UUID,
				"container": res.Container,
				"pod":       res.Pod,
				"namespace": res.Namespace,
			}
			if res.Reason != "" {
				metrics["reason"] = res.Reason
			}
			queryResults.Results = append(queryResults.Results, source.NewQueryResult(metrics, res.Data, nil))
		}
		ch := make(source.QueryResultsChan, 1)
		ch <- queryResults
		grp := source.NewQueryGroup()
		return source.WithGroup(grp, source.NewFuture(source.DecodeGPUSaturationResult, ch))
	}

	deviceMap := saturationDeviceMap()
	futures := &dcgmSaturationFutures{
		throttleViolation: makeFuture(deviceSaturationResult("GPU-1", "c", "power", 0.25)),
		throttleReason:    makeFuture(deviceSaturationResult("GPU-1", "c", "sw_power_cap", 0.2)),
		memoryUsedAvg:     makeFuture(deviceSaturationResult("GPU-1", "c", "", 0.85)),
		memoryUsedMax:     makeFuture(deviceSaturationResult("GPU-1", "c", "", 0.99)),
		memoryPressure:    makeFuture(deviceSaturationResult("GPU-1", "c", "", 0.4)),
		xidErrorCount:     makeFuture(deviceSaturationResult("GPU-1", "c", "", 2)),
		dramActiveAvg:     makeFuture(deviceSaturationResult("GPU-1", "c", "", 0.7)),
		dramActiveMax:     makeFuture(deviceSaturationResult("GPU-1", "c", "", 0.95)),
		smActiveAvg:       makeFuture(deviceSaturationResult("GPU-1", "c", "", 0.6)),
		smOccupancyAvg:    makeFuture(deviceSaturationResult("GPU-1", "c", "", 0.5)),
		pcieTxBytesAvg:    makeFuture(deviceSaturationResult("GPU-1", "c", "", 1.5e9)),
		pcieRxBytesAvg:    makeFuture(deviceSaturationResult("GPU-1", "c", "", 2.5e9)),
		nvlinkTxBytesAvg:  makeFuture(),
		nvlinkRxBytesAvg:  makeFuture(),
	}

	futures.awaitAndApply(deviceMap)

	sat := deviceMap["GPU-1"].Saturation
	if sat == nil {
		t.Fatalf("expected saturation on GPU-1")
	}
	if err := sat.Validate(); err != nil {
		t.Fatalf("hydrated saturation invalid: %v", err)
	}
	if sat.ThrottleViolationRatios["power"] != 0.25 || sat.ThrottleReasonRatios["sw_power_cap"] != 0.2 {
		t.Errorf("throttle ratios = %v / %v", sat.ThrottleViolationRatios, sat.ThrottleReasonRatios)
	}
	scalarChecks := map[string]*float64{
		"MemoryUsedRatioAvg=0.85": sat.MemoryUsedRatioAvg,
		"MemoryUsedRatioMax=0.99": sat.MemoryUsedRatioMax,
		"MemoryPressureRatio=0.4": sat.MemoryPressureRatio,
		"XIDErrorCount=2":         sat.XIDErrorCount,
		"DRAMActiveAvg=0.7":       sat.DRAMActiveAvg,
		"DRAMActiveMax=0.95":      sat.DRAMActiveMax,
		"SMActiveAvg=0.6":         sat.SMActiveAvg,
		"SMOccupancyAvg=0.5":      sat.SMOccupancyAvg,
		"PCIeTxBytesAvg=1.5e9":    sat.PCIeTxBytesAvg,
		"PCIeRxBytesAvg=2.5e9":    sat.PCIeRxBytesAvg,
	}
	for name, ptr := range scalarChecks {
		if ptr == nil {
			t.Errorf("%s: signal missing", name)
		}
	}
	// NVLink queries returned nothing: absent, not zero
	if sat.NVLinkTxBytesAvg != nil || sat.NVLinkRxBytesAvg != nil {
		t.Errorf("expected NVLink signals to stay nil, got %v / %v", sat.NVLinkTxBytesAvg, sat.NVLinkRxBytesAvg)
	}
	// no signals targeted GPU-2: Saturation stays nil
	if deviceMap["GPU-2"].Saturation != nil {
		t.Errorf("GPU-2 saturation should be nil, got %+v", deviceMap["GPU-2"].Saturation)
	}

	// nil bundle (feature disabled) must be a no-op
	var disabled *dcgmSaturationFutures
	disabled.awaitAndApply(deviceMap)
}
