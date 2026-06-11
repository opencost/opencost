package kubemodel

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func saturationFloat(v float64) *float64 {
	return &v
}

func mockDCGMDeviceSaturation() *DCGMDeviceSaturation {
	return &DCGMDeviceSaturation{
		ThrottleViolationRatios: map[string]float64{"power": 0.25},
		ThrottleReasonRatios:    map[string]float64{"sw_power_cap": 0.2},
		MemoryUsedRatioAvg:      saturationFloat(0.85),
		MemoryUsedRatioMax:      saturationFloat(0.99),
		MemoryPressureRatio:     saturationFloat(0.4),
		XIDErrorCount:           saturationFloat(2),
		DRAMActiveAvg:           saturationFloat(0.7),
		DRAMActiveMax:           saturationFloat(0.95),
		SMActiveAvg:             saturationFloat(0.6),
		SMOccupancyAvg:          saturationFloat(0.5),
		PCIeTxBytesAvg:          saturationFloat(1.5e9),
		PCIeRxBytesAvg:          saturationFloat(2.5e9),
		NVLinkTxBytesAvg:        saturationFloat(3.5e9),
		NVLinkRxBytesAvg:        saturationFloat(4.5e9),
	}
}

func mockSaturatedDCGMDevice() *DCGMDevice {
	return &DCGMDevice{
		UUID:                  "GPU-1",
		Start:                 time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		End:                   time.Date(2026, 6, 1, 1, 0, 0, 0, time.UTC),
		Device:                "nvidia0",
		ModelName:             "NVIDIA A100 80GB",
		Saturation:            mockDCGMDeviceSaturation(),
		PowerWatts:            140,
		TemperatureCelsius:    55,
		ComputeUtilizationAvg: 42.5,
		ComputeUtilizationMax: 97,
		MemoryUsedBytesAvg:    32e9,
		MemoryUsedBytesMax:    71e9,
	}
}

// TestDCGMDevice_DeviceInfoInterface verifies the vendor-neutral identity
// surface on the DCGM device.
func TestDCGMDevice_DeviceInfoInterface(t *testing.T) {
	var info DeviceInfo = mockSaturatedDCGMDevice()

	if info.GetIdentifier() != "GPU-1" {
		t.Errorf("GetIdentifier() = %q, want GPU-1", info.GetIdentifier())
	}
	if info.GetType() != "GPU" {
		t.Errorf("GetType() = %q, want GPU", info.GetType())
	}
	if info.GetName() != "NVIDIA A100 80GB" {
		t.Errorf("GetName() = %q", info.GetName())
	}
	if !info.GetStart().Before(info.GetEnd()) {
		t.Errorf("GetStart() not before GetEnd()")
	}
	if info.GetPower() != 140 {
		t.Errorf("GetPower() = %v, want 140", info.GetPower())
	}
	// MIG parentage is not derivable from dcgm-exporter labels yet
	if info.GetParent() != "" {
		t.Errorf("expected empty parent until a mapping source exists, got %q", info.GetParent())
	}
}

// TestDCGMDevice_DevicePerformanceInterface verifies the performance surface
// is backed by the device-level metric fields with documented units.
func TestDCGMDevice_DevicePerformanceInterface(t *testing.T) {
	var perf DevicePerformance = mockSaturatedDCGMDevice()

	if perf.GetComputeUtilizationAverage() != 42.5 || perf.GetComputeUtilizationMax() != 97 {
		t.Errorf("compute utilization = (%v, %v), want (42.5, 97)", perf.GetComputeUtilizationAverage(), perf.GetComputeUtilizationMax())
	}
	if perf.GetMemoryUtilizationAverage() != 32e9 || perf.GetMemoryUtilizationMax() != 71e9 {
		t.Errorf("memory utilization = (%v, %v), want (3.2e10, 7.1e10)", perf.GetMemoryUtilizationAverage(), perf.GetMemoryUtilizationMax())
	}
	if perf.GetTemp() != 55 {
		t.Errorf("GetTemp() = %v, want 55", perf.GetTemp())
	}
}

// TestDCGMDevice_DeviceSaturationInterface verifies the saturation getters
// expose every signal with correct values, that absence (nil Saturation or
// nil field) reports ok=false / nil rather than zero, and that returned
// maps are copies.
func TestDCGMDevice_DeviceSaturationInterface(t *testing.T) {
	var sat DeviceSaturation = mockSaturatedDCGMDevice()

	scalarChecks := map[string]struct {
		get  func() (float64, bool)
		want float64
	}{
		"MemoryUsedRatioAvg":       {sat.GetMemoryUsedRatioAvg, 0.85},
		"MemoryUsedRatioMax":       {sat.GetMemoryUsedRatioMax, 0.99},
		"MemoryPressureRatio":      {sat.GetMemoryPressureRatio, 0.4},
		"ErrorEventCount":          {sat.GetErrorEventCount, 2},
		"MemoryBandwidthActiveAvg": {sat.GetMemoryBandwidthActiveAvg, 0.7},
		"MemoryBandwidthActiveMax": {sat.GetMemoryBandwidthActiveMax, 0.95},
		"ComputeActiveAvg":         {sat.GetComputeActiveAvg, 0.6},
		"ComputeOccupancyAvg":      {sat.GetComputeOccupancyAvg, 0.5},
		"HostLinkTxBytesAvg":       {sat.GetHostLinkTxBytesAvg, 1.5e9},
		"HostLinkRxBytesAvg":       {sat.GetHostLinkRxBytesAvg, 2.5e9},
		"PeerLinkTxBytesAvg":       {sat.GetPeerLinkTxBytesAvg, 3.5e9},
		"PeerLinkRxBytesAvg":       {sat.GetPeerLinkRxBytesAvg, 4.5e9},
	}
	for name, check := range scalarChecks {
		got, ok := check.get()
		if !ok || got != check.want {
			t.Errorf("%s = (%v, %v), want (%v, true)", name, got, ok, check.want)
		}
	}

	if got := sat.GetThrottleViolationRatios(); got["power"] != 0.25 {
		t.Errorf("GetThrottleViolationRatios() = %v, want power: 0.25", got)
	}
	if got := sat.GetThrottleReasonRatios(); got["sw_power_cap"] != 0.2 {
		t.Errorf("GetThrottleReasonRatios() = %v, want sw_power_cap: 0.2", got)
	}

	// returned maps are copies: consumers must not mutate device state
	sat.GetThrottleViolationRatios()["power"] = 0.99
	if got := sat.GetThrottleViolationRatios(); got["power"] != 0.25 {
		t.Errorf("interface exposed internal throttle map: %v", got)
	}

	// absence semantics: nil Saturation reports ok=false / nil, never zero
	var absent DeviceSaturation = &DCGMDevice{UUID: "GPU-2"}
	if _, ok := absent.GetMemoryUsedRatioAvg(); ok {
		t.Errorf("expected ok=false for device without saturation")
	}
	if got := absent.GetThrottleViolationRatios(); got != nil {
		t.Errorf("expected nil throttle map for device without saturation, got %v", got)
	}

	// per-field absence: present Saturation with one nil field
	partial := mockSaturatedDCGMDevice()
	partial.Saturation.SMActiveAvg = nil
	if _, ok := partial.GetComputeActiveAvg(); ok {
		t.Errorf("expected ok=false for absent SM active signal")
	}
	if got, ok := partial.GetComputeOccupancyAvg(); !ok || got != 0.5 {
		t.Errorf("sibling signal lost: (%v, %v), want (0.5, true)", got, ok)
	}
}

func TestDCGMDeviceSaturation_Validate(t *testing.T) {
	cases := map[string]struct {
		mutate  func(*DCGMDeviceSaturation)
		wantErr bool
	}{
		"valid":  {mutate: func(s *DCGMDeviceSaturation) {}, wantErr: false},
		"nil ok": {mutate: nil, wantErr: false},
		"empty":  {mutate: func(s *DCGMDeviceSaturation) { *s = DCGMDeviceSaturation{} }, wantErr: false},
		"ratio above one": {
			mutate:  func(s *DCGMDeviceSaturation) { s.MemoryUsedRatioAvg = saturationFloat(1.1) },
			wantErr: true,
		},
		"negative ratio": {
			mutate:  func(s *DCGMDeviceSaturation) { s.SMActiveAvg = saturationFloat(-0.1) },
			wantErr: true,
		},
		"NaN ratio": {
			mutate:  func(s *DCGMDeviceSaturation) { s.DRAMActiveAvg = saturationFloat(math.NaN()) },
			wantErr: true,
		},
		"throttle map ratio above one": {
			mutate:  func(s *DCGMDeviceSaturation) { s.ThrottleReasonRatios["sw_power_cap"] = 1.5 },
			wantErr: true,
		},
		"negative byte rate": {
			mutate:  func(s *DCGMDeviceSaturation) { s.PCIeTxBytesAvg = saturationFloat(-1) },
			wantErr: true,
		},
		"negative xid count": {
			mutate:  func(s *DCGMDeviceSaturation) { s.XIDErrorCount = saturationFloat(-1) },
			wantErr: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var sat *DCGMDeviceSaturation
			if tc.mutate != nil {
				sat = mockDCGMDeviceSaturation()
				tc.mutate(sat)
			}
			err := sat.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}

	// invalid saturation must fail device validation too
	device := mockSaturatedDCGMDevice()
	device.Saturation.MemoryUsedRatioAvg = saturationFloat(2.0)
	window := Window{Start: device.Start, End: device.End}
	if err := device.ValidateDCGMDevice(window); err == nil {
		t.Errorf("expected ValidateDCGMDevice to reject invalid saturation")
	}
}

func TestDCGMDeviceSaturation_Clone(t *testing.T) {
	var nilSat *DCGMDeviceSaturation
	if nilSat.Clone() != nil {
		t.Fatalf("expected nil Clone of nil DCGMDeviceSaturation")
	}

	orig := mockDCGMDeviceSaturation()
	clone := orig.Clone()

	if !reflect.DeepEqual(orig, clone) {
		t.Fatalf("clone differs from original:\n got %+v\nwant %+v", clone, orig)
	}

	clone.ThrottleViolationRatios["power"] = 0.99
	*clone.MemoryUsedRatioAvg = 0.1
	if orig.ThrottleViolationRatios["power"] == 0.99 {
		t.Errorf("clone shares ThrottleViolationRatios map with original")
	}
	if *orig.MemoryUsedRatioAvg == 0.1 {
		t.Errorf("clone shares MemoryUsedRatioAvg pointer with original")
	}
}

// TestDCGMDevice_BinaryRoundtripWithSaturation verifies a DCGMDevice
// carrying saturation survives the bingen binary codec, and that absent
// saturation stays absent.
func TestDCGMDevice_BinaryRoundtripWithSaturation(t *testing.T) {
	cases := map[string]*DCGMDevice{
		"nil saturation": {
			UUID:      "GPU-1",
			Device:    "nvidia0",
			ModelName: "Tesla T4",
		},
		"populated saturation": mockSaturatedDCGMDevice(),
		"empty saturation struct": {
			UUID:       "GPU-1",
			Saturation: &DCGMDeviceSaturation{},
		},
	}

	for name, orig := range cases {
		t.Run(name, func(t *testing.T) {
			bs, err := orig.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary: %s", err)
			}

			decoded := new(DCGMDevice)
			if err := decoded.UnmarshalBinary(bs); err != nil {
				t.Fatalf("UnmarshalBinary: %s", err)
			}

			if decoded.UUID != orig.UUID || decoded.Device != orig.Device || decoded.ModelName != orig.ModelName {
				t.Errorf("device identity did not survive roundtrip: got %+v, want %+v", decoded, orig)
			}
			if decoded.PowerWatts != orig.PowerWatts || decoded.TemperatureCelsius != orig.TemperatureCelsius ||
				decoded.ComputeUtilizationAvg != orig.ComputeUtilizationAvg || decoded.ComputeUtilizationMax != orig.ComputeUtilizationMax ||
				decoded.MemoryUsedBytesAvg != orig.MemoryUsedBytesAvg || decoded.MemoryUsedBytesMax != orig.MemoryUsedBytesMax {
				t.Errorf("device metrics did not survive roundtrip: got %+v, want %+v", decoded, orig)
			}
			if (decoded.Saturation == nil) != (orig.Saturation == nil) {
				t.Fatalf("saturation presence did not survive roundtrip: got %v, want %v", decoded.Saturation, orig.Saturation)
			}
			if !reflect.DeepEqual(decoded.Saturation, orig.Saturation) {
				t.Errorf("saturation did not survive roundtrip: got %+v, want %+v", decoded.Saturation, orig.Saturation)
			}
		})
	}
}
