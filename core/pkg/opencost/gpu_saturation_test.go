package opencost

import (
	"math"
	"reflect"
	"sort"
	"testing"
)

func TestGPUThrottleReasonsFromMask(t *testing.T) {
	cases := map[string]struct {
		mask uint64
		want []string
	}{
		"zero mask": {
			mask: 0x0,
			want: []string{},
		},
		"sw power cap": {
			mask: GPUThrottleBitSwPowerCap,
			want: []string{GPUThrottleReasonSwPowerCap},
		},
		"hw slowdown": {
			mask: GPUThrottleBitHwSlowdown,
			want: []string{GPUThrottleReasonHwSlowdown},
		},
		"sync boost": {
			mask: GPUThrottleBitSyncBoost,
			want: []string{GPUThrottleReasonSyncBoost},
		},
		"sw thermal": {
			mask: GPUThrottleBitSwThermal,
			want: []string{GPUThrottleReasonSwThermal},
		},
		"hw thermal": {
			mask: GPUThrottleBitHwThermal,
			want: []string{GPUThrottleReasonHwThermal},
		},
		"hw power brake": {
			mask: GPUThrottleBitHwPowerBrake,
			want: []string{GPUThrottleReasonHwPowerBrake},
		},
		"non-saturation bits ignored": {
			// gpu_idle (0x1), applications_clocks_setting (0x2), and
			// display_clock_setting (0x100) are operating states, not
			// saturation, and must not decode as throttle reasons.
			mask: 0x1 | 0x2 | 0x100,
			want: []string{},
		},
		"combined saturation and non-saturation bits": {
			mask: 0x1 | GPUThrottleBitSwPowerCap | GPUThrottleBitHwThermal,
			want: []string{GPUThrottleReasonSwPowerCap, GPUThrottleReasonHwThermal},
		},
		"all saturation bits": {
			mask: GPUThrottleBitSwPowerCap | GPUThrottleBitHwSlowdown | GPUThrottleBitSyncBoost |
				GPUThrottleBitSwThermal | GPUThrottleBitHwThermal | GPUThrottleBitHwPowerBrake,
			want: []string{
				GPUThrottleReasonSwPowerCap, GPUThrottleReasonHwSlowdown, GPUThrottleReasonSyncBoost,
				GPUThrottleReasonSwThermal, GPUThrottleReasonHwThermal, GPUThrottleReasonHwPowerBrake,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := GPUThrottleReasonsFromMask(tc.mask)
			sort.Strings(got)
			want := append([]string{}, tc.want...)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("GPUThrottleReasonsFromMask(%#x) = %v, want %v", tc.mask, got, want)
			}
		})
	}
}

func TestGPUThrottleReasonBitsMatchNVML(t *testing.T) {
	// Bit positions are defined by NVML's nvmlClocksThrottleReasons and must
	// never drift: DCGM_FI_DEV_CLOCK_THROTTLE_REASONS reports them verbatim.
	want := map[string]uint64{
		GPUThrottleReasonSwPowerCap:   0x4,
		GPUThrottleReasonHwSlowdown:   0x8,
		GPUThrottleReasonSyncBoost:    0x10,
		GPUThrottleReasonSwThermal:    0x20,
		GPUThrottleReasonHwThermal:    0x40,
		GPUThrottleReasonHwPowerBrake: 0x80,
	}

	if len(GPUThrottleReasons) != len(want) {
		t.Fatalf("GPUThrottleReasons has %d entries, want %d", len(GPUThrottleReasons), len(want))
	}

	seen := map[string]bool{}
	for _, reason := range GPUThrottleReasons {
		bit, ok := want[reason.Name]
		if !ok {
			t.Errorf("unexpected throttle reason %q", reason.Name)
			continue
		}
		if reason.Bit != bit {
			t.Errorf("throttle reason %q has bit %#x, want %#x", reason.Name, reason.Bit, bit)
		}
		if seen[reason.Name] {
			t.Errorf("duplicate throttle reason %q", reason.Name)
		}
		seen[reason.Name] = true
	}
}

func f64(v float64) *float64 {
	return &v
}

func mockGPUSaturation() *GPUSaturation {
	return &GPUSaturation{
		ThrottleViolationRatios: map[string]float64{
			GPUThrottleViolationPower:   0.25,
			GPUThrottleViolationThermal: 0.1,
		},
		ThrottleReasonRatios: map[string]float64{
			GPUThrottleReasonSwPowerCap: 0.2,
			GPUThrottleReasonHwThermal:  0.05,
		},
		MemoryUsedRatioAvg:  f64(0.85),
		MemoryUsedRatioMax:  f64(0.99),
		MemoryPressureRatio: f64(0.4),
		XIDErrorCount:       f64(2),
		DRAMActiveAvg:       f64(0.7),
		DRAMActiveMax:       f64(0.95),
		SMActiveAvg:         f64(0.6),
		SMOccupancyAvg:      f64(0.5),
		PCIeTxBytesAvg:      f64(1.5e9),
		PCIeRxBytesAvg:      f64(2.5e9),
		NVLinkTxBytesAvg:    f64(3.5e9),
		NVLinkRxBytesAvg:    f64(4.5e9),
	}
}

func TestGPUSaturation_SanitizeNaN(t *testing.T) {
	nan := math.NaN()

	sat := mockGPUSaturation()
	sat.MemoryUsedRatioAvg = &nan
	sat.SMActiveAvg = &nan
	sat.ThrottleViolationRatios[GPUThrottleViolationSyncBoost] = math.NaN()
	sat.ThrottleReasonRatios[GPUThrottleReasonHwSlowdown] = math.NaN()

	sat.SanitizeNaN()

	if sat.MemoryUsedRatioAvg != nil {
		t.Errorf("expected NaN MemoryUsedRatioAvg to be nil")
	}
	if sat.SMActiveAvg != nil {
		t.Errorf("expected NaN SMActiveAvg to be nil")
	}
	if _, ok := sat.ThrottleViolationRatios[GPUThrottleViolationSyncBoost]; ok {
		t.Errorf("expected NaN throttle violation entry to be removed")
	}
	if _, ok := sat.ThrottleReasonRatios[GPUThrottleReasonHwSlowdown]; ok {
		t.Errorf("expected NaN throttle reason entry to be removed")
	}

	// non-NaN values survive
	if sat.MemoryUsedRatioMax == nil || *sat.MemoryUsedRatioMax != 0.99 {
		t.Errorf("expected MemoryUsedRatioMax to survive sanitization")
	}
	if v, ok := sat.ThrottleViolationRatios[GPUThrottleViolationPower]; !ok || v != 0.25 {
		t.Errorf("expected power violation ratio to survive sanitization")
	}

	// nil receiver must not panic
	var nilSat *GPUSaturation
	nilSat.SanitizeNaN()
}

func TestGPUSaturation_Clone(t *testing.T) {
	var nilSat *GPUSaturation
	if nilSat.Clone() != nil {
		t.Fatalf("expected nil Clone of nil GPUSaturation")
	}

	orig := mockGPUSaturation()
	clone := orig.Clone()

	if !orig.Equal(clone) {
		t.Fatalf("expected clone to equal original")
	}

	// deep copy: mutating the clone must not affect the original
	clone.ThrottleViolationRatios[GPUThrottleViolationPower] = 0.99
	*clone.MemoryUsedRatioAvg = 0.1
	if orig.ThrottleViolationRatios[GPUThrottleViolationPower] == 0.99 {
		t.Errorf("clone shares ThrottleViolationRatios map with original")
	}
	if *orig.MemoryUsedRatioAvg == 0.1 {
		t.Errorf("clone shares MemoryUsedRatioAvg pointer with original")
	}
}

func TestGPUSaturation_Equal(t *testing.T) {
	cases := map[string]struct {
		a, b *GPUSaturation
		want bool
	}{
		"both nil":  {nil, nil, true},
		"one nil":   {mockGPUSaturation(), nil, false},
		"identical": {mockGPUSaturation(), mockGPUSaturation(), true},
		"different scalar": {
			mockGPUSaturation(),
			func() *GPUSaturation { s := mockGPUSaturation(); s.SMActiveAvg = f64(0.99); return s }(),
			false,
		},
		"nil vs set scalar": {
			mockGPUSaturation(),
			func() *GPUSaturation { s := mockGPUSaturation(); s.SMActiveAvg = nil; return s }(),
			false,
		},
		"different map value": {
			mockGPUSaturation(),
			func() *GPUSaturation {
				s := mockGPUSaturation()
				s.ThrottleReasonRatios[GPUThrottleReasonSwPowerCap] = 0.99
				return s
			}(),
			false,
		},
		"missing map key": {
			mockGPUSaturation(),
			func() *GPUSaturation {
				s := mockGPUSaturation()
				delete(s.ThrottleReasonRatios, GPUThrottleReasonSwPowerCap)
				return s
			}(),
			false,
		},
		"empty": {&GPUSaturation{}, &GPUSaturation{}, true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := tc.a.Equal(tc.b); got != tc.want {
				t.Errorf("Equal() = %v, want %v", got, tc.want)
			}
			if got := tc.b.Equal(tc.a); got != tc.want {
				t.Errorf("Equal() reversed = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGPUSaturation_IsEmpty(t *testing.T) {
	var nilSat *GPUSaturation
	if !nilSat.IsEmpty() {
		t.Errorf("expected nil GPUSaturation to be empty")
	}
	if !(&GPUSaturation{}).IsEmpty() {
		t.Errorf("expected zero GPUSaturation to be empty")
	}
	if !(&GPUSaturation{ThrottleReasonRatios: map[string]float64{}}).IsEmpty() {
		t.Errorf("expected GPUSaturation with empty map to be empty")
	}
	if mockGPUSaturation().IsEmpty() {
		t.Errorf("expected populated GPUSaturation to be non-empty")
	}
	if (&GPUSaturation{XIDErrorCount: f64(1)}).IsEmpty() {
		t.Errorf("expected GPUSaturation with one field to be non-empty")
	}
}

// TestGPUAllocation_BinaryRoundtripWithSaturation verifies that a
// GPUAllocation carrying saturation data survives bingen binary
// marshal/unmarshal, and that absent saturation stays absent.
func TestGPUAllocation_BinaryRoundtripWithSaturation(t *testing.T) {
	shared := false
	cases := map[string]*GPUAllocation{
		"nil saturation": {
			GPUDevice:       "nvidia0",
			GPUModel:        "Tesla T4",
			GPUUUID:         "GPU-1",
			IsGPUShared:     &shared,
			GPUUsageAverage: f64(0.5),
		},
		"populated saturation": {
			GPUDevice:  "nvidia0",
			GPUModel:   "Tesla T4",
			GPUUUID:    "GPU-1",
			Saturation: mockGPUSaturation(),
		},
		"empty saturation struct": {
			GPUDevice:  "nvidia0",
			Saturation: &GPUSaturation{},
		},
	}

	for name, orig := range cases {
		t.Run(name, func(t *testing.T) {
			bs, err := orig.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary: %s", err)
			}

			decoded := new(GPUAllocation)
			if err := decoded.UnmarshalBinary(bs); err != nil {
				t.Fatalf("UnmarshalBinary: %s", err)
			}

			if decoded.GPUDevice != orig.GPUDevice || decoded.GPUModel != orig.GPUModel || decoded.GPUUUID != orig.GPUUUID {
				t.Errorf("device identity did not survive roundtrip: got %+v, want %+v", decoded, orig)
			}
			if (decoded.Saturation == nil) != (orig.Saturation == nil) {
				t.Fatalf("saturation presence did not survive roundtrip: got %v, want %v", decoded.Saturation, orig.Saturation)
			}
			if !decoded.Saturation.Equal(orig.Saturation) {
				t.Errorf("saturation did not survive roundtrip: got %+v, want %+v", decoded.Saturation, orig.Saturation)
			}
		})
	}
}
