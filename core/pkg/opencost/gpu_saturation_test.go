package opencost

import (
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
