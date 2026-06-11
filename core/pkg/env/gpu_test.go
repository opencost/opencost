package env

import "testing"

func TestGetGPUMemorySaturationThreshold(t *testing.T) {
	cases := map[string]struct {
		value string
		want  float64
	}{
		"unset uses default":       {"", 0.9},
		"valid value":              {"0.8", 0.8},
		"upper bound inclusive":    {"1.0", 1.0},
		"zero rejected":            {"0", 0.9},
		"negative rejected":        {"-0.5", 0.9},
		"above one rejected":       {"1.5", 0.9},
		"non-numeric uses default": {"high", 0.9},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if tc.value != "" {
				t.Setenv(GPUMemorySaturationThresholdEnvVar, tc.value)
			}
			if got := GetGPUMemorySaturationThreshold(); got != tc.want {
				t.Errorf("GetGPUMemorySaturationThreshold() = %v, want %v", got, tc.want)
			}
		})
	}
}
