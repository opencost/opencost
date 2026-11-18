package util

import (
	"regexp"
	"testing"
)

func TestIsAcceleratorResourceName(t *testing.T) {
	cases := map[string]bool{
		"amd.com/gpu":               true,
		"amd_com_gpu":               true,
		"gpu.intel.com/i915":        true,
		"gpu_intel_com_xe":          true,
		"google.com/tpu":            true,
		"aws_amazon_com_neuron":     true,
		"aws.amazon.com/neuroncore": false,
		"nvidia.com/gpu":            false, // handled separately
		"cpu":                       false,
		"":                          false,
	}
	for name, want := range cases {
		if got := IsAcceleratorResourceName(name); got != want {
			t.Errorf("IsAcceleratorResourceName(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestAcceleratorResourceRegex(t *testing.T) {
	re := regexp.MustCompile("^(?:" + AcceleratorResourceRegex() + ")$")
	for _, name := range []string{"amd.com/gpu", "amd_com_gpu", "google.com/tpu", "gpu_intel_com_i915", "aws.amazon.com/neuron"} {
		if !re.MatchString(name) {
			t.Errorf("regex does not match %q", name)
		}
	}
	for _, name := range []string{"amdxcom/gpu", "aws_amazon_com_neuroncore", "nvidia_com_gpu", "cpu"} {
		if re.MatchString(name) {
			t.Errorf("regex matches %q", name)
		}
	}
}
