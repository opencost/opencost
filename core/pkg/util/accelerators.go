package util

import (
	"regexp"
	"strings"
)

// AcceleratorResourceNames are the extended resource names that device plugins
// from vendors other than NVIDIA use to advertise accelerators. They are counted
// as GPUs alongside "nvidia.com/gpu", which is handled separately because of GPU
// sharing. Only device-level names are listed, so a node that also advertises
// per-core resources (for example "aws.amazon.com/neuroncore") is not counted
// twice.
var AcceleratorResourceNames = []string{
	"amd.com/gpu",
	"gpu.intel.com/i915",
	"gpu.intel.com/xe",
	"google.com/tpu",
	"aws.amazon.com/neuron",
}

var nonAlphanumeric = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// IsAcceleratorResourceName reports whether name is one of
// AcceleratorResourceNames, in its Kubernetes form ("amd.com/gpu") or in the
// sanitized form kube-state-metrics uses for labels ("amd_com_gpu").
func IsAcceleratorResourceName(name string) bool {
	for _, n := range AcceleratorResourceNames {
		if name == n || name == nonAlphanumeric.ReplaceAllString(n, "_") {
			return true
		}
	}
	return false
}

// AcceleratorResourceRegex returns a PromQL regex alternation that matches
// AcceleratorResourceNames in both their Kubernetes and sanitized forms.
func AcceleratorResourceRegex() string {
	alts := make([]string, 0, 2*len(AcceleratorResourceNames))
	for _, n := range AcceleratorResourceNames {
		alts = append(alts, nonAlphanumeric.ReplaceAllString(n, "_"), strings.ReplaceAll(n, ".", "[.]"))
	}
	return strings.Join(alts, "|")
}
