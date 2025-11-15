package gcp

import "strings"

// gpuSKUToGpuLabel defines minimal, explicit mappings for common non-A100 SKUs.
// A100 is handled separately to avoid ambiguous substring matches.
var gpuSKUToGpuLabel = map[string]string{
	// L4 (G2)
	"nvidia l4": "nvidia-l4",

	// T4
	"tesla t4":  "nvidia-tesla-t4",
	"nvidia t4": "nvidia-tesla-t4",

	// V100
	"tesla v100":  "nvidia-tesla-v100",
	"nvidia v100": "nvidia-tesla-v100",

	// P100 (reviewer example)
	"tesla p100":  "nvidia-tesla-p100",
	"nvidia p100": "nvidia-tesla-p100",
}

// NormalizeGPULabel converts a billing SKU description into the GKE/OpenCost GPU label.
func NormalizeGPULabel(desc string) string {
	d := strings.ToLower(desc)

	// ---- 1) Special-case A100 80GB vs 40GB / generic A100 ----
	// We do this *before* using the map to avoid flaky behavior due to
	// overlapping substrings ("nvidia a100" vs "nvidia a100 80gb").
	if strings.Contains(d, "a100") {
		has80 := strings.Contains(d, "80gb") || strings.Contains(d, "80 gb")
		has40 := strings.Contains(d, "40gb") || strings.Contains(d, "40 gb")

		if has80 {
			// A2-Ultra → nvidia-a100-80gb
			return "nvidia-a100-80gb"
		}
		if has40 {
			// A2-HighGPU 40GB → legacy label nvidia-tesla-a100
			return "nvidia-tesla-a100"
		}
		// Generic A100 → treat as legacy A2-HighGPU
		return "nvidia-tesla-a100"
	}

	// ---- 2) Other GPUs via explicit substring map ----
	for key, model := range gpuSKUToGpuLabel {
		if strings.Contains(d, key) {
			return model
		}
	}

	// ---- 3) No known GPU found ----
	return ""
}