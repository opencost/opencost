package gcp

import "strings"

// gpuSKUToGpuLabel defines minimal, explicit mappings for common SKUs.
var gpuSKUToGpuLabel = map[string]string{
	"nvidia tesla a100 80gb": "nvidia-a100-80gb",   // A2-Ultra
	"nvidia a100 80gb":       "nvidia-a100-80gb",
	"nvidia tesla a100":      "nvidia-tesla-a100",  // A2-HighGPU legacy label
	"nvidia a100":            "nvidia-tesla-a100",
	"nvidia l4":              "nvidia-l4",          // G2 nodes
	"tesla t4":               "nvidia-tesla-t4",
	"nvidia t4":              "nvidia-tesla-t4",
	"tesla v100":             "nvidia-tesla-v100",
	"nvidia v100":            "nvidia-tesla-v100",
}

// NormalizeGPULabel converts a billing SKU description into the GKE/OpenCost GPU label.
func NormalizeGPULabel(desc string) string {
	d := strings.ToLower(desc)

	// Fast path: explicit substring matches first
	for key, model := range gpuSKUToGpuLabel {
		if strings.Contains(d, key) {
			return model
		}
	}

	// Fallbacks/cleanup (keeps your current behavior)
	//  - drop packaging suffixes, unify prefix, handle generic A100 40/80GB
	g := d
	g = strings.ReplaceAll(g, "-sxm4", "")
	g = strings.ReplaceAll(g, "  ", " ")
	g = strings.TrimSpace(g)

	// If we can spot A100 capacity, decide 80GB vs legacy A100 label
	if strings.Contains(d, "a100") {
		if strings.Contains(d, "80gb") {
			return "nvidia-a100-80gb"
		}
		// default/40GB
		return "nvidia-tesla-a100"
	}

	return ""
}