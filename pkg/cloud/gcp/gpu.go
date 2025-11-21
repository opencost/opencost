package gcp

import (
	"regexp"
	"strings"
)

// ---- Original OpenCost regex fallback ----
var (
	nvidiaTeslaGPURegex = regexp.MustCompile(`(?i)nvidia[\s-]*tesla[\s-]*([a-z0-9]+)`)
	nvidiaGPURegex      = regexp.MustCompile(`(?i)nvidia[\s-]*([a-z0-9]+)`)
)

// Explicit substring → canonical GPU label
var gpuSKUToGpuLabel = map[string]string{
	// A100
	"nvidia tesla a100 80gb": "nvidia-a100-80gb",
	"nvidia a100 80gb":       "nvidia-a100-80gb",
	"nvidia tesla a100":      "nvidia-tesla-a100",
	"nvidia a100":            "nvidia-tesla-a100",

	// L4
	"nvidia l4": "nvidia-l4",

	// T4
	"tesla t4":  "nvidia-tesla-t4",
	"nvidia t4": "nvidia-tesla-t4",

	// V100
	"tesla v100":  "nvidia-tesla-v100",
	"nvidia v100": "nvidia-tesla-v100",

	// P100 (reviewer case)
	"tesla p100":  "nvidia-tesla-p100",
	"nvidia p100": "nvidia-tesla-p100",
}

// ---- Main Normalizer ----
func NormalizeGPULabel(desc string) string {
	d := strings.ToLower(desc)

	// --- Step 1: A100 detection first ---
	if strings.Contains(d, "a100") {
		has80 := strings.Contains(d, "80gb") || strings.Contains(d, "80 gb")
		has40 := strings.Contains(d, "40gb") || strings.Contains(d, "40 gb")

		if has80 {
			return "nvidia-a100-80gb"
		}
		if has40 {
			return "nvidia-tesla-a100"
		}
		return "nvidia-tesla-a100" // generic A100 → legacy
	}

	// --- Step 2: explicit substring mapping ---
	for key, model := range gpuSKUToGpuLabel {
		if strings.Contains(d, key) {
			return model
		}
	}

	// --- Step 3: regex fallback (original OpenCost behavior) ---
	if match := nvidiaTeslaGPURegex.FindStringSubmatch(desc); len(match) == 2 {
		return "nvidia-tesla-" + strings.ToLower(match[1])
	}
	if match := nvidiaGPURegex.FindStringSubmatch(desc); len(match) == 2 {
		return "nvidia-" + strings.ToLower(match[1])
	}

	return ""
}