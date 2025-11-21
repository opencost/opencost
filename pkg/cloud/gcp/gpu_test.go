package gcp

import "testing"

func TestNormalizeGPULabel(t *testing.T) {
	cases := []struct {
		desc string
		want string
	}{
		// A100 80GB (A2-Ultra)
		{"Nvidia A100 80GB GPU attached to instance", "nvidia-a100-80gb"},
		{"Nvidia Tesla A100 80GB GPU (SXM4) in region us-central1", "nvidia-a100-80gb"},

		// A100 40GB / generic A100 (A2-HighGPU legacy label)
		{"Nvidia Tesla A100 GPU attached", "nvidia-tesla-a100"},
		{"Nvidia Tesla A100 40GB GPU", "nvidia-tesla-a100"},

		// L4 (G2)
		{"NVIDIA L4 GPU attached", "nvidia-l4"},

		// T4
		{"Tesla T4 GPU", "nvidia-tesla-t4"},
		{"NVIDIA T4 accelerator", "nvidia-tesla-t4"},

		// V100
		{"NVIDIA V100 in use", "nvidia-tesla-v100"},

		// P100 – reviewer example, should be handled by regex fallback.
		{"Nvidia Tesla P100 GPU running in Melbourne", "nvidia-tesla-p100"},

		// No GPU
		{"E2 standard instance, no accelerator", ""},
	}

	for i, tc := range cases {
		got := NormalizeGPULabel(tc.desc)
		if got != tc.want {
			t.Fatalf("case %d: desc=%q: got %q, want %q", i, tc.desc, got, tc.want)
		}
	}
}