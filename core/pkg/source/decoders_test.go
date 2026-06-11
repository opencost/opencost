package source

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/util"
)

func TestDecodeGPUSaturationResult(t *testing.T) {
	values := []*util.Vector{{Timestamp: 1000, Value: 0.25}}

	t.Run("all labels present", func(t *testing.T) {
		result := NewQueryResult(map[string]any{
			"uid":           "pod-uid-1",
			"cluster_id":    "cluster-1",
			"namespace":     "gpu-ns",
			"pod":           "gpu-pod",
			"container":     "gpu-container",
			"device":        "nvidia0",
			"modelName":     "NVIDIA A100-SXM4-40GB",
			"UUID":          "GPU-1234",
			"GPU_I_PROFILE": "1g.5gb",
			"GPU_I_ID":      "3",
			"reason":        "sw_power_cap",
		}, values, nil)

		decoded := DecodeGPUSaturationResult(result)

		if decoded.UID != "pod-uid-1" {
			t.Errorf("UID = %q, want %q", decoded.UID, "pod-uid-1")
		}
		if decoded.Cluster != "cluster-1" {
			t.Errorf("Cluster = %q, want %q", decoded.Cluster, "cluster-1")
		}
		if decoded.Namespace != "gpu-ns" {
			t.Errorf("Namespace = %q, want %q", decoded.Namespace, "gpu-ns")
		}
		if decoded.Pod != "gpu-pod" {
			t.Errorf("Pod = %q, want %q", decoded.Pod, "gpu-pod")
		}
		if decoded.Container != "gpu-container" {
			t.Errorf("Container = %q, want %q", decoded.Container, "gpu-container")
		}
		if decoded.Device != "nvidia0" {
			t.Errorf("Device = %q, want %q", decoded.Device, "nvidia0")
		}
		if decoded.ModelName != "NVIDIA A100-SXM4-40GB" {
			t.Errorf("ModelName = %q, want %q", decoded.ModelName, "NVIDIA A100-SXM4-40GB")
		}
		if decoded.UUID != "GPU-1234" {
			t.Errorf("UUID = %q, want %q", decoded.UUID, "GPU-1234")
		}
		if decoded.MIGProfile != "1g.5gb" {
			t.Errorf("MIGProfile = %q, want %q", decoded.MIGProfile, "1g.5gb")
		}
		if decoded.MIGInstance != "3" {
			t.Errorf("MIGInstance = %q, want %q", decoded.MIGInstance, "3")
		}
		if decoded.Reason != "sw_power_cap" {
			t.Errorf("Reason = %q, want %q", decoded.Reason, "sw_power_cap")
		}
		if len(decoded.Data) != 1 || decoded.Data[0].Value != 0.25 {
			t.Errorf("Data not passed through: %+v", decoded.Data)
		}
	})

	t.Run("optional labels absent", func(t *testing.T) {
		// non-MIG GPU without a reason-labeled query: those labels simply
		// do not exist on the series and must decode to empty strings
		result := NewQueryResult(map[string]any{
			"uid":        "pod-uid-1",
			"cluster_id": "cluster-1",
			"namespace":  "gpu-ns",
			"pod":        "gpu-pod",
			"container":  "gpu-container",
			"UUID":       "GPU-1234",
		}, values, nil)

		decoded := DecodeGPUSaturationResult(result)

		if decoded.MIGProfile != "" || decoded.MIGInstance != "" || decoded.Reason != "" {
			t.Errorf("expected absent labels to decode to empty strings, got %+v", decoded)
		}
		if decoded.Device != "" || decoded.ModelName != "" {
			t.Errorf("expected absent device labels to decode to empty strings, got %+v", decoded)
		}
		if decoded.UUID != "GPU-1234" {
			t.Errorf("UUID = %q, want %q", decoded.UUID, "GPU-1234")
		}
	})
}
