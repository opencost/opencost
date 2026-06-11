package clustercache

import (
	"reflect"
	"testing"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func strPtr(s string) *string { return &s }
func intPtr(i int64) *int64   { return &i }
func boolPtr(b bool) *bool    { return &b }

func TestTransformResourceSlice(t *testing.T) {
	if TransformResourceSlice(nil) != nil {
		t.Fatalf("nil slice must transform to nil")
	}

	slice := &resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "node1-gpu-slice"},
		Spec: resourcev1.ResourceSliceSpec{
			Driver:   "gpu.nvidia.com",
			Pool:     resourcev1.ResourcePool{Name: "node1"},
			NodeName: strPtr("node1"),
			Devices: []resourcev1.Device{
				{
					Name: "gpu-0",
					Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
						"uuid":          {StringValue: strPtr("GPU-1234")},
						"index":         {IntValue: intPtr(0)},
						"migEnabled":    {BoolValue: boolPtr(false)},
						"driverVersion": {VersionValue: strPtr("550.54.15")},
					},
					Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
						"memory": {Value: resource.MustParse("80Gi")},
					},
				},
				{Name: "gpu-1"},
			},
		},
	}

	got := TransformResourceSlice(slice)

	if got.Name != "node1-gpu-slice" || got.Driver != "gpu.nvidia.com" || got.Pool != "node1" || got.NodeName != "node1" {
		t.Errorf("slice identity = %+v", got)
	}
	if len(got.Devices) != 2 {
		t.Fatalf("expected 2 devices, got %d", len(got.Devices))
	}

	wantAttrs := map[string]string{
		"uuid":          "GPU-1234",
		"index":         "0",
		"migEnabled":    "false",
		"driverVersion": "550.54.15",
	}
	if !reflect.DeepEqual(got.Devices[0].Attributes, wantAttrs) {
		t.Errorf("attributes = %v, want %v", got.Devices[0].Attributes, wantAttrs)
	}
	if got.Devices[0].Capacity["memory"] != "80Gi" {
		t.Errorf("capacity = %v, want memory: 80Gi", got.Devices[0].Capacity)
	}
	// device without attributes/capacity keeps nil maps
	if got.Devices[1].Attributes != nil || got.Devices[1].Capacity != nil {
		t.Errorf("bare device should keep nil maps: %+v", got.Devices[1])
	}
}

func TestTransformResourceClaim(t *testing.T) {
	if TransformResourceClaim(nil) != nil {
		t.Fatalf("nil claim must transform to nil")
	}

	t.Run("pending claim has requests only", func(t *testing.T) {
		claim := &resourcev1.ResourceClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "train-gpus", Namespace: "ml", UID: "claim-uid-1"},
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{
						{
							Name: "gpus",
							Exactly: &resourcev1.ExactDeviceRequest{
								DeviceClassName: "gpu.nvidia.com",
								Count:           2,
							},
						},
					},
				},
			},
		}

		got := TransformResourceClaim(claim)
		if got.UID != "claim-uid-1" || got.Name != "train-gpus" || got.Namespace != "ml" {
			t.Errorf("claim identity = %+v", got)
		}
		if got.Allocated || len(got.AllocatedDevices) != 0 || len(got.ReservedFor) != 0 {
			t.Errorf("pending claim must not report allocation: %+v", got)
		}
		want := []ResourceClaimDeviceRequest{{Name: "gpus", DeviceClassName: "gpu.nvidia.com", Count: 2}}
		if !reflect.DeepEqual(got.DeviceRequests, want) {
			t.Errorf("requests = %+v, want %+v", got.DeviceRequests, want)
		}
	})

	t.Run("allocated and reserved claim", func(t *testing.T) {
		claim := &resourcev1.ResourceClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "train-gpus", Namespace: "ml", UID: "claim-uid-1"},
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{
						// firstAvailable form: class/count resolved via
						// allocation results, not the request
						{Name: "gpus"},
					},
				},
			},
			Status: resourcev1.ResourceClaimStatus{
				Allocation: &resourcev1.AllocationResult{
					Devices: resourcev1.DeviceAllocationResult{
						Results: []resourcev1.DeviceRequestAllocationResult{
							{Request: "gpus", Driver: "gpu.nvidia.com", Pool: "node1", Device: "gpu-0"},
							{Request: "gpus", Driver: "gpu.nvidia.com", Pool: "node1", Device: "gpu-1"},
						},
					},
				},
				ReservedFor: []resourcev1.ResourceClaimConsumerReference{
					{Resource: "pods", Name: "trainer-0", UID: "pod-uid-1"},
				},
			},
		}

		got := TransformResourceClaim(claim)
		if !got.Allocated {
			t.Errorf("expected Allocated=true")
		}
		wantDevices := []ResourceClaimAllocatedDevice{
			{Request: "gpus", Driver: "gpu.nvidia.com", Pool: "node1", Device: "gpu-0"},
			{Request: "gpus", Driver: "gpu.nvidia.com", Pool: "node1", Device: "gpu-1"},
		}
		if !reflect.DeepEqual(got.AllocatedDevices, wantDevices) {
			t.Errorf("allocated devices = %+v, want %+v", got.AllocatedDevices, wantDevices)
		}
		wantConsumers := []ResourceClaimConsumer{{Resource: "pods", Name: "trainer-0", UID: "pod-uid-1"}}
		if !reflect.DeepEqual(got.ReservedFor, wantConsumers) {
			t.Errorf("reservedFor = %+v, want %+v", got.ReservedFor, wantConsumers)
		}
		// firstAvailable request transforms with empty class/count
		if got.DeviceRequests[0].DeviceClassName != "" || got.DeviceRequests[0].Count != 0 {
			t.Errorf("firstAvailable request should have empty class/count: %+v", got.DeviceRequests[0])
		}
	})
}

func TestDeviceAttributeString(t *testing.T) {
	cases := map[string]struct {
		attr resourcev1.DeviceAttribute
		want string
	}{
		"string":  {resourcev1.DeviceAttribute{StringValue: strPtr("GPU-1")}, "GPU-1"},
		"int":     {resourcev1.DeviceAttribute{IntValue: intPtr(42)}, "42"},
		"bool":    {resourcev1.DeviceAttribute{BoolValue: boolPtr(true)}, "true"},
		"version": {resourcev1.DeviceAttribute{VersionValue: strPtr("1.2.3")}, "1.2.3"},
		"empty":   {resourcev1.DeviceAttribute{}, ""},
	}
	for name, tc := range cases {
		if got := deviceAttributeString(tc.attr); got != tc.want {
			t.Errorf("%s: deviceAttributeString() = %q, want %q", name, got, tc.want)
		}
	}
}
