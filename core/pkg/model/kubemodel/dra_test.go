package kubemodel

import (
	"reflect"
	"testing"
	"time"
)

func mockDRAResourceClaim() *DRAResourceClaim {
	return &DRAResourceClaim{
		UID:       "claim-uid-1",
		Name:      "train-gpus",
		Namespace: "ml",
		DeviceRequests: []DRADeviceRequest{
			{Name: "gpus", DeviceClassName: "gpu.nvidia.com", Count: 2},
		},
		Allocated: true,
		AllocatedDevices: []DRAAllocatedDevice{
			{Request: "gpus", Driver: "gpu.nvidia.com", Pool: "node1", Device: "gpu-0", DeviceUUID: "GPU-1"},
			{Request: "gpus", Driver: "gpu.nvidia.com", Pool: "node1", Device: "gpu-1", DeviceUUID: "GPU-2"},
		},
		ReservedForPodUIDs: []string{"pod-uid-1"},
	}
}

func mockDRAResourceSlice() *DRAResourceSlice {
	return &DRAResourceSlice{
		Name:     "node1-gpu-slice",
		Driver:   "gpu.nvidia.com",
		Pool:     "node1",
		NodeName: "node1",
		Devices: []DRASliceDevice{
			{
				Name:       "gpu-0",
				UUID:       "GPU-1",
				Attributes: map[string]string{"gpu.nvidia.com/uuid": "GPU-1", "index": "0"},
				Capacity:   map[string]string{"memory": "80Gi"},
			},
			{Name: "gpu-1", UUID: "GPU-2"},
		},
	}
}

func TestDeviceUUIDFromAttributes(t *testing.T) {
	cases := map[string]struct {
		attrs map[string]string
		want  string
	}{
		"plain uuid":            {map[string]string{"uuid": "GPU-1"}, "GPU-1"},
		"driver-qualified uuid": {map[string]string{"gpu.nvidia.com/uuid": "GPU-2"}, "GPU-2"},
		"plain wins over qualified": {
			map[string]string{"uuid": "GPU-1", "gpu.nvidia.com/uuid": "GPU-2"}, "GPU-1",
		},
		"no uuid attribute": {map[string]string{"index": "0"}, ""},
		"nil attributes":    {nil, ""},
		"suffix must follow slash": {
			map[string]string{"notuuid": "GPU-9"}, "",
		},
	}
	for name, tc := range cases {
		if got := DeviceUUIDFromAttributes(tc.attrs); got != tc.want {
			t.Errorf("%s: DeviceUUIDFromAttributes() = %q, want %q", name, got, tc.want)
		}
	}
}

func TestDRAResourceClaim_Validate(t *testing.T) {
	cases := map[string]struct {
		mutate  func(*DRAResourceClaim)
		wantErr bool
	}{
		"valid":        {func(c *DRAResourceClaim) {}, false},
		"missing uid":  {func(c *DRAResourceClaim) { c.UID = "" }, true},
		"missing name": {func(c *DRAResourceClaim) { c.Name = "" }, true},
		"devices without allocated flag": {
			func(c *DRAResourceClaim) { c.Allocated = false }, true,
		},
		"negative request count": {
			func(c *DRAResourceClaim) { c.DeviceRequests[0].Count = -1 }, true,
		},
		"pending claim": {
			func(c *DRAResourceClaim) {
				c.Allocated = false
				c.AllocatedDevices = nil
				c.ReservedForPodUIDs = nil
			}, false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			claim := mockDRAResourceClaim()
			tc.mutate(claim)
			if err := claim.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestDRAResourceSlice_Validate(t *testing.T) {
	slice := mockDRAResourceSlice()
	if err := slice.Validate(); err != nil {
		t.Errorf("Validate() unexpected error: %v", err)
	}
	slice.Name = ""
	if err := slice.Validate(); err == nil {
		t.Errorf("expected error for missing name")
	}
	slice = mockDRAResourceSlice()
	slice.Driver = ""
	if err := slice.Validate(); err == nil {
		t.Errorf("expected error for missing driver")
	}
}

func TestRegisterDRA(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	kms := NewKubeModelSet(start, start.Add(time.Hour))

	if err := kms.RegisterDRAResourceClaim(mockDRAResourceClaim()); err != nil {
		t.Fatalf("RegisterDRAResourceClaim: %v", err)
	}
	// duplicate registration is a no-op
	if err := kms.RegisterDRAResourceClaim(mockDRAResourceClaim()); err != nil {
		t.Fatalf("duplicate RegisterDRAResourceClaim: %v", err)
	}
	if err := kms.RegisterDRAResourceSlice(mockDRAResourceSlice()); err != nil {
		t.Fatalf("RegisterDRAResourceSlice: %v", err)
	}

	if len(kms.ResourceClaims) != 1 || len(kms.ResourceSlices) != 1 {
		t.Errorf("registered counts = (%d claims, %d slices), want (1, 1)", len(kms.ResourceClaims), len(kms.ResourceSlices))
	}
	if kms.Metadata.ObjectCount != 2 {
		t.Errorf("ObjectCount = %d, want 2", kms.Metadata.ObjectCount)
	}

	// invalid objects are rejected and recorded
	if err := kms.RegisterDRAResourceClaim(&DRAResourceClaim{}); err == nil {
		t.Errorf("expected error registering invalid claim")
	}
	if err := kms.RegisterDRAResourceSlice(&DRAResourceSlice{}); err == nil {
		t.Errorf("expected error registering invalid slice")
	}
}

// TestDRA_BinaryRoundtrip verifies claims and slices survive the bingen
// codec standalone and through a KubeModelSet, and that absence stays
// absent.
func TestDRA_BinaryRoundtrip(t *testing.T) {
	t.Run("claim", func(t *testing.T) {
		orig := mockDRAResourceClaim()
		bs, err := orig.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %s", err)
		}
		decoded := new(DRAResourceClaim)
		if err := decoded.UnmarshalBinary(bs); err != nil {
			t.Fatalf("UnmarshalBinary: %s", err)
		}
		if !reflect.DeepEqual(orig, decoded) {
			t.Errorf("claim roundtrip mismatch:\n got %+v\nwant %+v", decoded, orig)
		}
	})

	t.Run("slice", func(t *testing.T) {
		orig := mockDRAResourceSlice()
		bs, err := orig.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %s", err)
		}
		decoded := new(DRAResourceSlice)
		if err := decoded.UnmarshalBinary(bs); err != nil {
			t.Fatalf("UnmarshalBinary: %s", err)
		}
		if !reflect.DeepEqual(orig, decoded) {
			t.Errorf("slice roundtrip mismatch:\n got %+v\nwant %+v", decoded, orig)
		}
	})

	t.Run("empty and pending shapes", func(t *testing.T) {
		pending := &DRAResourceClaim{UID: "u", Name: "n", Namespace: "ns"}
		bs, err := pending.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %s", err)
		}
		decoded := new(DRAResourceClaim)
		if err := decoded.UnmarshalBinary(bs); err != nil {
			t.Fatalf("UnmarshalBinary: %s", err)
		}
		if !reflect.DeepEqual(pending, decoded) {
			t.Errorf("pending claim roundtrip mismatch:\n got %+v\nwant %+v", decoded, pending)
		}
	})

	t.Run("kubemodelset", func(t *testing.T) {
		start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
		kms := NewKubeModelSet(start, start.Add(time.Hour))
		kms.Cluster = &Cluster{UID: "c1", Name: "c1", Start: start, End: start.Add(time.Hour)}
		if err := kms.RegisterDRAResourceClaim(mockDRAResourceClaim()); err != nil {
			t.Fatalf("register claim: %v", err)
		}
		if err := kms.RegisterDRAResourceSlice(mockDRAResourceSlice()); err != nil {
			t.Fatalf("register slice: %v", err)
		}

		bs, err := kms.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %s", err)
		}
		decoded := new(KubeModelSet)
		if err := decoded.UnmarshalBinary(bs); err != nil {
			t.Fatalf("UnmarshalBinary: %s", err)
		}

		if !reflect.DeepEqual(kms.ResourceClaims, decoded.ResourceClaims) {
			t.Errorf("claims did not survive set roundtrip:\n got %+v\nwant %+v", decoded.ResourceClaims, kms.ResourceClaims)
		}
		if !reflect.DeepEqual(kms.ResourceSlices, decoded.ResourceSlices) {
			t.Errorf("slices did not survive set roundtrip:\n got %+v\nwant %+v", decoded.ResourceSlices, kms.ResourceSlices)
		}
	})
}
