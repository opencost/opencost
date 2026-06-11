package kubemodel

import (
	"reflect"
	"testing"
	"time"

	cc "github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
)

func draTestSlices() []*cc.ResourceSlice {
	return []*cc.ResourceSlice{
		{
			Name:     "node1-gpu-slice",
			Driver:   "gpu.nvidia.com",
			Pool:     "node1",
			NodeName: "node1",
			Devices: []cc.ResourceSliceDevice{
				{
					Name:       "gpu-0",
					Attributes: map[string]string{"gpu.nvidia.com/uuid": "GPU-1", "index": "0"},
					Capacity:   map[string]string{"memory": "80Gi"},
				},
				// no uuid attribute published
				{Name: "gpu-1", Attributes: map[string]string{"index": "1"}},
			},
		},
	}
}

func draTestClaims() []*cc.ResourceClaim {
	return []*cc.ResourceClaim{
		{
			UID:       "claim-uid-1",
			Name:      "train-gpus",
			Namespace: "ml",
			DeviceRequests: []cc.ResourceClaimDeviceRequest{
				{Name: "gpus", DeviceClassName: "gpu.nvidia.com", Count: 2},
			},
			Allocated: true,
			AllocatedDevices: []cc.ResourceClaimAllocatedDevice{
				{Request: "gpus", Driver: "gpu.nvidia.com", Pool: "node1", Device: "gpu-0"},
				{Request: "gpus", Driver: "gpu.nvidia.com", Pool: "node1", Device: "gpu-1"},
			},
			ReservedFor: []cc.ResourceClaimConsumer{
				{Resource: "pods", Name: "trainer-0", UID: "pod-uid-1"},
				{Resource: "deployments", Name: "owner", UID: "deploy-uid-1"},
			},
		},
		{
			UID:       "claim-uid-2",
			Name:      "pending",
			Namespace: "ml",
			DeviceRequests: []cc.ResourceClaimDeviceRequest{
				{Name: "gpu", DeviceClassName: "gpu.nvidia.com", Count: 1},
			},
		},
	}
}

func TestComputeDRA(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	kms := kubemodel.NewKubeModelSet(start, start.Add(time.Hour))

	km := &KubeModel{clusterCache: &cc.MockClusterCache{
		ResourceSlices: draTestSlices(),
		ResourceClaims: draTestClaims(),
	}}

	if err := km.computeDRA(kms); err != nil {
		t.Fatalf("computeDRA: %v", err)
	}

	if len(kms.ResourceSlices) != 1 {
		t.Fatalf("expected 1 slice, got %d", len(kms.ResourceSlices))
	}
	slice := kms.ResourceSlices["node1-gpu-slice"]
	if slice.Driver != "gpu.nvidia.com" || slice.NodeName != "node1" {
		t.Errorf("slice identity = %+v", slice)
	}
	// UUID resolved from the driver-qualified attribute
	if slice.Devices[0].UUID != "GPU-1" || slice.Devices[0].Capacity["memory"] != "80Gi" {
		t.Errorf("slice device 0 = %+v", slice.Devices[0])
	}
	if slice.Devices[1].UUID != "" {
		t.Errorf("device without uuid attribute must keep empty UUID: %+v", slice.Devices[1])
	}

	if len(kms.ResourceClaims) != 2 {
		t.Fatalf("expected 2 claims, got %d", len(kms.ResourceClaims))
	}
	claim := kms.ResourceClaims["claim-uid-1"]
	if !claim.Allocated || claim.Namespace != "ml" {
		t.Errorf("claim = %+v", claim)
	}

	// the allocation join: device gpu-0 resolves to its telemetry UUID,
	// gpu-1 (no published uuid) stays empty rather than fabricated
	wantAllocated := []kubemodel.DRAAllocatedDevice{
		{Request: "gpus", Driver: "gpu.nvidia.com", Pool: "node1", Device: "gpu-0", DeviceUUID: "GPU-1"},
		{Request: "gpus", Driver: "gpu.nvidia.com", Pool: "node1", Device: "gpu-1", DeviceUUID: ""},
	}
	if !reflect.DeepEqual(claim.AllocatedDevices, wantAllocated) {
		t.Errorf("allocated devices = %+v, want %+v", claim.AllocatedDevices, wantAllocated)
	}

	// only pod consumers become workload associations
	if !reflect.DeepEqual(claim.ReservedForPodUIDs, []string{"pod-uid-1"}) {
		t.Errorf("ReservedForPodUIDs = %v, want [pod-uid-1]", claim.ReservedForPodUIDs)
	}

	// pending claim carries its request but no allocation
	pending := kms.ResourceClaims["claim-uid-2"]
	if pending.Allocated || len(pending.AllocatedDevices) != 0 {
		t.Errorf("pending claim must not report allocation: %+v", pending)
	}
	if pending.DeviceRequests[0].Count != 1 || pending.DeviceRequests[0].DeviceClassName != "gpu.nvidia.com" {
		t.Errorf("pending request = %+v", pending.DeviceRequests[0])
	}
}

func TestComputeDRA_Absent(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	// nil cache (no k8s access): no-op
	kms := kubemodel.NewKubeModelSet(start, start.Add(time.Hour))
	km := &KubeModel{}
	if err := km.computeDRA(kms); err != nil {
		t.Fatalf("computeDRA with nil cache: %v", err)
	}
	if len(kms.ResourceClaims) != 0 || len(kms.ResourceSlices) != 0 {
		t.Errorf("expected empty DRA maps with nil cache")
	}

	// cache without DRA API: GetAll returns nil, still a no-op
	km = &KubeModel{clusterCache: &cc.MockClusterCache{}}
	if err := km.computeDRA(kms); err != nil {
		t.Fatalf("computeDRA without DRA API: %v", err)
	}
	if len(kms.ResourceClaims) != 0 || len(kms.ResourceSlices) != 0 {
		t.Errorf("expected empty DRA maps without DRA API")
	}
}

func TestTransformDRAResourceClaims_UUIDJoin(t *testing.T) {
	// claims referencing devices from different pools/drivers must not
	// cross-match
	uuids := map[draDeviceKey]string{
		{driver: "gpu.nvidia.com", pool: "node1", device: "gpu-0"}: "GPU-1",
		{driver: "gpu.nvidia.com", pool: "node2", device: "gpu-0"}: "GPU-9",
	}
	claims := []*cc.ResourceClaim{{
		UID: "c1", Name: "c", Namespace: "ns", Allocated: true,
		AllocatedDevices: []cc.ResourceClaimAllocatedDevice{
			{Driver: "gpu.nvidia.com", Pool: "node2", Device: "gpu-0"},
			{Driver: "other.driver", Pool: "node1", Device: "gpu-0"},
		},
	}}

	got := transformDRAResourceClaims(claims, uuids)
	if got[0].AllocatedDevices[0].DeviceUUID != "GPU-9" {
		t.Errorf("pool must disambiguate: got %q, want GPU-9", got[0].AllocatedDevices[0].DeviceUUID)
	}
	if got[0].AllocatedDevices[1].DeviceUUID != "" {
		t.Errorf("unknown driver must not match: got %q", got[0].AllocatedDevices[1].DeviceUUID)
	}
}
