package huawei

import (
	"testing"

	v1 "k8s.io/api/core/v1"

	"github.com/opencost/opencost/core/pkg/clustercache"
)

const testPVEVSID = "66666666-7777-8888-9999-000000000000"

// TestGetPVKey_EVSVolumeID checks an everest PV carries its EVS volume ID.
func TestGetPVKey_EVSVolumeID(t *testing.T) {
	h := &Huawei{}
	pv := &clustercache.PersistentVolume{Spec: v1.PersistentVolumeSpec{
		StorageClassName: "csi-disk",
		PersistentVolumeSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{Driver: "disk.csi.everest.io", VolumeHandle: testPVEVSID},
		},
	}}
	if got := h.GetPVKey(pv, nil, "la-south-2").ID(); got != testPVEVSID {
		t.Fatalf("expected the EVS volume ID %s, got %q", testPVEVSID, got)
	}
	if got := h.GetPVKey(&clustercache.PersistentVolume{}, nil, "la-south-2").ID(); got != "" {
		t.Fatalf("expected no ID for a non-CSI volume, got %q", got)
	}
}
