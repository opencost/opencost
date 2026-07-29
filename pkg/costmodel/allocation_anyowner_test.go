package costmodel

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
)

// TestResToPodAnyOwnerMap covers the generic owner resolution that attributes a
// pod to its direct controller regardless of kind, so workload CRDs (PyTorchJob,
// RayCluster, ...) that have no dedicated query are no longer left unattributed.
func TestResToPodAnyOwnerMap(t *testing.T) {
	cases := map[string]struct {
		owners   []*source.OwnerResult
		expected map[podKey]controllerKey
	}{
		"attributes an unrecognized kind to its direct controller": {
			owners: []*source.OwnerResult{
				{Cluster: "cluster1", Namespace: "ns", Pod: "ray-head-0", OwnerKind: "RayCluster", OwnerName: "myray", Controller: true},
			},
			expected: map[podKey]controllerKey{
				newPodKey("cluster1", "ns", "ray-head-0"): newControllerKey("cluster1", "ns", "raycluster", "myray"),
			},
		},
		"lowercases the owner kind": {
			owners: []*source.OwnerResult{
				{Cluster: "cluster1", Namespace: "ns", Pod: "master-0", OwnerKind: "PyTorchJob", OwnerName: "bert", Controller: true},
			},
			expected: map[podKey]controllerKey{
				newPodKey("cluster1", "ns", "master-0"): newControllerKey("cluster1", "ns", "pytorchjob", "bert"),
			},
		},
		"skips non-controller owner references": {
			owners: []*source.OwnerResult{
				{Cluster: "cluster1", Namespace: "ns", Pod: "p", OwnerKind: "RayCluster", OwnerName: "myray", Controller: false},
			},
			expected: map[podKey]controllerKey{},
		},
		"skips rows missing owner name or kind": {
			owners: []*source.OwnerResult{
				{Cluster: "cluster1", Namespace: "ns", Pod: "p", OwnerKind: "", OwnerName: "x", Controller: true},
				{Cluster: "cluster1", Namespace: "ns", Pod: "p", OwnerKind: "Job", OwnerName: "", Controller: true},
			},
			expected: map[podKey]controllerKey{},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := resToPodAnyOwnerMap(tc.owners, map[podKey][]podKey{}, false)
			if len(got) != len(tc.expected) {
				t.Fatalf("expected %d entries, got %d (%v)", len(tc.expected), len(got), got)
			}
			for k, want := range tc.expected {
				gv, ok := got[k]
				if !ok {
					t.Errorf("missing expected key %s", k)
					continue
				}
				if gv != want {
					t.Errorf("key %s: expected %s, got %s", k, want, gv)
				}
			}
		})
	}
}

// TestAnyOwnerPrecedence guards the design contract: the any-owner base map is
// applied before the dedicated per-kind maps, so a pod present in both is
// attributed to the specific kind (a Deployment's pod resolves to the
// Deployment, not the ReplicaSet the generic map would otherwise assign).
func TestAnyOwnerPrecedence(t *testing.T) {
	// A Deployment's pod appears in both the generic any-owner map (as its
	// ReplicaSet) and the dedicated deployment map; the deployment must win.
	pk := newPodKey("cluster1", "ns", "web-abc")
	alloc := &opencost.Allocation{Properties: &opencost.AllocationProperties{}}

	// A pod with no controlling owner is in no map; it must stay unattributed
	// (empty controller -> __unallocated__), i.e. no regression from this change.
	barePk := newPodKey("cluster1", "ns", "bare-pod")
	bareAlloc := &opencost.Allocation{Properties: &opencost.AllocationProperties{}}

	podMap := map[podKey]*pod{
		pk:     {Allocations: map[string]*opencost.Allocation{"web": alloc}},
		barePk: {Allocations: map[string]*opencost.Allocation{"c": bareAlloc}},
	}

	anyOwner := map[podKey]controllerKey{pk: newControllerKey("cluster1", "ns", "replicaset", "web-rs")}
	deployment := map[podKey]controllerKey{pk: newControllerKey("cluster1", "ns", "deployment", "web")}

	applyControllersToPods(podMap, anyOwner)
	applyControllersToPods(podMap, deployment)

	if alloc.Properties.ControllerKind != "deployment" || alloc.Properties.Controller != "web" {
		t.Fatalf("expected deployment/web to win, got %s/%s", alloc.Properties.ControllerKind, alloc.Properties.Controller)
	}
	if bareAlloc.Properties.ControllerKind != "" || bareAlloc.Properties.Controller != "" {
		t.Fatalf("expected uncontrolled pod to stay unattributed, got %s/%s", bareAlloc.Properties.ControllerKind, bareAlloc.Properties.Controller)
	}
}
