package pricing

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/model/shared"
	"github.com/opencost/opencost/core/pkg/unit"
)

func nodePricing(instanceType string, price float64) *NodePricing {
	return &NodePricing{
		Properties: NodePricingProperties{
			Provider:     shared.Provider("AWS"),
			Region:       "us-east-1",
			InstanceType: instanceType,
		},
		Prices: Prices{
			ResourceNode: {Unit: unit.Hour, Price: price},
		},
	}
}

func pvPricing(volumeType VolumeType, price float64) *PersistentVolumePricing {
	return &PersistentVolumePricing{
		Properties: PersistentVolumePricingProperties{
			Provider:   shared.Provider("AWS"),
			Region:     "us-east-1",
			VolumeType: volumeType,
		},
		Prices: Prices{
			ResourceStorage: {Unit: unit.GiBHour, Price: price},
		},
	}
}

// TestChecksumPriceSensitivity verifies that the checksum changes when only a
// price value changes, even if all properties are identical.
func TestChecksumPriceSensitivity(t *testing.T) {
	a := &PricingSet{NodePricing: []*NodePricing{nodePricing("m5.large", 0.096)}}
	b := &PricingSet{NodePricing: []*NodePricing{nodePricing("m5.large", 0.192)}}

	csA, err := a.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	csB, err := b.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if csA == csB {
		t.Errorf("expected differing checksums for differing prices, got %q for both", csA)
	}
}

// TestChecksumOrderStability verifies that the checksum is independent of the
// ordering of pricing slices and that computing it does not mutate the input.
func TestChecksumOrderStability(t *testing.T) {
	n1 := nodePricing("m5.large", 0.096)
	n2 := nodePricing("m5.xlarge", 0.192)
	n3 := nodePricing("m5.2xlarge", 0.384)

	forward := &PricingSet{NodePricing: []*NodePricing{n1, n2, n3}}
	reverse := &PricingSet{NodePricing: []*NodePricing{n3, n2, n1}}

	csForward, err := forward.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	csReverse, err := reverse.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if csForward != csReverse {
		t.Errorf("expected checksum to be order-independent, got %q vs %q", csForward, csReverse)
	}

	// Checksum must not mutate the caller's slice ordering.
	if forward.NodePricing[0] != n1 || forward.NodePricing[1] != n2 || forward.NodePricing[2] != n3 {
		t.Errorf("Checksum mutated the input NodePricing slice ordering")
	}
}

// TestChecksumNilReceiver verifies that Checksum handles a nil receiver like
// IsEmpty and Currencies do, rather than panicking.
func TestChecksumNilReceiver(t *testing.T) {
	var ps *PricingSet
	if _, err := ps.Checksum(); err != nil {
		t.Errorf("unexpected error on nil receiver: %v", err)
	}
}

// TestIsEmptyAllKinds verifies that a set holding only Cluster/Network/Service
// pricing is not reported empty.
func TestIsEmptyAllKinds(t *testing.T) {
	if !(&PricingSet{}).IsEmpty() {
		t.Errorf("expected empty set to report empty")
	}

	cases := map[string]*PricingSet{
		"cluster": {ClusterPricing: []*ClusterPricing{{Properties: ClusterPricingProperties{Provider: shared.Provider("AWS")}}}},
		"network": {NetworkPricing: []*NetworkPricing{{Properties: NetworkPricingProperties{Provider: shared.Provider("AWS")}}}},
		"node":    {NodePricing: []*NodePricing{nodePricing("m5.large", 0.096)}},
		"volume":  {PersistentVolumePricing: []*PersistentVolumePricing{pvPricing(VolumeTypeGP3, 0.0001)}},
		"service": {ServicePricing: []*ServicePricing{{Properties: ServicePricingProperties{Provider: shared.Provider("AWS")}}}},
	}

	for name, ps := range cases {
		if ps.IsEmpty() {
			t.Errorf("set with only %s pricing should not be empty", name)
		}
	}
}

// TestMockGetPricingSetAllKinds verifies that the mock's GetPricingSet exposes
// the same kinds as its readers, not just node + persistent volume.
func TestMockGetPricingSetAllKinds(t *testing.T) {
	mpm, err := NewMockPricingModule()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ps, err := mpm.GetPricingSet(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(ps.NodePricing) != len(mpm.NodePricing) {
		t.Errorf("expected %d node pricing, got %d", len(mpm.NodePricing), len(ps.NodePricing))
	}
	if len(ps.PersistentVolumePricing) != len(mpm.PersistentVolumePricing) {
		t.Errorf("expected %d volume pricing, got %d", len(mpm.PersistentVolumePricing), len(ps.PersistentVolumePricing))
	}
	if len(ps.ClusterPricing) != len(mpm.ClusterPricing) {
		t.Errorf("expected %d cluster pricing, got %d", len(mpm.ClusterPricing), len(ps.ClusterPricing))
	}
	if len(ps.NetworkPricing) != len(mpm.NetworkPricing) {
		t.Errorf("expected %d network pricing, got %d", len(mpm.NetworkPricing), len(ps.NetworkPricing))
	}
	if len(ps.ServicePricing) != len(mpm.ServicePricing) {
		t.Errorf("expected %d service pricing, got %d", len(mpm.ServicePricing), len(ps.ServicePricing))
	}
}
