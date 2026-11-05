package pricing

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/cloud"
	"github.com/opencost/opencost/core/pkg/unit"
)

func nodePricing(instanceType string, price float64) *NodePricing {
	return &NodePricing{
		Properties: NodePricingProperties{
			Provider:     cloud.ProviderAWS,
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
			Provider:   cloud.ProviderAWS,
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
// ordering of pricing slices.
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
		"cluster": {ClusterPricing: []*ClusterPricing{{Properties: ClusterPricingProperties{Provider: cloud.ProviderAWS}}}},
		"network": {NetworkPricing: []*NetworkPricing{{Properties: NetworkPricingProperties{Provider: cloud.ProviderAWS}}}},
		"node":    {NodePricing: []*NodePricing{nodePricing("m5.large", 0.096)}},
		"volume":  {PersistentVolumePricing: []*PersistentVolumePricing{pvPricing(VolumeTypeGP3, 0.0001)}},
		"service": {ServicePricing: []*ServicePricing{{Properties: ServicePricingProperties{Provider: cloud.ProviderAWS}}}},
	}

	for name, ps := range cases {
		if ps.IsEmpty() {
			t.Errorf("set with only %s pricing should not be empty", name)
		}
	}
}

// fullPricingSet returns a PricingSet that exercises every kind plus every
// reference-typed field (Labels maps, Prices maps, and *time.Time pointers) so
// that Clone independence can be verified end to end.
func fullPricingSet() *PricingSet {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

	node := nodePricing("m5.large", 0.096)
	node.Properties.Labels = map[string]string{"team": "platform"}
	node.Properties.Start = &start
	node.Properties.End = &end

	pv := pvPricing(VolumeTypeGP3, 0.0001)
	pv.Properties.Labels = map[string]string{"env": "prod"}
	pv.Properties.Start = &start

	return &PricingSet{
		ClusterPricing: []*ClusterPricing{{
			Properties: ClusterPricingProperties{Provider: cloud.ProviderAWS, Start: &start},
			Prices:     Prices{ResourceCluster: {Unit: unit.Hour, Price: 1.0}},
		}},
		NetworkPricing: []*NetworkPricing{{
			Properties: NetworkPricingProperties{Provider: cloud.ProviderAWS, End: &end},
			Prices:     Prices{ResourceInternetEgress: {Unit: unit.GiB, Price: 0.09}},
		}},
		NodePricing:             []*NodePricing{node},
		PersistentVolumePricing: []*PersistentVolumePricing{pv},
		ServicePricing: []*ServicePricing{{
			Properties: ServicePricingProperties{Provider: cloud.ProviderAWS, Region: "us-east-1", Start: &start},
			Prices:     Prices{ResourceService: {Unit: unit.Hour, Price: 0.025}},
		}},
	}
}

// TestCloneEquality verifies that a clone is equal to the original by checksum.
func TestCloneEquality(t *testing.T) {
	orig := fullPricingSet()
	clone := orig.Clone()

	csOrig, err := orig.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	csClone, err := clone.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if csOrig != csClone {
		t.Errorf("expected clone checksum %q to equal original %q", csClone, csOrig)
	}
}

// TestCloneIndependence verifies that mutating a clone's nested slices, maps,
// and time pointers does not affect the original.
func TestCloneIndependence(t *testing.T) {
	orig := fullPricingSet()

	csBefore, err := orig.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	clone := orig.Clone()

	// Mutate every reference-typed field reachable from the clone.
	clone.NodePricing[0].Properties.Labels["team"] = "mutated"
	clone.NodePricing[0].Prices[ResourceNode] = Price{Unit: unit.Hour, Price: 99}
	*clone.NodePricing[0].Properties.Start = time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	clone.NodePricing[0].Properties.End = nil
	clone.PersistentVolumePricing[0].Properties.Labels["env"] = "mutated"
	clone.PersistentVolumePricing[0].Prices[ResourceStorage] = Price{Unit: unit.GiBHour, Price: 99}
	clone.ClusterPricing[0].Prices[ResourceCluster] = Price{Unit: unit.Hour, Price: 99}
	clone.NetworkPricing[0].Prices[ResourceInternetEgress] = Price{Unit: unit.GiB, Price: 99}
	clone.ServicePricing[0].Prices[ResourceService] = Price{Unit: unit.Hour, Price: 99}

	// Replace whole slices to confirm the slice headers are independent too.
	clone.NodePricing = append(clone.NodePricing, nodePricing("m5.xlarge", 0.192))

	csAfter, err := orig.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if csBefore != csAfter {
		t.Errorf("mutating clone changed original: checksum %q -> %q", csBefore, csAfter)
	}
}

// TestCloneNilReceiver verifies that Clone handles a nil receiver by returning
// an empty, non-nil set rather than panicking.
func TestCloneNilReceiver(t *testing.T) {
	var ps *PricingSet
	clone := ps.Clone()

	if clone == nil {
		t.Fatal("expected non-nil clone from nil receiver")
	}
	if !clone.IsEmpty() {
		t.Errorf("expected empty clone from nil receiver")
	}
}

// TestClonePreservesNilSlices verifies that Clone does not turn nil pricing
// slices into empty ones, keeping serialization semantics stable.
func TestClonePreservesNilSlices(t *testing.T) {
	clone := (&PricingSet{}).Clone()

	if clone.NodePricing != nil {
		t.Errorf("expected nil NodePricing slice, got %v", clone.NodePricing)
	}
	if clone.ClusterPricing != nil {
		t.Errorf("expected nil ClusterPricing slice, got %v", clone.ClusterPricing)
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
