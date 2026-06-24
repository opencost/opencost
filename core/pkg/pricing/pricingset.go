package pricing

import (
	"cmp"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"slices"
)

type PricingSet struct {
	ClusterPricing          []*ClusterPricing          `json:"clusterPricing" yaml:"clusterPricing"`
	NetworkPricing          []*NetworkPricing          `json:"networkPricing" yaml:"networkPricing"`
	NodePricing             []*NodePricing             `json:"nodePricing" yaml:"nodePricing"`
	PersistentVolumePricing []*PersistentVolumePricing `json:"persistentVolumePricing" yaml:"persistentVolumePricing"`
	ServicePricing          []*ServicePricing          `json:"servicePricing" yaml:"servicePricing"`
}

func (ps *PricingSet) IsEmpty() bool {
	if ps == nil {
		return true
	}

	return len(ps.ClusterPricing) == 0 &&
		len(ps.NetworkPricing) == 0 &&
		len(ps.NodePricing) == 0 &&
		len(ps.PersistentVolumePricing) == 0 &&
		len(ps.ServicePricing) == 0
}

// Checksum returns a hash that is stable across map and slice ordering and
// sensitive to both pricing properties and price values.
//
// TODO: Consider commutative hash folding via a multiset hashing algorithm
// if the string-based Checksum() implementation is too resource intensive
// for large pricing sets. For now, this string version is more readable.
func (ps *PricingSet) Checksum() (string, error) {
	if ps == nil {
		ps = &PricingSet{}
	}

	// Each item's String() is prefixed with its kind so that items of
	// different kinds cannot collide, then all keys are sorted to make the
	// hash independent of input ordering.
	keys := make([]string, 0,
		len(ps.ClusterPricing)+
			len(ps.NetworkPricing)+
			len(ps.NodePricing)+
			len(ps.PersistentVolumePricing)+
			len(ps.ServicePricing))

	for _, cp := range ps.ClusterPricing {
		keys = append(keys, "cluster:"+cp.String())
	}
	for _, np := range ps.NetworkPricing {
		keys = append(keys, "network:"+np.String())
	}
	for _, np := range ps.NodePricing {
		keys = append(keys, "node:"+np.String())
	}
	for _, pvp := range ps.PersistentVolumePricing {
		keys = append(keys, "persistentvolume:"+pvp.String())
	}
	for _, sp := range ps.ServicePricing {
		keys = append(keys, "service:"+sp.String())
	}

	slices.Sort(keys)

	hasher := fnv.New64a()
	for _, key := range keys {
		if _, err := hasher.Write([]byte(key)); err != nil {
			return "", fmt.Errorf("fnv hash: %w", err)
		}
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// Sort sorts the pricing data to ensure deterministic serialization.
func (ps *PricingSet) Sort() {
	if ps == nil {
		return
	}

	// Sort clusters
	slices.SortFunc(ps.ClusterPricing, func(a, b *ClusterPricing) int {
		return cmp.Compare(a.String(), b.String())
	})

	// Sort network
	slices.SortFunc(ps.NetworkPricing, func(a, b *NetworkPricing) int {
		return cmp.Compare(a.String(), b.String())
	})

	// Sort nodes
	slices.SortFunc(ps.NodePricing, func(a, b *NodePricing) int {
		return cmp.Compare(a.String(), b.String())
	})

	// Sort persistent volumes
	slices.SortFunc(ps.PersistentVolumePricing, func(a, b *PersistentVolumePricing) int {
		return cmp.Compare(a.String(), b.String())
	})

	// Sort services
	slices.SortFunc(ps.ServicePricing, func(a, b *ServicePricing) int {
		return cmp.Compare(a.String(), b.String())
	})
}
