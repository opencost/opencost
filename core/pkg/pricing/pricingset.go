package pricing

import (
	"cmp"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"slices"
	"time"
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

// Clone returns a deep copy of the PricingSet. Mutating the returned set
// (including its nested slices, maps, and time pointers) does not affect the
// original, and vice versa. A nil receiver returns an empty, non-nil set.
func (ps *PricingSet) Clone() *PricingSet {
	if ps == nil {
		return &PricingSet{}
	}

	return &PricingSet{
		ClusterPricing:          cloneSlice(ps.ClusterPricing, (*ClusterPricing).clone),
		NetworkPricing:          cloneSlice(ps.NetworkPricing, (*NetworkPricing).clone),
		NodePricing:             cloneSlice(ps.NodePricing, (*NodePricing).clone),
		PersistentVolumePricing: cloneSlice(ps.PersistentVolumePricing, (*PersistentVolumePricing).clone),
		ServicePricing:          cloneSlice(ps.ServicePricing, (*ServicePricing).clone),
	}
}

// cloneSlice deep-copies a slice of pointers using the provided element clone
// function, preserving a nil source slice as nil.
func cloneSlice[T any](src []*T, clone func(*T) *T) []*T {
	if src == nil {
		return nil
	}

	dst := make([]*T, len(src))
	for i, e := range src {
		dst[i] = clone(e)
	}

	return dst
}

func (cp *ClusterPricing) clone() *ClusterPricing {
	if cp == nil {
		return nil
	}

	clone := *cp
	clone.Properties.Start = cloneTime(cp.Properties.Start)
	clone.Properties.End = cloneTime(cp.Properties.End)
	clone.Prices = clonePrices(cp.Prices)

	return &clone
}

func (np *NetworkPricing) clone() *NetworkPricing {
	if np == nil {
		return nil
	}

	clone := *np
	clone.Properties.Start = cloneTime(np.Properties.Start)
	clone.Properties.End = cloneTime(np.Properties.End)
	clone.Prices = clonePrices(np.Prices)

	return &clone
}

func (np *NodePricing) clone() *NodePricing {
	if np == nil {
		return nil
	}

	clone := *np
	clone.Properties.Labels = cloneLabels(np.Properties.Labels)
	clone.Properties.Start = cloneTime(np.Properties.Start)
	clone.Properties.End = cloneTime(np.Properties.End)
	clone.Prices = clonePrices(np.Prices)

	return &clone
}

func (pvp *PersistentVolumePricing) clone() *PersistentVolumePricing {
	if pvp == nil {
		return nil
	}

	clone := *pvp
	clone.Properties.Labels = cloneLabels(pvp.Properties.Labels)
	clone.Properties.Start = cloneTime(pvp.Properties.Start)
	clone.Properties.End = cloneTime(pvp.Properties.End)
	clone.Prices = clonePrices(pvp.Prices)

	return &clone
}

func (sp *ServicePricing) clone() *ServicePricing {
	if sp == nil {
		return nil
	}

	clone := *sp
	clone.Properties.Start = cloneTime(sp.Properties.Start)
	clone.Properties.End = cloneTime(sp.Properties.End)
	clone.Prices = clonePrices(sp.Prices)

	return &clone
}

// clonePrices returns a deep copy of a Prices map, preserving nil as nil. Price
// values contain no reference fields, so a shallow value copy per entry is safe.
func clonePrices(src Prices) Prices {
	if src == nil {
		return nil
	}

	dst := make(Prices, len(src))
	for k, v := range src {
		dst[k] = v
	}

	return dst
}

// cloneLabels returns a deep copy of a labels map, preserving nil as nil.
func cloneLabels(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}

	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}

	return dst
}

// cloneTime returns a copy of the time pointer, preserving nil as nil.
func cloneTime(src *time.Time) *time.Time {
	if src == nil {
		return nil
	}

	t := *src

	return &t
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
