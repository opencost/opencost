package pricing

import (
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
// sensitive to both pricing properties and price values. It does not mutate
// the receiver.
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
// Sorted by: Provider, Region, <Instance/Volume>Type
func (ps *PricingSet) Sort() {
	if ps == nil {
		return
	}

	// Sort nodes
	slices.SortFunc(ps.NodePricing, func(a, b *NodePricing) int {
		// Compare by Provider
		if a.Properties.Provider != b.Properties.Provider {
			if a.Properties.Provider < b.Properties.Provider {
				return -1
			}
			return 1
		}

		// Compare by Region
		if a.Properties.Region != b.Properties.Region {
			if a.Properties.Region < b.Properties.Region {
				return -1
			}
			return 1
		}

		// Compare by InstanceType
		if a.Properties.InstanceType != b.Properties.InstanceType {
			if a.Properties.InstanceType < b.Properties.InstanceType {
				return -1
			}
			return 1
		}

		return 0
	})

	// Sort volumes
	slices.SortFunc(ps.PersistentVolumePricing, func(a, b *PersistentVolumePricing) int {
		// Compare by Provider
		if a.Properties.Provider != b.Properties.Provider {
			if a.Properties.Provider < b.Properties.Provider {
				return -1
			}
			return 1
		}

		// Compare by Region
		if a.Properties.Region != b.Properties.Region {
			if a.Properties.Region < b.Properties.Region {
				return -1
			}
			return 1
		}

		// Compare by VolumeType
		if a.Properties.VolumeType < b.Properties.VolumeType {
			return -1
		}
		if a.Properties.VolumeType > b.Properties.VolumeType {
			return 1
		}

		return 0
	})
}
