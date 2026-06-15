package pricing

import (
	"fmt"
	"maps"
	"slices"

	"github.com/opencost/opencost/core/pkg/unit"
)

type PricingSet struct {
	Nodes   []*NodePricing   `json:"nodes" yaml:"nodes"`
	Volumes []*VolumePricing `json:"volumes" yaml:"volumes"`
}

func (ps *PricingSet) IsEmpty() bool {
	if ps == nil {
		return true
	}

	return len(ps.Nodes) == 0 && len(ps.Volumes) == 0
}

func (ps *PricingSet) Currencies() []unit.Currency {
	if ps == nil {
		return []unit.Currency{}
	}

	currencies := map[unit.Currency]struct{}{}

	for _, np := range ps.Nodes {
		for _, curr := range np.GetCurrencies() {
			currencies[curr] = struct{}{}
		}
	}

	for _, vp := range ps.Volumes {
		for _, curr := range vp.GetCurrencies() {
			currencies[curr] = struct{}{}
		}
	}

	return slices.Collect(maps.Keys(currencies))
}


// Sort sorts the pricing data to ensure deterministic serialization.
// Sorted by: Provider, Region, <Instance/Volume>Type
func (ps *PricingSet) Sort() {
	if ps == nil {
		return
	}

	// Sort nodes
	slices.SortFunc(ps.Nodes, func(a, b *NodePricing) int {
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
	slices.SortFunc(ps.Volumes, func(a, b *VolumePricing) int {
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

func nodeMergeKey(node *NodePricing) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s",
		node.Properties.Provider,
		node.Properties.Region,
		node.Properties.InstanceType,
		node.Properties.Provisioning,
		node.Properties.Commitment,
	)
}

func volumeMergeKey(volume *VolumePricing) string {
	return fmt.Sprintf("%s:%s:%s",
		volume.Properties.Provider,
		volume.Properties.Region,
		volume.Properties.VolumeType,
	)
}

func mergePrices(dst Prices, src Prices) {
	if dst == nil || src == nil {
		return
	}

	for curr, prices := range src {
		dst[curr] = prices
	}
}

func normalizeNodes(nodes []*NodePricing) []*NodePricing {
	nodeMap := make(map[string]*NodePricing)
	result := make([]*NodePricing, 0, len(nodes))

	for _, node := range nodes {
		key := nodeMergeKey(node)
		if existing, ok := nodeMap[key]; ok {
			mergePrices(existing.Prices, node.Prices)
			continue
		}

		nodeMap[key] = node
		result = append(result, node)
	}

	return result
}

func normalizeVolumes(volumes []*VolumePricing) []*VolumePricing {
	volumeMap := make(map[string]*VolumePricing)
	result := make([]*VolumePricing, 0, len(volumes))

	for _, volume := range volumes {
		key := volumeMergeKey(volume)
		if existing, ok := volumeMap[key]; ok {
			mergePrices(existing.Prices, volume.Prices)
			continue
		}

		volumeMap[key] = volume
		result = append(result, volume)
	}

	return result
}

// Normalize collapses duplicate logical pricing entries into a single record.
func (ps *PricingSet) Normalize() {
	if ps == nil {
		return
	}

	ps.Nodes = normalizeNodes(ps.Nodes)
	ps.Volumes = normalizeVolumes(ps.Volumes)
}

// Merge merges another PricingSet into this one
func (ps *PricingSet) Merge(other *PricingSet) {
	if ps == nil || other == nil {
		return
	}

	otherNodes := normalizeNodes(other.Nodes)
	otherVolumes := normalizeVolumes(other.Volumes)

	nodeMap := make(map[string]*NodePricing, len(ps.Nodes))
	volumeMap := make(map[string]*VolumePricing, len(ps.Volumes))

	for _, node := range ps.Nodes {
		nodeMap[nodeMergeKey(node)] = node
	}
	for _, volume := range ps.Volumes {
		volumeMap[volumeMergeKey(volume)] = volume
	}

	for _, node := range otherNodes {
		key := nodeMergeKey(node)
		if existingNode, exists := nodeMap[key]; exists {
			mergePrices(existingNode.Prices, node.Prices)
		} else {
			nodeMap[key] = node
			ps.Nodes = append(ps.Nodes, node)
		}
	}

	for _, volume := range otherVolumes {
		key := volumeMergeKey(volume)
		if existingVolume, exists := volumeMap[key]; exists {
			mergePrices(existingVolume.Prices, volume.Prices)
		} else {
			volumeMap[key] = volume
			ps.Volumes = append(ps.Volumes, volume)
		}
	}
}
