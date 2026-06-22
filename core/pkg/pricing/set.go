package pricing

import (
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
