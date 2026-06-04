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
