package basic

import (
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

type MemoryPricingStore struct {
	pricing *pricing.PricingSet
}

func NewMemoryPricingStore() *MemoryPricingStore {
	return &MemoryPricingStore{
		pricing: &pricing.PricingSet{},
	}
}

func (mps *MemoryPricingStore) GetCurrencies(ctx context.Context) []unit.Currency {
	currencies := map[unit.Currency]struct{}{}

	for _, np := range mps.pricing.Nodes {
		for _, curr := range np.GetCurrencies() {
			currencies[curr] = struct{}{}
		}
	}

	for _, vp := range mps.pricing.Volumes {
		for _, curr := range vp.GetCurrencies() {
			currencies[curr] = struct{}{}
		}
	}

	return slices.Collect(maps.Keys(currencies))
}

func (mps *MemoryPricingStore) GetPricingSet(ctx context.Context) (*pricing.PricingSet, error) {
	return mps.pricing, nil
}

func (mps *MemoryPricingStore) SetPricingSet(ctx context.Context, pricing *pricing.PricingSet) error {
	if pricing == nil {
		return errors.New("nil pricing")
	}

	mps.pricing = pricing

	return nil
}
