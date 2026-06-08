package pricing

import (
	"context"
	"errors"
)

type MemoryPricingStore struct {
	pricing *PricingSet
}

func NewMemoryPricingStore() *MemoryPricingStore {
	return &MemoryPricingStore{
		pricing: &PricingSet{},
	}
}

func (mps *MemoryPricingStore) GetPricingSet(ctx context.Context) (*PricingSet, error) {
	return mps.pricing, nil
}

func (mps *MemoryPricingStore) SetPricingSet(ctx context.Context, pricing *PricingSet) error {
	if pricing == nil {
		return errors.New("nil pricing")
	}

	mps.pricing = pricing

	return nil
}
