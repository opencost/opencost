package pricing

import (
	"context"
	"errors"
	"sync"
)

type MemoryPricingStore struct {
	mu      sync.RWMutex
	pricing *PricingSet
}

func NewMemoryPricingStore() *MemoryPricingStore {
	return &MemoryPricingStore{
		pricing: &PricingSet{},
	}
}

// GetPricingSet returns a deep copy of the stored pricing set so that callers
// cannot mutate the store's internal state through the returned pointer.
func (mps *MemoryPricingStore) GetPricingSet(ctx context.Context) (*PricingSet, error) {
	mps.mu.RLock()
	defer mps.mu.RUnlock()

	return mps.pricing.Clone(), nil
}

// SetPricingSet stores a deep copy of the provided pricing set so that the
// caller cannot mutate the store's internal state after setting it.
func (mps *MemoryPricingStore) SetPricingSet(ctx context.Context, pricing *PricingSet) error {
	if pricing == nil {
		return errors.New("nil pricing")
	}

	mps.mu.Lock()
	defer mps.mu.Unlock()

	mps.pricing = pricing.Clone()

	return nil
}
