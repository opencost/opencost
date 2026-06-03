package basic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/unit"
)

type StoragePricingStore struct {
	store storage.Storage
	path  string
}

func NewStoragePricingStore(store storage.Storage, path string) *StoragePricingStore {
	return &StoragePricingStore{
		store: store,
		path:  path,
	}
}

func (sps *StoragePricingStore) GetCurrencies(ctx context.Context) ([]unit.Currency, error) {
	currencies := map[unit.Currency]struct{}{}

	pricing, err := sps.GetPricingSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting pricing: %w", err)
	}

	for _, np := range pricing.Nodes {
		for _, curr := range np.GetCurrencies() {
			currencies[curr] = struct{}{}
		}
	}

	for _, vp := range pricing.Volumes {
		for _, curr := range vp.GetCurrencies() {
			currencies[curr] = struct{}{}
		}
	}

	return slices.Collect(maps.Keys(currencies)), nil
}

func (sps *StoragePricingStore) GetPricingSet(ctx context.Context) (*pricing.PricingSet, error) {
	data, err := sps.store.Read(sps.path)
	if err != nil {
		return nil, fmt.Errorf("reading path '%s': %w", sps.path, err)
	}

	var pricing *pricing.PricingSet
	err = json.Unmarshal(data, &pricing)
	if err != nil {
		return nil, fmt.Errorf("decoding pricing: %w", err)
	}

	return pricing, nil
}

func (sps *StoragePricingStore) SetPricingSet(ctx context.Context, pricing *pricing.PricingSet) error {
	if pricing == nil {
		return errors.New("nil pricing")
	}

	data, err := json.Marshal(pricing)
	if err != nil {
		return fmt.Errorf("encoding pricing: %w", err)
	}

	err = sps.store.Write(sps.path, data)
	if err != nil {
		return fmt.Errorf("writing pricing: %w", err)
	}

	return nil
}
