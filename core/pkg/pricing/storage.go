package pricing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/opencost/opencost/core/pkg/storage"
)

type StoragePricingStore struct {
	store storage.Storage
	path  string
}

func NewStoragePricingStore(ctx context.Context, store storage.Storage, path string) (*StoragePricingStore, error) {
	if store == nil {
		return nil, errors.New("nil storage")
	}

	if path == "" {
		return nil, errors.New("empty path")
	}

	sps := &StoragePricingStore{
		store: store,
		path:  path,
	}

	exists, err := store.Exists(path)
	if err != nil {
		return nil, fmt.Errorf("checking pricing path %q: %w", path, err)
	}

	if !exists {
		if err := sps.SetPricingSet(ctx, &PricingSet{}); err != nil {
			return nil, fmt.Errorf("initializing empty pricing set at %q: %w", path, err)
		}
	}

	return sps, nil
}

func (sps *StoragePricingStore) GetPricingSet(ctx context.Context) (*PricingSet, error) {
	data, err := sps.store.Read(sps.path)
	if err != nil {
		return nil, fmt.Errorf("reading path '%s': %w", sps.path, err)
	}

	var pricing PricingSet
	err = json.Unmarshal(data, &pricing)
	if err != nil {
		return nil, fmt.Errorf("decoding pricing: %w", err)
	}

	return &pricing, nil
}

func (sps *StoragePricingStore) SetPricingSet(ctx context.Context, pricing *PricingSet) error {
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
