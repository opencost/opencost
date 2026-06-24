package basic

import (
	"context"
	"fmt"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/storage"
)

const basicPricingFilePath = "basic-pricing.json"

func DefaultBasicPricingStore(ctx context.Context) (pricing.PricingStore, error) {
	storage := storage.NewFileStorage(env.GetConfigPath())

	store, err := pricing.NewStoragePricingStore(ctx, storage, basicPricingFilePath)
	if err != nil {
		return nil, fmt.Errorf("creating store at %q: %w", env.GetPathFromConfig(basicPricingFilePath), err)
	}

	return store, nil
}
