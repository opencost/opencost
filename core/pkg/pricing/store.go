package pricing

import (
	"context"
)

type PricingStore interface {
	GetPricingSet(ctx context.Context) (*PricingSet, error)
	SetPricingSet(ctx context.Context, pricing *PricingSet) error
}
