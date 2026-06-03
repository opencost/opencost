package basic

import (
	"context"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

type PricingStore interface {
	GetCurrencies(ctx context.Context) []unit.Currency
	GetPricingSet(ctx context.Context) (*pricing.PricingSet, error)
	SetPricingSet(ctx context.Context, pricing *pricing.PricingSet) error
}
