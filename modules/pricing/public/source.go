package public

import "github.com/opencost/opencost/core/pkg/pricing"

// TODO

type PricingSource interface {
	GetPricing() (*pricing.PricingSet, error)
}
