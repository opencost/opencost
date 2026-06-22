package public

import "github.com/opencost/opencost/core/pkg/pricing"

type PricingSource interface {
	GetPricing() (*pricing.PricingSet, error)
}
