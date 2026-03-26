package pricingmodel

type PricingSource interface {
	GetPricing() (*PricingModelSet, error)
}
