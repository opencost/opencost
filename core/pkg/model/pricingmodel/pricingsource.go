package pricingmodel

type PricingSource interface {
	PricingSourceKey() string
	GetPricing() (*PricingModelSet, error)
}
