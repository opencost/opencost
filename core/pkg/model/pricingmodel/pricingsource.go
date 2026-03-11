package pricingmodel

type PricingSource interface {
	// PricingSourceType returns the instance type of the PricingSource, each implementation of this interface should
	// provide a unique type that all instances should return
	PricingSourceType() PricingSourceType
	// PricingSourceKey returns the unique key of the PricingSource instance. In PricingSource implementation that may
	// have multiple instances running side by side this key (derived from some configuration will) will Identify each
	// instance. In PricingSource implementations where there will only be a single instance (ex Provider List Pricing)
	// The PricingSourceKey should match the PricingSourceType
	PricingSourceKey() string
	GetPricing() (*PricingModelSet, error)
}
