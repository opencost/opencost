package pricingmodel

// @bingen:generate:PricingModelSet
type PricingModelSet struct {
	Window      Window
	NodePricing map[NodeKey]NodePricing
}
