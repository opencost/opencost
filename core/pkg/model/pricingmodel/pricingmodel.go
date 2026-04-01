package pricingmodel

import (
	"time"
)

// @bingen:generate:PricingModelSet
type PricingModelSet struct {
	Window      Window
	NodePricing map[NodeKey]NodePricing
}

func NewPricingModelSet(start, end time.Time) *PricingModelSet {
	return &PricingModelSet{
		Window: Window{
			Start: start,
			End:   end,
		},
		NodePricing: make(map[NodeKey]NodePricing),
	}
}
