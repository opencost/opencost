package pricingmodel

import (
	"time"
)

// @bingen:generate:PricingModelSet
type PricingModelSet struct {
	TimeStamp   time.Time
	Source      string
	NodePricing map[NodeKey]NodePricing
}

func NewPricingModelSet(timeStamp time.Time, source string) *PricingModelSet {
	return &PricingModelSet{
		TimeStamp:   timeStamp,
		Source:      source,
		NodePricing: make(map[NodeKey]NodePricing),
	}
}
