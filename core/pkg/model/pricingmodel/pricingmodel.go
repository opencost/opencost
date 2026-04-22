package pricingmodel

import (
	"time"
)

// @bingen:generate:PricingSourceType
type PricingSourceType string

// @bingen:generate[stringtable,streamable]:PricingModelSet
type PricingModelSet struct {
	TimeStamp   time.Time
	SourceType  PricingSourceType
	SourceKey   string
	NodePricing map[NodeKey]NodePricing
}

// NewPricingModelSet creates a PricingModelSet with SourceKey initialized to sourceType.
func NewPricingModelSet(timeStamp time.Time, sourceType PricingSourceType, sourceKey string) *PricingModelSet {
	return &PricingModelSet{
		TimeStamp:   timeStamp,
		SourceType:  sourceType,
		SourceKey:   sourceKey,
		NodePricing: make(map[NodeKey]NodePricing),
	}
}
