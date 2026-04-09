package pricingmodel

import (
	"github.com/opencost/opencost/core/pkg/model/shared"
)

// @bingen:generate:NodeKey
type NodeKey struct {
	Provider  shared.Provider
	Region    string
	NodeType  string
	UsageType shared.UsageType
}

// @bingen:generate:NodePricing
type NodePricing struct {
	TotalHourlyRate float64
}
