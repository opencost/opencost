package pricingmodel

import (
	"github.com/opencost/opencost/core/pkg/model/shared"
)

// @bingen:generate:NodePricingType
type NodePricingType string

const (
	NodePricingTypeTotal   NodePricingType = "Total"
	NodePricingTypeCPUCore NodePricingType = "CPUCore"
	NodePricingTypeRamGB   NodePricingType = "RamGB"
	NodePricingTypeDevice  NodePricingType = "Device"
)

// @bingen:generate:NodeKey
type NodeKey struct {
	Provider    shared.Provider
	PricingType NodePricingType
	UsageType   shared.UsageType
	Region      string
	NodeType    string
	Family      string
	DeviceType  string
}

// @bingen:generate:NodePricing
type NodePricing struct {
	HourlyRate float64
}
