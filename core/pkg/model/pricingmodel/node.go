package pricingmodel

import (
	"github.com/opencost/opencost/core/pkg/model/shared"
)

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
	Region      string
	NodeType    string
	UsageType   shared.UsageType
	Family      string
	Accelerator string
	Type        NodePricingType
}

// @bingen:generate:NodePricing
type NodePricing struct {
	HourlyRate float64
}
