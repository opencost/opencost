package pricingmodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/model/shared"
)

// TODO: assess whether we need any of this, or whether we can adapt all existing
// references to it to use core/pkg/pricing concepts, instead.
//
// See:
//   pkg/cloud/aws/pricinglistpricingsource.go
//   pkg/cloud/azure/retailpricingsource.go
//   pkg/cloud/gcp/billingpricingsource.go

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

type PricingSourceType string

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

type NodePricingType string

const (
	NodePricingTypeTotal   NodePricingType = "Total"
	NodePricingTypeCPUCore NodePricingType = "CPUCore"
	NodePricingTypeRamGB   NodePricingType = "RamGB"
	NodePricingTypeDevice  NodePricingType = "Device"
)

type NodeKey struct {
	Provider    shared.Provider
	PricingType NodePricingType
	UsageType   shared.UsageType
	Region      string
	NodeType    string
	Family      string
	DeviceType  string
}

type NodePricing struct {
	HourlyRate float64
}
