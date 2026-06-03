package pricing

import (
	"context"

	"github.com/opencost/opencost/core/pkg/reader"
)

type PricingRepository interface {
	NodePricingRepository
	VolumePricingRepository
}

type NodePricingRepository interface {
	NewNodePricingReader(ctx context.Context) (reader.Reader[*NodePricing], error)
	GetNodePricing(provider Provider, instanceType string, region string) (*NodePricing, error)
}

type VolumePricingRepository interface {
	NewVolumePricingReader(ctx context.Context) (reader.Reader[*VolumePricing], error)
	GetVolumePricing(VolumePricingProperties) (*VolumePricing, error)
}
