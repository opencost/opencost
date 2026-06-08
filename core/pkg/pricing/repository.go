package pricing

import (
	"context"

	"github.com/opencost/opencost/core/pkg/reader"
)

type PricingRepository interface {
	NodePricingRepository
	VolumePricingRepository
}

// TODO: add the following function for Opencost pricing
// GetNodePricing(NodePricingProperties) (*NodePricing, error)
type NodePricingRepository interface {
	NewNodePricingReader(ctx context.Context) (reader.Reader[*NodePricing], error)
}

// TODO: add the following function for Opencost pricing
// GetVolumePricing(VolumePricingProperties) (*VolumePricing, error)
type VolumePricingRepository interface {
	NewVolumePricingReader(ctx context.Context) (reader.Reader[*VolumePricing], error)
}
