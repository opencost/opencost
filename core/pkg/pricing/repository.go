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
	// GetNodePricing() (*NodePricing, error)
	// ListNodePricing(req NodePricingRequest) ([]*NodePricing, error)
	NewNodePricingReader(ctx context.Context) (reader.Reader[*NodePricing], error)
}

type VolumePricingRepository interface {
	// GetVolumePricing() (*VolumePricing, error)
	// ListVolumePricing(req VolumePricingRequest) ([]*VolumePricing, error)
	NewVolumePricingReader(ctx context.Context) (reader.Reader[*VolumePricing], error)
}
