package public

import (
	"context"
	"errors"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/reader"
)

type PricingModule struct {
}

func (pm *PricingModule) NewNodePricingReader(ctx context.Context) (reader.Reader[*pricing.NodePricing], error) {
	return nil, errors.New("not implemented")
}

func (pm *PricingModule) NewVolumePricingReader(ctx context.Context) (reader.Reader[*pricing.VolumePricing], error) {
	return nil, errors.New("not implemented")
}
