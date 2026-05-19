package public

import (
	"context"

	"github.com/opencost/opencost/core/pkg/reader"
)

type PricingModule struct {
}

func (pm *PricingModule) NewNodePricingReader(ctx context.Context) (reader.Reader[*NodePricing], error) {

}
