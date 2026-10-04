package pricing

import "context"

type PricingModule interface {
	PricingSource
	Checksum(context.Context) (string, error)
}
