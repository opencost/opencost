package pricing

import (
	"context"

	"github.com/opencost/opencost/core/pkg/reader"
)

type PricingSource interface {
	ClusterPricingSource
	NetworkPricingSource
	NodePricingSource
	PersistentVolumePricingSource
	ServicePricingSource

	GetPricingSet(context.Context) (*PricingSet, error)
	SourceKind() string
	SourceName() string
}

type ClusterPricingSource interface {
	NewClusterPricingReader(ctx context.Context) (reader.Reader[*ClusterPricing], error)
}

type NetworkPricingSource interface {
	NewNetworkPricingReader(ctx context.Context) (reader.Reader[*NetworkPricing], error)
}

type NodePricingSource interface {
	NewNodePricingReader(ctx context.Context) (reader.Reader[*NodePricing], error)
}

type PersistentVolumePricingSource interface {
	NewPersistentVolumePricingReader(ctx context.Context) (reader.Reader[*PersistentVolumePricing], error)
}

type ServicePricingSource interface {
	NewServicePricingReader(ctx context.Context) (reader.Reader[*ServicePricing], error)
}
