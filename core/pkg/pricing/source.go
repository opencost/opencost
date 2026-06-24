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
	GetClusterPricing(ctx context.Context, props ClusterPricingProperties) (*ClusterPricing, error)
	NewClusterPricingReader(ctx context.Context) (reader.Reader[*ClusterPricing], error)
}

type NetworkPricingSource interface {
	GetNetworkPricing(ctx context.Context, props NetworkPricingProperties) (*NetworkPricing, error)
	NewNetworkPricingReader(ctx context.Context) (reader.Reader[*NetworkPricing], error)
}

type NodePricingSource interface {
	NewNodePricingReader(ctx context.Context) (reader.Reader[*NodePricing], error)
	GetNodePricing(ctx context.Context, props NodePricingProperties) (*NodePricing, error)
}

type PersistentVolumePricingSource interface {
	NewPersistentVolumePricingReader(ctx context.Context) (reader.Reader[*PersistentVolumePricing], error)
	GetPersistentVolumePricing(ctx context.Context, props PersistentVolumePricingProperties) (*PersistentVolumePricing, error)
}

type ServicePricingSource interface {
	GetServicePricing(ctx context.Context, props ServicePricingProperties) (*ServicePricing, error)
	NewServicePricingReader(ctx context.Context) (reader.Reader[*ServicePricing], error)
}
