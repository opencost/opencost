package pricing

import (
	"context"

	"github.com/opencost/opencost/core/pkg/model/shared"
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

// TODO: add the following function for Opencost pricing
// GetClusterPricing(ClusterPricingProperties) (*ClusterPricing, error)
type ClusterPricingSource interface {
	NewClusterPricingReader(ctx context.Context) (reader.Reader[*ClusterPricing], error)
}

// TODO: add the following function for Opencost pricing
// GetNetworkPricing(NetworkPricingProperties) (*NetworkPricing, error)
type NetworkPricingSource interface {
	NewNetworkPricingReader(ctx context.Context) (reader.Reader[*NetworkPricing], error)
}

type NodePricingSource interface {
	NewNodePricingReader(ctx context.Context) (reader.Reader[*NodePricing], error)
	GetNodePricing(provider shared.Provider, instanceType string, region string) (*NodePricing, error)
}

type PersistentVolumePricingSource interface {
	NewPersistentVolumePricingReader(ctx context.Context) (reader.Reader[*PersistentVolumePricing], error)
	GetPersistentVolumePricing(PersistentVolumePricingProperties) (*PersistentVolumePricing, error)
}

// TODO: add the following function for Opencost pricing
// GetServicePricing(ServicePricingProperties) (*ServicePricing, error)
type ServicePricingSource interface {
	NewServicePricingReader(ctx context.Context) (reader.Reader[*ServicePricing], error)
}
