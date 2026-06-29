package usd

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/reader"
	"github.com/opencost/opencost/core/pkg/unit"
	"github.com/opencost/opencost/modules/pricing/public"
)

// PricingModule must satisfy the pricing.PricingModule interface
var _ pricing.PricingModule = (*PricingModule)(nil)

//go:embed pricing-data.json
var embeddedPricingData []byte

type PricingModule struct {
	store pricing.PricingStore
}

// NewPricingModule creates a new USD pricing module that reads from embedded pricing-data.json
func NewPricingModule() (*PricingModule, error) {
	ctx := context.Background()
	
	// Create an embedded storage that serves the embedded data directly without copying
	embeddedStorage := public.NewEmbeddedStorage(embeddedPricingData, "pricing-data.json")
	
	// Create a pricing store backed by the embedded storage
	store, err := pricing.NewStoragePricingStore(ctx, embeddedStorage, "pricing-data.json")
	if err != nil {
		return nil, fmt.Errorf("creating USD pricing store: %w", err)
	}

	return &PricingModule{
		store: store,
	}, nil
}

func (pm *PricingModule) GetNodePricing(ctx context.Context, props pricing.NodePricingProperties) (*pricing.NodePricing, error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, err
	}

	for _, np := range ps.NodePricing {
		if np.Properties.Provider == props.Provider &&
			np.Properties.InstanceType == props.InstanceType &&
			np.Properties.Region == props.Region {
			return np, nil
		}
	}

	return nil, fmt.Errorf("node pricing not found for provider=%s, instanceType=%s, region=%s",
		props.Provider, props.InstanceType, props.Region)
}

func (pm *PricingModule) NewNodePricingReader(ctx context.Context) (reader.Reader[*pricing.NodePricing], error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, err
	}
	return reader.NewSliceReader(ps.NodePricing), nil
}

func (pm *PricingModule) GetPersistentVolumePricing(ctx context.Context, props pricing.PersistentVolumePricingProperties) (*pricing.PersistentVolumePricing, error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, err
	}

	for _, pvp := range ps.PersistentVolumePricing {
		if pvp.Properties.Provider == props.Provider &&
			pvp.Properties.VolumeType == props.VolumeType &&
			pvp.Properties.Region == props.Region {
			return pvp, nil
		}
	}

	return nil, fmt.Errorf("volume pricing not found for provider=%s, volumeType=%s, region=%s",
		props.Provider, props.VolumeType, props.Region)
}

func (pm *PricingModule) NewPersistentVolumePricingReader(ctx context.Context) (reader.Reader[*pricing.PersistentVolumePricing], error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, err
	}
	return reader.NewSliceReader(ps.PersistentVolumePricing), nil
}

func (pm *PricingModule) GetClusterPricing(ctx context.Context, props pricing.ClusterPricingProperties) (*pricing.ClusterPricing, error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, err
	}

	for _, cp := range ps.ClusterPricing {
		if cp.Properties.Provider == props.Provider {
			return cp, nil
		}
	}

	return nil, fmt.Errorf("cluster pricing not found for provider=%s", props.Provider)
}

func (pm *PricingModule) NewClusterPricingReader(ctx context.Context) (reader.Reader[*pricing.ClusterPricing], error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, err
	}
	return reader.NewSliceReader(ps.ClusterPricing), nil
}

func (pm *PricingModule) GetNetworkPricing(ctx context.Context, props pricing.NetworkPricingProperties) (*pricing.NetworkPricing, error) {
	return nil, fmt.Errorf("network pricing not yet implemented")
}

func (pm *PricingModule) NewNetworkPricingReader(ctx context.Context) (reader.Reader[*pricing.NetworkPricing], error) {
	return nil, fmt.Errorf("network pricing not yet implemented")
}

func (pm *PricingModule) GetServicePricing(ctx context.Context, props pricing.ServicePricingProperties) (*pricing.ServicePricing, error) {
	return nil, fmt.Errorf("service pricing not yet implemented")
}

func (pm *PricingModule) NewServicePricingReader(ctx context.Context) (reader.Reader[*pricing.ServicePricing], error) {
	return nil, fmt.Errorf("service pricing not yet implemented")
}

func (pm *PricingModule) GetPricingSet(ctx context.Context) (*pricing.PricingSet, error) {
	return pm.store.GetPricingSet(ctx)
}

func (pm *PricingModule) SourceKind() string {
	return "public-usd"
}

func (pm *PricingModule) SourceName() string {
	return "usd"
}

func (pm *PricingModule) Checksum(ctx context.Context) (string, error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return "", err
	}
	return ps.Checksum()
}

// Currency returns USD
func Currency() unit.Currency {
	return unit.USD
}
