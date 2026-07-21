package cny

import (
	"context"
	"embed"
	"fmt"
	"io"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/reader"
	"github.com/opencost/opencost/core/pkg/unit"
)

// PricingModule must satisfy the pricing.PricingModule interface
var _ pricing.PricingModule = (*PricingModule)(nil)

//go:embed *.jsonl
var embeddedFS embed.FS

type PricingModule struct{}

// NewPricingModule creates a new CNY pricing module backed by embedded JSONL files.
func NewPricingModule() (*PricingModule, error) {
	return &PricingModule{}, nil
}

func (pm *PricingModule) newNodeReader() (reader.Reader[*pricing.NodePricing], error) {
	f, err := embeddedFS.Open("nodes.jsonl")
	if err != nil {
		return nil, fmt.Errorf("opening embedded nodes.jsonl: %w", err)
	}
	return reader.NewJSONLinesReader[*pricing.NodePricing](f), nil
}

func (pm *PricingModule) newPVReader() (reader.Reader[*pricing.PersistentVolumePricing], error) {
	f, err := embeddedFS.Open("persistentvolumes.jsonl")
	if err != nil {
		return nil, fmt.Errorf("opening embedded persistentvolumes.jsonl: %w", err)
	}
	return reader.NewJSONLinesReader[*pricing.PersistentVolumePricing](f), nil
}

func (pm *PricingModule) GetNodePricing(ctx context.Context, props pricing.NodePricingProperties) (*pricing.NodePricing, error) {
	r, err := pm.newNodeReader()
	if err != nil {
		return nil, err
	}
	defer r.Close()

	dst := make([]*pricing.NodePricing, 64)
	for {
		n, err := r.Read(ctx, dst)
		for _, np := range dst[:n] {
			if np.Properties.Provider == props.Provider &&
				np.Properties.InstanceType == props.InstanceType &&
				np.Properties.Region == props.Region {
				return np, nil
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	return nil, fmt.Errorf("node pricing not found for provider=%s, instanceType=%s, region=%s",
		props.Provider, props.InstanceType, props.Region)
}

func (pm *PricingModule) NewNodePricingReader(ctx context.Context) (reader.Reader[*pricing.NodePricing], error) {
	return pm.newNodeReader()
}

func (pm *PricingModule) GetPersistentVolumePricing(ctx context.Context, props pricing.PersistentVolumePricingProperties) (*pricing.PersistentVolumePricing, error) {
	r, err := pm.newPVReader()
	if err != nil {
		return nil, err
	}
	defer r.Close()

	dst := make([]*pricing.PersistentVolumePricing, 64)
	for {
		n, err := r.Read(ctx, dst)
		for _, pvp := range dst[:n] {
			if pvp.Properties.Provider == props.Provider &&
				pvp.Properties.VolumeType == props.VolumeType &&
				pvp.Properties.Region == props.Region {
				return pvp, nil
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	return nil, fmt.Errorf("volume pricing not found for provider=%s, volumeType=%s, region=%s",
		props.Provider, props.VolumeType, props.Region)
}

func (pm *PricingModule) NewPersistentVolumePricingReader(ctx context.Context) (reader.Reader[*pricing.PersistentVolumePricing], error) {
	return pm.newPVReader()
}

func (pm *PricingModule) GetClusterPricing(ctx context.Context, props pricing.ClusterPricingProperties) (*pricing.ClusterPricing, error) {
	return nil, fmt.Errorf("cluster pricing not yet implemented")
}

func (pm *PricingModule) NewClusterPricingReader(ctx context.Context) (reader.Reader[*pricing.ClusterPricing], error) {
	return nil, fmt.Errorf("cluster pricing not yet implemented")
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
	ps := &pricing.PricingSet{}

	nodeReader, err := pm.newNodeReader()
	if err != nil {
		return nil, err
	}
	defer nodeReader.Close()
	dst := make([]*pricing.NodePricing, 64)
	for {
		n, err := nodeReader.Read(ctx, dst)
		ps.NodePricing = append(ps.NodePricing, dst[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	pvReader, err := pm.newPVReader()
	if err != nil {
		return nil, err
	}
	defer pvReader.Close()
	pvDst := make([]*pricing.PersistentVolumePricing, 64)
	for {
		n, err := pvReader.Read(ctx, pvDst)
		ps.PersistentVolumePricing = append(ps.PersistentVolumePricing, pvDst[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	return ps, nil
}

func (pm *PricingModule) SourceKind() string {
	return "public"
}

func (pm *PricingModule) SourceName() string {
	return "public-cny"
}

func (pm *PricingModule) Checksum(ctx context.Context) (string, error) {
	ps, err := pm.GetPricingSet(ctx)
	if err != nil {
		return "", err
	}
	return ps.Checksum()
}

// Currency returns CNY
func Currency() unit.Currency {
	return unit.CNY
}
