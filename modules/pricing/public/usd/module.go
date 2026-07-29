package usd

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

// NewPricingModule creates a new USD pricing module backed by embedded JSONL files.
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

func (pm *PricingModule) NewNodePricingReader(ctx context.Context) (reader.Reader[*pricing.NodePricing], error) {
	return pm.newNodeReader()
}

func (pm *PricingModule) NewPersistentVolumePricingReader(ctx context.Context) (reader.Reader[*pricing.PersistentVolumePricing], error) {
	return pm.newPVReader()
}

func (pm *PricingModule) NewClusterPricingReader(ctx context.Context) (reader.Reader[*pricing.ClusterPricing], error) {
	return nil, fmt.Errorf("cluster pricing not yet implemented")
}

func (pm *PricingModule) NewNetworkPricingReader(ctx context.Context) (reader.Reader[*pricing.NetworkPricing], error) {
	return nil, fmt.Errorf("network pricing not yet implemented")
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
	return "public-usd"
}

func (pm *PricingModule) Checksum(ctx context.Context) (string, error) {
	ps, err := pm.GetPricingSet(ctx)
	if err != nil {
		return "", err
	}
	return ps.Checksum()
}

// Currency returns USD
func Currency() unit.Currency {
	return unit.USD
}
