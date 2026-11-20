package pricing

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/opencost/opencost/core/pkg/reader"
	"gopkg.in/yaml.v3"
)

// MockPricingModule must satisfy the PricingModule interface
var _ PricingModule = (*MockPricingModule)(nil)

type MockPricingModule struct {
	ClusterPricing          []*ClusterPricing
	NetworkPricing          []*NetworkPricing
	NodePricing             []*NodePricing
	PersistentVolumePricing []*PersistentVolumePricing
	ServicePricing          []*ServicePricing
}

func NewMockPricingModule() (*MockPricingModule, error) {
	mpm := &MockPricingModule{
		ClusterPricing:          []*ClusterPricing{},
		NetworkPricing:          []*NetworkPricing{},
		NodePricing:             []*NodePricing{},
		PersistentVolumePricing: []*PersistentVolumePricing{},
		ServicePricing:          []*ServicePricing{},
	}

	// Default
	err := mpm.loadTestFile("default.yaml")
	if err != nil {
		return nil, fmt.Errorf("error loading test default pricing: %w", err)
	}

	// AWS
	err = mpm.loadTestFile("aws.yaml")
	if err != nil {
		return nil, fmt.Errorf("error loading test AWS pricing: %w", err)
	}

	// Azure
	err = mpm.loadTestFile("azure.yaml")
	if err != nil {
		return nil, fmt.Errorf("error loading test Azure pricing: %w", err)
	}

	// GCP
	err = mpm.loadTestFile("gcp.yaml")
	if err != nil {
		return nil, fmt.Errorf("error loading test GCP pricing: %w", err)
	}

	return mpm, nil
}

func (mpm *MockPricingModule) NewClusterPricingReader(ctx context.Context) (reader.Reader[*ClusterPricing], error) {
	return reader.NewSliceReader(mpm.ClusterPricing), nil
}

func (mpm *MockPricingModule) NewNetworkPricingReader(ctx context.Context) (reader.Reader[*NetworkPricing], error) {
	return reader.NewSliceReader(mpm.NetworkPricing), nil
}

func (mpm *MockPricingModule) NewNodePricingReader(ctx context.Context) (reader.Reader[*NodePricing], error) {
	return reader.NewSliceReader(mpm.NodePricing), nil
}

func (mpm *MockPricingModule) NewPersistentVolumePricingReader(ctx context.Context) (reader.Reader[*PersistentVolumePricing], error) {
	return reader.NewSliceReader(mpm.PersistentVolumePricing), nil
}

func (mpm *MockPricingModule) NewServicePricingReader(ctx context.Context) (reader.Reader[*ServicePricing], error) {
	return reader.NewSliceReader(mpm.ServicePricing), nil
}

func (mpm *MockPricingModule) GetPricingSet(ctx context.Context) (*PricingSet, error) {
	ps := &PricingSet{
		ClusterPricing:          mpm.ClusterPricing,
		NetworkPricing:          mpm.NetworkPricing,
		NodePricing:             mpm.NodePricing,
		PersistentVolumePricing: mpm.PersistentVolumePricing,
		ServicePricing:          mpm.ServicePricing,
	}

	return ps, nil
}

func (mpm *MockPricingModule) SourceKind() string {
	return "test"
}

func (mpm *MockPricingModule) SourceName() string {
	return "mock"
}

func (mpm *MockPricingModule) Checksum(ctx context.Context) (string, error) {
	ps, err := mpm.GetPricingSet(ctx)
	if err != nil {
		return "", fmt.Errorf("getting pricing set: %w", err)
	}

	return ps.Checksum()
}

//go:embed test/*
var pricingTestFS embed.FS

func (mpm *MockPricingModule) loadTestFile(filename string) error {
	path := filepath.Join("test", filename)
	bs, err := pricingTestFS.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read embedded pricing file: %w", err)
	}

	var set *PricingSet

	// Detect file format based on extension
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".json":
		err = json.Unmarshal(bs, &set)
		if err != nil {
			return fmt.Errorf("failed to parse json: %w", err)
		}
	case ".yaml", ".yml":
		err = yaml.Unmarshal(bs, &set)
		if err != nil {
			return fmt.Errorf("failed to parse yaml: %w", err)
		}
	default:
		return fmt.Errorf("unsupported file format: %s (expected .json, .yaml, or .yml)", ext)
	}

	if set == nil {
		return errors.New("nil set")
	}

	mpm.ClusterPricing = append(mpm.ClusterPricing, set.ClusterPricing...)
	mpm.NetworkPricing = append(mpm.NetworkPricing, set.NetworkPricing...)
	mpm.NodePricing = append(mpm.NodePricing, set.NodePricing...)
	mpm.PersistentVolumePricing = append(mpm.PersistentVolumePricing, set.PersistentVolumePricing...)
	mpm.ServicePricing = append(mpm.ServicePricing, set.ServicePricing...)

	return nil
}
