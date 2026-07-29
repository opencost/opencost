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

func (mpm *MockPricingModule) GetClusterPricing(ctx context.Context, props ClusterPricingProperties) (*ClusterPricing, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Search through the mock data for a matching cluster pricing entry
	for _, cp := range mpm.ClusterPricing {
		if cp.Properties.Provider == props.Provider {
			return cp, nil
		}
	}
	return nil, fmt.Errorf("cluster pricing not found for provider=%s", props.Provider)
}

func (mpm *MockPricingModule) NewClusterPricingReader(ctx context.Context) (reader.Reader[*ClusterPricing], error) {
	return reader.NewSliceReader(mpm.ClusterPricing), nil
}

func (mpm *MockPricingModule) GetNetworkPricing(ctx context.Context, props NetworkPricingProperties) (*NetworkPricing, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Search through the mock data for a matching network pricing entry
	for _, np := range mpm.NetworkPricing {
		if np.Properties.Provider == props.Provider {
			return np, nil
		}
	}

	return nil, fmt.Errorf("network pricing not found for provider=%s", props.Provider)
}

func (mpm *MockPricingModule) NewNetworkPricingReader(ctx context.Context) (reader.Reader[*NetworkPricing], error) {
	return reader.NewSliceReader(mpm.NetworkPricing), nil
}

func (mpm *MockPricingModule) GetNodePricing(ctx context.Context, props NodePricingProperties) (*NodePricing, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Search through the mock data for a matching node pricing entry
	for _, np := range mpm.NodePricing {
		if np.Properties.Provider == props.Provider &&
			np.Properties.Region == props.Region &&
			np.Properties.InstanceType == props.InstanceType &&
			np.Properties.Provisioning == props.Provisioning {
			return np, nil
		}
	}
	return nil, fmt.Errorf("node pricing not found for provider=%s, region=%s, instanceType=%s, provisioning=%s",
		props.Provider, props.Region, props.InstanceType, props.Provisioning)
}

func (mpm *MockPricingModule) NewNodePricingReader(ctx context.Context) (reader.Reader[*NodePricing], error) {
	return reader.NewSliceReader(mpm.NodePricing), nil
}

func (mpm *MockPricingModule) GetPersistentVolumePricing(ctx context.Context, props PersistentVolumePricingProperties) (*PersistentVolumePricing, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Search through the mock data for a matching volume pricing entry
	for _, vp := range mpm.PersistentVolumePricing {
		if vp.Properties.Provider == props.Provider &&
			vp.Properties.Region == props.Region &&
			vp.Properties.VolumeType == props.VolumeType {
			return vp, nil
		}
	}
	return nil, fmt.Errorf("volume pricing not found for provider=%s, region=%s, volumeType=%s", props.Provider, props.Region, props.VolumeType)
}

func (mpm *MockPricingModule) NewPersistentVolumePricingReader(ctx context.Context) (reader.Reader[*PersistentVolumePricing], error) {
	return reader.NewSliceReader(mpm.PersistentVolumePricing), nil
}

func (mpm *MockPricingModule) GetServicePricing(ctx context.Context, props ServicePricingProperties) (*ServicePricing, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Search through the mock data for a matching service pricing entry
	for _, sp := range mpm.ServicePricing {
		if sp.Properties.Provider == props.Provider &&
			sp.Properties.Region == props.Region {
			return sp, nil
		}
	}
	return nil, fmt.Errorf("service pricing not found for provider=%s, region=%s", props.Provider, props.Region)
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
