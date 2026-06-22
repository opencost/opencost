package pricing

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/opencost/opencost/core/pkg/reader"
	"gopkg.in/yaml.v3"
)

type MockPricingRepository struct {
	NodePricing   []*NodePricing
	VolumePricing []*VolumePricing
}

func NewMockPricingRepository() (*MockPricingRepository, error) {
	repo := &MockPricingRepository{
		NodePricing:   []*NodePricing{},
		VolumePricing: []*VolumePricing{},
	}

	// Default
	defaultPricingSet, err := loadTestFile("default.yaml")
	if err != nil {
		return nil, fmt.Errorf("error loading test default pricing: %w", err)
	}
	repo.NodePricing = append(repo.NodePricing, defaultPricingSet.Nodes...)
	repo.VolumePricing = append(repo.VolumePricing, defaultPricingSet.Volumes...)

	// AWS
	awsPricingSet, err := loadTestFile("aws.yaml")
	if err != nil {
		return nil, fmt.Errorf("error loading test AWS pricing: %w", err)
	}
	repo.NodePricing = append(repo.NodePricing, awsPricingSet.Nodes...)
	repo.VolumePricing = append(repo.VolumePricing, awsPricingSet.Volumes...)

	// Azure
	azurePricingSet, err := loadTestFile("azure.yaml")
	if err != nil {
		return nil, fmt.Errorf("error loading test Azure pricing: %w", err)
	}
	repo.NodePricing = append(repo.NodePricing, azurePricingSet.Nodes...)
	repo.VolumePricing = append(repo.VolumePricing, azurePricingSet.Volumes...)

	// GCP
	gcpPricingSet, err := loadTestFile("gcp.yaml")
	if err != nil {
		return nil, fmt.Errorf("error loading test GCP pricing: %w", err)
	}
	repo.NodePricing = append(repo.NodePricing, gcpPricingSet.Nodes...)
	repo.VolumePricing = append(repo.VolumePricing, gcpPricingSet.Volumes...)

	return repo, nil
}

func (repo *MockPricingRepository) NewNodePricingReader(ctx context.Context) (reader.Reader[*NodePricing], error) {
	return reader.NewSliceReader(repo.NodePricing), nil
}

func (repo *MockPricingRepository) GetNodePricing(provider Provider, instanceType string, region string) (*NodePricing, error) {
	// Search through the mock data for a matching node pricing entry
	for _, np := range repo.NodePricing {
		if np.Properties.Provider == provider &&
			np.Properties.InstanceType == instanceType &&
			np.Properties.Region == region {
			return np, nil
		}
	}
	return nil, fmt.Errorf("node pricing not found for provider=%s, instanceType=%s, region=%s", provider, instanceType, region)
}

func (repo *MockPricingRepository) NewVolumePricingReader(ctx context.Context) (reader.Reader[*VolumePricing], error) {
	return reader.NewSliceReader(repo.VolumePricing), nil
}

func (repo *MockPricingRepository) GetVolumePricing(props VolumePricingProperties) (*VolumePricing, error) {
	// Search through the mock data for a matching volume pricing entry
	for _, vp := range repo.VolumePricing {
		if vp.Properties.Provider == props.Provider &&
			vp.Properties.Region == props.Region &&
			vp.Properties.VolumeType == props.VolumeType {
			return vp, nil
		}
	}
	return nil, fmt.Errorf("volume pricing not found for provider=%s, region=%s, volumeType=%s", props.Provider, props.Region, props.VolumeType)
}

//go:embed test/*
var pricingTestFS embed.FS

func loadTestFile(filename string) (*PricingSet, error) {
	path := filepath.Join("test", filename)
	bs, err := pricingTestFS.ReadFile(path)
	if err != nil {
		panic(fmt.Errorf("failed to read embedded pricing file: %w", err))
	}

	var set *PricingSet

	// Detect file format based on extension
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".json":
		err = json.Unmarshal(bs, &set)
		if err != nil {
			return nil, fmt.Errorf("failed to parse json: %w", err)
		}
	case ".yaml", ".yml":
		err = yaml.Unmarshal(bs, &set)
		if err != nil {
			return nil, fmt.Errorf("failed to parse yaml: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported file format: %s (expected .json, .yaml, or .yml)", ext)
	}

	return set, nil
}
