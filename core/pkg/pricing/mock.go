package pricing

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/opencost/opencost/core/pkg/reader"
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

	awsPricingSet, err := loadTestFile("aws.json")
	if err != nil {
		return nil, fmt.Errorf("error loading test AWS pricing: %w", err)
	}
	repo.NodePricing = append(repo.NodePricing, awsPricingSet.Nodes...)
	repo.VolumePricing = append(repo.VolumePricing, awsPricingSet.Volumes...)

	// azurePricingSet, err := loadTestFile("azure.json")
	// if err != nil {
	// 	return nil, fmt.Errorf("error loading test AWS pricing: %w", err)
	// }
	// repo.NodePricing = append(repo.NodePricing, azurePricingSet.Nodes...)
	// repo.VolumePricing = append(repo.VolumePricing, azurePricingSet.Volumes...)

	// gcpPricingSet, err := loadTestFile("gcp.json")
	// if err != nil {
	// 	return nil, fmt.Errorf("error loading test AWS pricing: %w", err)
	// }
	// repo.NodePricing = append(repo.NodePricing, gcpPricingSet.Nodes...)
	// repo.VolumePricing = append(repo.VolumePricing, gcpPricingSet.Volumes...)

	return repo, nil
}

func (repo *MockPricingRepository) NewNodePricingReader(ctx context.Context) (reader.Reader[*NodePricing], error) {
	return reader.NewSliceReader(repo.NodePricing), nil
}

func (repo *MockPricingRepository) NewVolumePricingReader(ctx context.Context) (reader.Reader[*VolumePricing], error) {
	return reader.NewSliceReader(repo.VolumePricing), nil
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

	err = json.Unmarshal(bs, &set)
	if err != nil {
		return nil, fmt.Errorf("failed to parse json: %w", err)
	}

	return set, nil
}
