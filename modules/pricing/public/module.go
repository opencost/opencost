package public

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/reader"
	"github.com/opencost/opencost/core/pkg/unit"
)

type PricingModuleConfig struct {
	BaseDir  string
	Provider pricing.Provider
	Currency unit.Currency
}

type PricingModule struct {
	config     PricingModuleConfig
	Providers  *ProviderPricing `json:"provider" yaml:"provider"`
	pricingSet *pricing.PricingSet
}

func NewPricingModule(config PricingModuleConfig) (*PricingModule, error) {
	pm := &PricingModule{
		config:    config,
		Providers: &ProviderPricing{},
	}

	ctx := context.Background()

	pricingSet, err := pm.loadPricingSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load pricing: %w", err)
	}

	// Store the pricing set for reader access
	pm.pricingSet = pricingSet

	err = pm.indexPricingSet(ctx, pricingSet)
	if err != nil {
		return nil, fmt.Errorf("failed to load pricing: %w", err)
	}

	return pm, nil
}

type ProviderPricing map[pricing.Provider]*InstanceTypePricing

type InstanceTypePricing map[string]*RegionPricing

type RegionPricing map[string]*pricing.Prices

func (pm *PricingModule) loadPricingSet(_ context.Context) (*pricing.PricingSet, error) {
	providerLower := strings.ToLower(string(pm.config.Provider))

	// Load pricing from provider in directory
	filename := fmt.Sprintf("%s-%s.json", providerLower, strings.ToLower(string(pm.config.Currency)))
	path := filepath.Join(pm.config.BaseDir, providerLower, filename)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read pricing file %s: %w", path, err)
	}

	var pricingSet *pricing.PricingSet
	if err := json.Unmarshal(data, &pricingSet); err != nil {
		return nil, fmt.Errorf("failed to parse pricing file %s: %w", path, err)
	}

	return pricingSet, nil
}

func (pm *PricingModule) indexPricingSet(_ context.Context, pricingSet *pricing.PricingSet) error {
	providers := make(ProviderPricing)

	for _, node := range pricingSet.Nodes {
		provider := node.Properties.Provider
		instanceType := node.Properties.InstanceType
		region := node.Properties.Region

		// Instance type map
		if providers[provider] == nil {
			instanceMap := make(InstanceTypePricing)
			providers[provider] = &instanceMap
		}
		// Region map
		if (*providers[provider])[instanceType] == nil {
			regionMap := make(RegionPricing)
			(*providers[provider])[instanceType] = &regionMap
		}

		(*(*providers[provider])[instanceType])[region] = &node.Prices
	}

	pm.Providers = &providers
	log.Infof("Indexed %d node pricing records for provider %s (%s)",
		len(pricingSet.Nodes), pm.config.Provider, pm.config.Currency)

	return nil
}

// GetNodePricing provides fast lookup for node pricing by provider, instance type, and region
func (pm *PricingModule) GetNodePricing(provider pricing.Provider, instanceType string, region string) (*pricing.NodePricing, error) {
	if pm.Providers == nil {
		return nil, fmt.Errorf("pricing not loaded")
	}

	providerPricing := (*pm.Providers)[provider]
	if providerPricing == nil {
		return nil, fmt.Errorf("provider %s not found", provider)
	}

	instancePricing := (*providerPricing)[instanceType]
	if instancePricing == nil {
		return nil, fmt.Errorf("instance type %s not found for provider %s", instanceType, provider)
	}

	regionPricing := (*instancePricing)[region]
	if regionPricing == nil {
		return nil, fmt.Errorf("region %s not found for instance type %s in provider %s", region, instanceType, provider)
	}

	// Reconstruct NodePricing from Prices
	return &pricing.NodePricing{
		Properties: pricing.NodePricingProperties{
			Provider:     provider,
			InstanceType: instanceType,
			Region:       region,
		},
		Prices: *regionPricing,
	}, nil
}

// GetVolumePricing provides fast lookup for node pricing by provider, instance type, and region
func (pm *PricingModule) GetVolumePricing(provider pricing.Provider, volumeType string, region string) (*pricing.VolumePricing, error) {
	if pm.Providers == nil {
		return nil, fmt.Errorf("pricing not loaded")
	}

	providerPricing := (*pm.Providers)[provider]
	if providerPricing == nil {
		return nil, fmt.Errorf("provider %s not found", provider)
	}

	instancePricing := (*providerPricing)[volumeType]
	if instancePricing == nil {
		return nil, fmt.Errorf("volume type %s not found for provider %s", volumeType, provider)
	}

	regionPricing := (*instancePricing)[region]
	if regionPricing == nil {
		return nil, fmt.Errorf("region %s not found for volume type %s in provider %s", region, volumeType, provider)
	}

	// Reconstruct NodePricing from Prices
	return &pricing.VolumePricing{
		Properties: pricing.VolumePricingProperties{
			Provider:   provider,
			VolumeType: pricing.VolumeType(volumeType),
			Region:     region,
		},
		Prices: *regionPricing,
	}, nil
}

func (pm *PricingModule) NewNodePricingReader(ctx context.Context) (reader.Reader[*pricing.NodePricing], error) {
	return reader.NewSliceReader(pm.pricingSet.Nodes), nil
}

func (pm *PricingModule) NewVolumePricingReader(ctx context.Context) (reader.Reader[*pricing.VolumePricing], error) {
	return reader.NewSliceReader(pm.pricingSet.Volumes), nil
}
