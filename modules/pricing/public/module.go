package public

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/reader"
	"github.com/opencost/opencost/core/pkg/unit"
)

type PricingModuleConfig struct {
	ProviderConfigs []ProviderConfig
	RefreshInterval time.Duration
}

type ProviderConfig struct {
	Provider pricing.Provider
	Currencies []unit.Currency
}

type PricingModule struct {
	config     PricingModuleConfig
	Providers  *ProviderPricing `json:"provider" yaml:"provider"`
	pricingSet *pricing.PricingSet
	mu         sync.RWMutex
	stopCh     chan struct{}
	doneCh     chan struct{}
}


func NewPricingModule(config PricingModuleConfig) (*PricingModule, error) {
	pm := &PricingModule{
		config:    config,
		Providers: &ProviderPricing{},
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}

	ctx := context.Background()

	// Generate pricing data directly from the provider API
	pricingSet, err := GeneratePricingSet(config.ProviderConfigs)
	if err != nil {
		return nil, fmt.Errorf("failed to generate pricing: %w", err)
	}

	// Store the pricing set for reader access
	pm.pricingSet = pricingSet

	err = pm.indexPricingSet(ctx, pricingSet)
	if err != nil {
		return nil, fmt.Errorf("failed to index pricing: %w", err)
	}

	// Start background refresh if configured
	if config.RefreshInterval > 0 {
		go pm.backgroundRefresh()
		log.Infof("Started background pricing refresh with interval: %v", config.RefreshInterval)
	}

	return pm, nil
}

type ProviderPricing map[pricing.Provider]*InstanceTypePricing

type InstanceTypePricing map[string]*RegionPricing

type RegionPricing map[string]*pricing.Prices


func (pm *PricingModule) indexPricingSet(_ context.Context, pricingSet *pricing.PricingSet) error {
	providers := make(ProviderPricing)

	// Index nodes
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

	// Index volumes
	for _, volume := range pricingSet.Volumes {
		provider := volume.Properties.Provider
		volumeType := string(volume.Properties.VolumeType)
		region := volume.Properties.Region

		// Instance type map
		if providers[provider] == nil {
			instanceMap := make(InstanceTypePricing)
			providers[provider] = &instanceMap
		}
		// Region map
		if (*providers[provider])[volumeType] == nil {
			regionMap := make(RegionPricing)
			(*providers[provider])[volumeType] = &regionMap
		}

		(*(*providers[provider])[volumeType])[region] = &volume.Prices
	}

	pm.Providers = &providers
	log.Infof("Indexed %d node pricing records and %d volume pricing records",
		len(pricingSet.Nodes), len(pricingSet.Volumes))

	return nil
}

// GetNodePricing provides fast lookup for node pricing by provider, instance type, and region
func (pm *PricingModule) GetNodePricing(provider pricing.Provider, instanceType string, region string) (*pricing.NodePricing, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

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
	pm.mu.RLock()
	defer pm.mu.RUnlock()

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
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return reader.NewSliceReader(pm.pricingSet.Nodes), nil
}

func (pm *PricingModule) NewVolumePricingReader(ctx context.Context) (reader.Reader[*pricing.VolumePricing], error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return reader.NewSliceReader(pm.pricingSet.Volumes), nil
}

// GetPricingSet returns the current in-memory pricing set
func (pm *PricingModule) GetPricingSet() *pricing.PricingSet {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.pricingSet
}

// ComparePricingSet compares the current in-memory pricing set with a new one
// Returns true if they are identical, false if different
func (pm *PricingModule) ComparePricingSet(newPricingSet *pricing.PricingSet) (bool, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.pricingSet == nil {
		return false, fmt.Errorf("current pricing set is nil")
	}
	if newPricingSet == nil {
		return false, fmt.Errorf("new pricing set is nil")
	}

	// Compare by serializing both to JSON and computing checksums
	currentJSON, err := pm.serializePricingSet(pm.pricingSet)
	if err != nil {
		return false, fmt.Errorf("failed to serialize current pricing set: %w", err)
	}

	newJSON, err := pm.serializePricingSet(newPricingSet)
	if err != nil {
		return false, fmt.Errorf("failed to serialize new pricing set: %w", err)
	}

	return string(currentJSON) == string(newJSON), nil
}

// UpdatePricingSet replaces the current pricing set with a new one and re-indexes it
func (pm *PricingModule) UpdatePricingSet(ctx context.Context, newPricingSet *pricing.PricingSet) error {
	if newPricingSet == nil {
		return fmt.Errorf("new pricing set is nil")
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Store the new pricing set
	pm.pricingSet = newPricingSet

	// Re-index the pricing data
	err := pm.indexPricingSet(ctx, newPricingSet)
	if err != nil {
		return fmt.Errorf("failed to index new pricing set: %w", err)
	}

	log.Infof("Updated pricing set: %d node pricing records and %d volume pricing records",
		len(newPricingSet.Nodes), len(newPricingSet.Volumes))

	return nil
}

// serializePricingSet converts a pricing set to JSON bytes for comparison
func (pm *PricingModule) serializePricingSet(ps *pricing.PricingSet) ([]byte, error) {
	return json.Marshal(ps)
}

// backgroundRefresh periodically fetches new pricing data and updates the module
func (pm *PricingModule) backgroundRefresh() {
	defer close(pm.doneCh)

	ticker := time.NewTicker(pm.config.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Infof("Starting scheduled pricing refresh")

			// Fetch new pricing data
			newPricingSet, err := GeneratePricingSet(pm.config.ProviderConfigs)
			if err != nil {
				log.Errorf("Failed to refresh pricing data: %v", err)
				continue
			}

			// Compare with existing data
			isIdentical, err := pm.ComparePricingSet(newPricingSet)
			if err != nil {
				log.Errorf("Failed to compare pricing data: %v", err)
				continue
			}

			if isIdentical {
				log.Infof("Pricing data unchanged, skipping update")
				continue
			}

			// Update with new data
			ctx := context.Background()
			if err := pm.UpdatePricingSet(ctx, newPricingSet); err != nil {
				log.Errorf("Failed to update pricing data: %v", err)
				continue
			}

			log.Infof("Successfully refreshed pricing data")

		case <-pm.stopCh:
			log.Infof("Stopping background pricing refresh")
			return
		}
	}
}

// Stop gracefully stops the background refresh goroutine
func (pm *PricingModule) Stop() {
	if pm.config.RefreshInterval > 0 {
		close(pm.stopCh)
		<-pm.doneCh
		log.Infof("Background pricing refresh stopped")
	}
}
