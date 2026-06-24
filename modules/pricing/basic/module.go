package basic

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/reader"
	"github.com/opencost/opencost/core/pkg/unit"
)

// PricingModule must satisfy the pricing.PricingModule interface
var _ pricing.PricingModule = (*PricingModule)(nil)

type PricingModule struct {
	mu    sync.RWMutex
	store pricing.PricingStore
}

func NewBasicPricingModule(store pricing.PricingStore) (*PricingModule, error) {
	pricingSet, err := store.GetPricingSet(context.Background())
	if err != nil {
		return nil, fmt.Errorf("checking pricing store: %w", err)
	}

	if pricingSet.IsEmpty() {
		// Populate store with a default pricing set.
		err := store.SetPricingSet(context.Background(), GetDefaultPricingSet())
		if err != nil {
			return nil, fmt.Errorf("setting default pricing: %w", err)
		}

		pricingSet, err = store.GetPricingSet(context.Background())
		if err != nil {
			return nil, fmt.Errorf("checking default pricing: %w", err)
		}

		if pricingSet.IsEmpty() {
			return nil, errors.New("unable to initialize store")
		}
	}

	pm := &PricingModule{
		store: store,
	}

	return pm, nil
}

func (pm *PricingModule) GetClusterPricing(ctx context.Context, props pricing.ClusterPricingProperties) (*pricing.ClusterPricing, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	cp, err := pm.getClusterPricing(ctx)
	if err != nil {
		return nil, err
	}

	if cp != nil {
		return cp, nil
	}

	return nil, errors.New("no cluster pricing")
}

func (pm *PricingModule) NewClusterPricingReader(ctx context.Context) (reader.Reader[*pricing.ClusterPricing], error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	cp, err := pm.getClusterPricing(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting node pricing: %w", err)
	}

	return reader.NewSliceReader([]*pricing.ClusterPricing{cp}), nil
}

func (pm *PricingModule) GetNetworkPricing(ctx context.Context, props pricing.NetworkPricingProperties) (*pricing.NetworkPricing, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	nps, err := pm.getNetworkPricing(ctx)
	if err != nil {
		return nil, err
	}

	// Search through the mock data for a matching network pricing entry
	for _, np := range nps {
		if np.Properties.Provider == props.Provider &&
			np.Properties.TrafficDirection == props.TrafficDirection &&
			np.Properties.TrafficType == props.TrafficType &&
			np.Properties.IsNatGateway == props.IsNatGateway {
			return np, nil
		}
	}
	return nil, fmt.Errorf("network pricing not found for provider=%s, trafficDirection=%s, trafficType=%s, isNatGateway=%t",
		props.Provider, props.TrafficDirection, props.TrafficType, props.IsNatGateway)
}

func (pm *PricingModule) NewNetworkPricingReader(ctx context.Context) (reader.Reader[*pricing.NetworkPricing], error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	np, err := pm.getNetworkPricing(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting node pricing: %w", err)
	}

	return reader.NewSliceReader(slices.Clone(np)), nil
}

func (pm *PricingModule) GetNodePricing(ctx context.Context, props pricing.NodePricingProperties) (*pricing.NodePricing, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	np, err := pm.getNodePricing(ctx)
	if err != nil {
		return nil, err
	}

	if np != nil {
		return np, nil
	}

	return nil, errors.New("no node pricing")
}

func (pm *PricingModule) NewNodePricingReader(ctx context.Context) (reader.Reader[*pricing.NodePricing], error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	np, err := pm.getNodePricing(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting node pricing: %w", err)
	}

	return reader.NewSliceReader([]*pricing.NodePricing{np}), nil
}

func (pm *PricingModule) GetPersistentVolumePricing(ctx context.Context, props pricing.PersistentVolumePricingProperties) (*pricing.PersistentVolumePricing, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	pvp, err := pm.getPersistentVolumePricing(ctx)
	if err != nil {
		return nil, err
	}

	if pvp != nil {
		return pvp, nil
	}

	return nil, errors.New("no persistent volume pricing")
}

func (pm *PricingModule) NewPersistentVolumePricingReader(ctx context.Context) (reader.Reader[*pricing.PersistentVolumePricing], error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	pvp, err := pm.getPersistentVolumePricing(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting volume pricing: %w", err)
	}

	return reader.NewSliceReader([]*pricing.PersistentVolumePricing{pvp}), nil
}

func (pm *PricingModule) GetServicePricing(ctx context.Context, props pricing.ServicePricingProperties) (*pricing.ServicePricing, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	sp, err := pm.getServicePricing(ctx)
	if err != nil {
		return nil, err
	}

	if sp != nil {
		return sp, nil
	}

	return nil, errors.New("no service pricing")
}

func (pm *PricingModule) NewServicePricingReader(ctx context.Context) (reader.Reader[*pricing.ServicePricing], error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	sp, err := pm.getServicePricing(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting service pricing: %w", err)
	}

	return reader.NewSliceReader([]*pricing.ServicePricing{sp}), nil
}

func (pm *PricingModule) GetPricingSet(ctx context.Context) (*pricing.PricingSet, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return pm.store.GetPricingSet(ctx)
}

func (pm *PricingModule) SourceKind() string {
	return "basic"
}

func (pm *PricingModule) SourceName() string {
	return "basic"
}

func (pm *PricingModule) Checksum(ctx context.Context) (string, error) {
	pricingSet, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return "", fmt.Errorf("basic pricing module: error getting pricing set: %w", err)
	}

	checksum, err := pricingSet.Checksum()
	if err != nil {
		return "", fmt.Errorf("basic pricing module: error computing checksum: %s", err)
	}

	return checksum, nil
}

// Public CRUD functions

func (pm *PricingModule) SetNodePricePerCPUCoreHour(ctx context.Context, price float64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.setNodePrice(ctx, pricing.ResourceCPU, unit.VCPUHour, price)
}

func (pm *PricingModule) SetNodePricePerRAMGiBHour(ctx context.Context, price float64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.setNodePrice(ctx, pricing.ResourceRAM, unit.GiBHour, price)
}

func (pm *PricingModule) SetNodePricePerGPUHour(ctx context.Context, price float64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.setNodePrice(ctx, pricing.ResourceGPU, unit.GPUHour, price)
}

func (pm *PricingModule) SetNodePricePerLocalDiskGiBHour(ctx context.Context, price float64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.setNodePrice(ctx, pricing.ResourceStorage, unit.GiBHour, price)
}

func (pm *PricingModule) SetVolumePricePerStorageGiBHour(ctx context.Context, price float64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.setVolumePrice(ctx, pricing.ResourceStorage, unit.GiBHour, price)
}

// Private functions to set a price by resource and unit

func (pm *PricingModule) setNodePrice(ctx context.Context, resource pricing.Resource, unit unit.Unit, price float64) error {
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		return fmt.Errorf("getting node pricing: %w", err)
	}

	if np.Prices == nil {
		np.Prices = pricing.Prices{}
	}
	np.Prices[resource] = pricing.Price{Unit: unit, Price: price}

	err = pm.setNodePricing(ctx, np)
	if err != nil {
		return fmt.Errorf("setting node pricing: %w", err)
	}

	return nil
}

func (pm *PricingModule) setVolumePrice(ctx context.Context, resource pricing.Resource, unit unit.Unit, price float64) error {
	vp, err := pm.getPersistentVolumePricing(ctx)
	if err != nil {
		return fmt.Errorf("getting volume pricing: %w", err)
	}

	if vp.Prices == nil {
		vp.Prices = pricing.Prices{}
	}
	vp.Prices[resource] = pricing.Price{Unit: unit, Price: price}

	err = pm.setPersistentVolumePricing(ctx, vp)
	if err != nil {
		return fmt.Errorf("setting volume pricing: %w", err)
	}

	return nil
}

// Private functions to get and set pricing

func (pm *PricingModule) getClusterPricing(ctx context.Context) (*pricing.ClusterPricing, error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting pricing: %w", err)
	}

	if len(ps.ClusterPricing) == 0 {
		return nil, errors.New("not found")
	}

	// Only one default ClusterPricing is allowed in basic pricing.
	// If multiple exist, return only the first one.
	return ps.ClusterPricing[0], nil
}

func (pm *PricingModule) setClusterPricing(ctx context.Context, cp *pricing.ClusterPricing) error {
	// TODO
	return errors.New("not implemented")
}

func (pm *PricingModule) getNetworkPricing(ctx context.Context) ([]*pricing.NetworkPricing, error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting pricing: %w", err)
	}

	if len(ps.NetworkPricing) == 0 {
		return nil, errors.New("not found")
	}

	return ps.NetworkPricing, nil
}

func (pm *PricingModule) setNetworkPricing(ctx context.Context, np *pricing.NetworkPricing) error {
	// TODO
	return errors.New("not implemented")
}

func (pm *PricingModule) getNodePricing(ctx context.Context) (*pricing.NodePricing, error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting pricing: %w", err)
	}

	if len(ps.NodePricing) == 0 {
		return nil, errors.New("not found")
	}

	// Only one default NodePricing is allowed in basic pricing.
	// If multiple exist, return only the first one.
	return ps.NodePricing[0], nil
}

func (pm *PricingModule) setNodePricing(ctx context.Context, np *pricing.NodePricing) error {
	if np == nil {
		return errors.New("nil node pricing")
	}

	// Get the pricing set
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return fmt.Errorf("getting pricing: %w", err)
	}

	// Only one default NodePricing is allowed in basic pricing.
	ps.NodePricing = []*pricing.NodePricing{np}

	// Set the new pricing set
	err = pm.store.SetPricingSet(ctx, ps)
	if err != nil {
		return fmt.Errorf("setting pricing: %w", err)
	}

	return nil
}

func (pm *PricingModule) getPersistentVolumePricing(ctx context.Context) (*pricing.PersistentVolumePricing, error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting pricing: %w", err)
	}

	if len(ps.PersistentVolumePricing) == 0 {
		return nil, errors.New("not found")
	}

	// Only one default VolumePricing is allowed in basic pricing.
	// If multiple exist, return only the first one.
	return ps.PersistentVolumePricing[0], nil
}

func (pm *PricingModule) setPersistentVolumePricing(ctx context.Context, vp *pricing.PersistentVolumePricing) error {
	if vp == nil {
		return errors.New("nil volume pricing")
	}

	// Get the pricing set
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return fmt.Errorf("getting pricing: %w", err)
	}

	// Only one default VolumePricing is allowed in basic pricing.
	ps.PersistentVolumePricing = []*pricing.PersistentVolumePricing{vp}

	// Set the new pricing set
	err = pm.store.SetPricingSet(ctx, ps)
	if err != nil {
		return fmt.Errorf("setting pricing: %w", err)
	}

	return nil
}

func (pm *PricingModule) getServicePricing(ctx context.Context) (*pricing.ServicePricing, error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting pricing: %w", err)
	}

	if len(ps.ServicePricing) == 0 {
		return nil, errors.New("not found")
	}

	// Only one default ServicePricing is allowed in basic pricing.
	// If multiple exist, return only the first one.
	return ps.ServicePricing[0], nil
}

func (pm *PricingModule) setServicePricing(ctx context.Context, sp *pricing.ServicePricing) error {
	// TODO
	return errors.New("not implemented")
}
