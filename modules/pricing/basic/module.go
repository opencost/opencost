package basic

import (
	"context"
	"errors"
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/reader"
	"github.com/opencost/opencost/core/pkg/unit"
)

type PricingModule struct {
	store PricingStore
}

func NewBasicPricingModule(ctx context.Context, store PricingStore) *PricingModule {
	return &PricingModule{
		store: store,
	}
}

func (pm *PricingModule) GetCurrency(ctx context.Context) unit.Currency {
	currencies, err := pm.store.GetCurrencies(ctx)
	if len(currencies) == 0 || err != nil {
		// Default to USD if the store has no prices / currencies
		return unit.USD
	}

	// Expect only one currency. If multiple exist, then default
	// to the first currency listed.
	return currencies[0]
}

func (pm *PricingModule) GetNodePricing(ctx context.Context) (*pricing.NodePricing, error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting pricing: %w", err)
	}

	if len(ps.Nodes) == 0 {
		return nil, errors.New("not found")
	}

	// Only one default NodePricing is allowed in basic pricing.
	// If multiple exist, return only the first one.
	return ps.Nodes[0], nil
}

func (pm *PricingModule) SetNodePricing(ctx context.Context, np *pricing.NodePricing) error {
	if np == nil {
		return errors.New("nil node pricing")
	}

	// Check that only one currency exists in the given node pricing, and that
	// it matches the current configuration.
	currs := np.GetCurrencies()
	if len(currs) == 0 {
		return errors.New("empty node pricing")
	}
	if len(currs) > 1 {
		return fmt.Errorf("restricted to one currency, but received %d", len(currs))
	}
	curr := currs[0]

	if curr != pm.GetCurrency(ctx) {
		return fmt.Errorf("incorrect currency '%s' (currently configured for '%s')", curr, pm.GetCurrency(ctx))
	}

	// Get the pricing set
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return fmt.Errorf("getting pricing: %w", err)
	}

	// Only one default NodePricing is allowed in basic pricing.
	ps.Nodes = []*pricing.NodePricing{np}

	// Set the new pricing set
	err = pm.store.SetPricingSet(ctx, ps)
	if err != nil {
		return fmt.Errorf("setting pricing: %w", err)
	}

	return nil
}

func (pm *PricingModule) GetVolumePricing(ctx context.Context) (*pricing.VolumePricing, error) {
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting pricing: %w", err)
	}

	if len(ps.Volumes) == 0 {
		return nil, errors.New("not found")
	}

	// Only one default VolumePricing is allowed in basic pricing.
	// If multiple exist, return only the first one.
	return ps.Volumes[0], nil
}

func (pm *PricingModule) SetVolumePricing(ctx context.Context, vp *pricing.VolumePricing) error {
	if vp == nil {
		return errors.New("nil node pricing")
	}

	// Check that only one currency exists in the given node pricing, and that
	// it matches the current configuration.
	currs := vp.GetCurrencies()
	if len(currs) == 0 {
		return errors.New("empty node pricing")
	}
	if len(currs) > 1 {
		return fmt.Errorf("restricted to one currency, but received %d", len(currs))
	}
	curr := currs[0]

	if curr != pm.GetCurrency(ctx) {
		return fmt.Errorf("incorrect currency '%s' (currently configured for '%s')", curr, pm.GetCurrency(ctx))
	}

	// Get the pricing set
	ps, err := pm.store.GetPricingSet(ctx)
	if err != nil {
		return fmt.Errorf("getting pricing: %w", err)
	}

	// Only one default VolumePricing is allowed in basic pricing.
	ps.Volumes = []*pricing.VolumePricing{vp}

	// Set the new pricing set
	err = pm.store.SetPricingSet(ctx, ps)
	if err != nil {
		return fmt.Errorf("setting pricing: %w", err)
	}

	return nil
}

func (pm *PricingModule) SetCurrency(ctx context.Context, currency unit.Currency) error {
	// 1. Convert existing node pricing to new currency
	np, err := pm.GetNodePricing(ctx)
	if err != nil {
		return fmt.Errorf("getting node pricing: %w", err)
	}

	prices, ok := np.Prices[pm.GetCurrency(ctx)]
	if !ok {
		log.Warnf("setting currency to '%s': no node prices found for existing currency '%s'", currency, pm.GetCurrency(ctx))
		// There are no prices for the current currency.
		// Set default prices using the new currency.
		np = GetDefaultNodePricing(currency)
	}

	// Set up new Prices entry for the new currency
	np.Prices[currency] = []pricing.Price{}

	// Convert all existing prices to the new currency
	for _, price := range prices {
		np.Prices[currency] = append(np.Prices[currency], pricing.Price{
			Currency: currency,
			Unit:     price.Unit,
			Price:    price.Price,
		})
	}

	// Set node pricing
	err = pm.SetNodePricing(ctx, np)
	if err != nil {
		return fmt.Errorf("setting node pricing: %w", err)
	}

	// 2. Convert existing volume pricing to new currency
	vp, err := pm.GetVolumePricing(ctx)
	if err != nil {
		return fmt.Errorf("getting volume pricing: %w", err)
	}

	prices, ok = vp.Prices[pm.GetCurrency(ctx)]
	if !ok {
		log.Warnf("setting currency to '%s': no volume prices found for existing currency '%s'", currency, pm.GetCurrency(ctx))
		// There are no prices for the current currency.
		// Set default prices using the new currency.
		vp = GetDefaultVolumePricing(currency)
	}

	// Set up new Prices entry for the new currency
	vp.Prices[currency] = []pricing.Price{}

	// Convert all existing prices to the new currency
	for _, price := range prices {
		vp.Prices[currency] = append(vp.Prices[currency], pricing.Price{
			Currency: currency,
			Unit:     price.Unit,
			Price:    price.Price,
		})
	}

	// Set volume pricing
	err = pm.SetVolumePricing(ctx, vp)
	if err != nil {
		return fmt.Errorf("setting volume pricing: %w", err)
	}

	return nil
}

func (pm *PricingModule) SetPricePerCPUCoreHour(ctx context.Context, price float64) error {
	np, err := pm.GetNodePricing(ctx)
	if err != nil {
		return fmt.Errorf("getting node pricing: %w", err)
	}

	prices, ok := np.Prices[pm.GetCurrency(ctx)]
	if !ok {
		log.Warnf("setting price per VCPU-hour to '%f': no node prices found for existing currency '%s'", price, pm.GetCurrency(ctx))
		// There are no prices for the current currency.
		// Set default prices using the new currency.
		np = GetDefaultNodePricing(pm.GetCurrency(ctx))
	}

	// Set the price with unit VCPUHour to the given price
	for i, p := range prices {
		if p.Unit == unit.VCPUHour {
			prices[i] = pricing.Price{
				Currency: p.Currency,
				Unit:     p.Unit,
				Price:    price,
			}
		}
	}

	return nil
}

func (pm *PricingModule) SetPricePerRAMGiBHour(ctx context.Context, price float64) error {
	np, err := pm.GetNodePricing(ctx)
	if err != nil {
		return fmt.Errorf("getting node pricing: %w", err)
	}

	prices, ok := np.Prices[pm.GetCurrency(ctx)]
	if !ok {
		log.Warnf("setting price per RAM GiB-hour to '%f': no node prices found for existing currency '%s'", price, pm.GetCurrency(ctx))
		// There are no prices for the current currency.
		// Set default prices using the new currency.
		np = GetDefaultNodePricing(pm.GetCurrency(ctx))
	}

	// TODO: does this need to be RAMGiBHour?

	// Set the price with unit GiBHour to the given price
	for i, p := range prices {
		if p.Unit == unit.GiBHour {
			prices[i] = pricing.Price{
				Currency: p.Currency,
				Unit:     p.Unit,
				Price:    price,
			}
		}
	}

	return nil
}

func (pm *PricingModule) SetPricePerGPUHour(ctx context.Context, price float64) error {
	np, err := pm.GetNodePricing(ctx)
	if err != nil {
		return fmt.Errorf("getting node pricing: %w", err)
	}

	prices, ok := np.Prices[pm.GetCurrency(ctx)]
	if !ok {
		log.Warnf("setting price per GPU-hour to '%f': no node prices found for existing currency '%s'", price, pm.GetCurrency(ctx))
		// There are no prices for the current currency.
		// Set default prices using the new currency.
		np = GetDefaultNodePricing(pm.GetCurrency(ctx))
	}

	// Set the price with unit GPUHour to the given price
	for i, p := range prices {
		if p.Unit == unit.GPUHour {
			prices[i] = pricing.Price{
				Currency: p.Currency,
				Unit:     p.Unit,
				Price:    price,
			}
		}
	}

	return nil
}

func (pm *PricingModule) SetPricePerLocalDiskGiBHour(ctx context.Context, price float64) error {
	// TODO: cannot implement without disambiguating RAMGiBHour from LocalStorageGiBHour
	return errors.New("not implemented")
}

func (pm *PricingModule) NewNodePricingReader(ctx context.Context) (reader.Reader[*pricing.NodePricing], error) {
	np, err := pm.GetNodePricing(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting node pricing: %w", err)
	}
	return reader.NewSliceReader([]*pricing.NodePricing{np}), nil
}

func (pm *PricingModule) NewVolumePricingReader(ctx context.Context) (reader.Reader[*pricing.VolumePricing], error) {
	vp, err := pm.GetVolumePricing(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting volume pricing: %w", err)
	}
	return reader.NewSliceReader([]*pricing.VolumePricing{vp}), nil
}
