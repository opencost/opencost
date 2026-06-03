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
	currency unit.Currency
	store    PricingStore
}

func NewBasicPricingModule(store PricingStore) (*PricingModule, error) {
	currencies := store.GetCurrencies(context.Background())

	if len(currencies) == 0 {
		// Store has no pricing. Create default store.
		defaultPricingSet := &pricing.PricingSet{
			Nodes:   []*pricing.NodePricing{GetDefaultNodePricing(unit.USD)},
			Volumes: []*pricing.VolumePricing{GetDefaultVolumePricing(unit.USD)},
		}

		err := store.SetPricingSet(context.Background(), defaultPricingSet)
		if err != nil {
			return nil, fmt.Errorf("creating default pricing: %w", err)
		}

		currencies = store.GetCurrencies(context.Background())
		if len(currencies) == 0 {
			return nil, errors.New("failed to create default pricing")
		}
	}

	if len(currencies) > 1 {
		log.Warnf("basic pricing module detected multiple currencies: %v", currencies)
	}

	pm := &PricingModule{
		currency: currencies[0],
		store:    store,
	}

	return pm, nil
}

func (pm *PricingModule) GetCurrency() unit.Currency {
	return pm.currency
}

func (pm *PricingModule) SetCurrency(ctx context.Context, currency unit.Currency) error {
	prevCurrency := pm.currency
	if currency == prevCurrency {
		return nil
	}

	// 1. Convert existing node pricing to new currency
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		return fmt.Errorf("getting node pricing: %w", err)
	}

	// Set up new Prices for the new currency
	newPrices := []pricing.Price{}

	// Convert all existing prices to the new currency
	oldPrices, ok := np.Prices[prevCurrency]
	if !ok {
		log.Warnf("setting currency to '%s': no node prices found for existing currency '%s'", currency, pm.currency)
		// There are no prices for the current currency.
		// Set default prices using the new currency.
		newPrices = GetDefaultNodePricing(currency).Prices[currency]
	}

	for _, price := range oldPrices {
		newPrices = append(newPrices, pricing.Price{
			Currency: currency,
			Unit:     price.Unit,
			Price:    price.Price,
		})
	}

	// Set new prices under new currency
	np.Prices = make(pricing.Prices, 1)
	np.Prices[currency] = newPrices

	// Set node pricing on the module
	err = pm.setNodePricing(ctx, np)
	if err != nil {
		return fmt.Errorf("setting node pricing: %w", err)
	}

	// 2. Convert existing volume pricing to new currency
	vp, err := pm.getVolumePricing(ctx)
	if err != nil {
		return fmt.Errorf("getting node pricing: %w", err)
	}

	// Set up new Prices for the new currency
	newPrices = []pricing.Price{}

	// Convert all existing prices to the new currency
	oldPrices, ok = vp.Prices[prevCurrency]
	if !ok {
		log.Warnf("setting currency to '%s': no node prices found for existing currency '%s'", currency, pm.currency)
		// There are no prices for the current currency.
		// Set default prices using the new currency.
		newPrices = GetDefaultVolumePricing(currency).Prices[currency]
	}

	for _, price := range oldPrices {
		newPrices = append(newPrices, pricing.Price{
			Currency: currency,
			Unit:     price.Unit,
			Price:    price.Price,
		})
	}

	// Set new prices under new currency
	vp.Prices = make(pricing.Prices, 1)
	vp.Prices[currency] = newPrices

	// Set node pricing on the module
	err = pm.setVolumePricing(ctx, vp)
	if err != nil {
		return fmt.Errorf("setting node pricing: %w", err)
	}

	return nil
}

func (pm *PricingModule) SetPricePerCPUCoreHour(ctx context.Context, price float64) error {
	return pm.setNodePrice(ctx, unit.VCPUHour, price)
}

func (pm *PricingModule) SetPricePerRAMGiBHour(ctx context.Context, price float64) error {
	return pm.setNodePrice(ctx, unit.RAMGiBHour, price)
}

func (pm *PricingModule) SetPricePerGPUHour(ctx context.Context, price float64) error {
	return pm.setNodePrice(ctx, unit.GPUHour, price)
}

func (pm *PricingModule) SetPricePerLocalDiskGiBHour(ctx context.Context, price float64) error {
	return pm.setNodePrice(ctx, unit.StorageGiBHour, price)
}

func (pm *PricingModule) NewNodePricingReader(ctx context.Context) (reader.Reader[*pricing.NodePricing], error) {
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting node pricing: %w", err)
	}
	return reader.NewSliceReader([]*pricing.NodePricing{np}), nil
}

func (pm *PricingModule) NewVolumePricingReader(ctx context.Context) (reader.Reader[*pricing.VolumePricing], error) {
	vp, err := pm.getVolumePricing(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting volume pricing: %w", err)
	}
	return reader.NewSliceReader([]*pricing.VolumePricing{vp}), nil
}

func (pm *PricingModule) setNodePrice(ctx context.Context, unit unit.Unit, price float64) error {
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		return fmt.Errorf("getting node pricing: %w", err)
	}

	prices, ok := np.Prices[pm.currency]
	if !ok {
		log.Warnf("setting price per %s to '%f': no node prices found for existing currency '%s'", unit, price, pm.currency)
		// There are no prices for the current currency.
		// Set default prices using the new currency.
		np = GetDefaultNodePricing(pm.currency)
	}

	// Set the price with unit GiBHour to the given price
	for i, p := range prices {
		if p.Unit == unit {
			prices[i] = pricing.Price{
				Currency: p.Currency,
				Unit:     p.Unit,
				Price:    price,
			}
		}
	}

	// Set the new node pricing
	err = pm.setNodePricing(ctx, np)
	if err != nil {
		return fmt.Errorf("setting node pricing: %w", err)
	}

	return nil
}

func (pm *PricingModule) setVolumePrice(ctx context.Context, unit unit.Unit, price float64) error {
	vp, err := pm.getVolumePricing(ctx)
	if err != nil {
		return fmt.Errorf("getting volume pricing: %w", err)
	}

	prices, ok := vp.Prices[pm.currency]
	if !ok {
		log.Warnf("setting price per %s to '%f': no volume prices found for existing currency '%s'", unit, price, pm.currency)
		// There are no prices for the current currency.
		// Set default prices using the new currency.
		vp = GetDefaultVolumePricing(pm.currency)
	}

	// Set the price with unit GiBHour to the given price
	for i, p := range prices {
		if p.Unit == unit {
			prices[i] = pricing.Price{
				Currency: p.Currency,
				Unit:     p.Unit,
				Price:    price,
			}
		}
	}

	// Set the new volume pricing
	err = pm.setVolumePricing(ctx, vp)
	if err != nil {
		return fmt.Errorf("setting node pricing: %w", err)
	}

	return nil
}

func (pm *PricingModule) getNodePricing(ctx context.Context) (*pricing.NodePricing, error) {
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

func (pm *PricingModule) setNodePricing(ctx context.Context, np *pricing.NodePricing) error {
	if np == nil {
		return errors.New("nil node pricing")
	}

	// Make sure precisely one currency is set
	currs := np.GetCurrencies()
	if len(currs) == 0 {
		return errors.New("pricing is empty")
	}
	if len(currs) > 1 {
		return fmt.Errorf("setting multiple currencies: %v", currs)
	}

	// Update PricingModule to use given currency
	pm.currency = currs[0]

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

func (pm *PricingModule) getVolumePricing(ctx context.Context) (*pricing.VolumePricing, error) {
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

func (pm *PricingModule) setVolumePricing(ctx context.Context, vp *pricing.VolumePricing) error {
	if vp == nil {
		return errors.New("nil volume pricing")
	}

	// Make sure precisely one currency is set
	currs := vp.GetCurrencies()
	if len(currs) == 0 {
		return errors.New("pricing is empty")
	}
	if len(currs) > 1 {
		return fmt.Errorf("setting multiple currencies: %v", currs)
	}

	// Update PricingModule to use given currency
	pm.currency = currs[0]

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
