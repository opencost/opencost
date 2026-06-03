package basic

import (
	"context"
	"os"
	"testing"

	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/unit"
	"github.com/stretchr/testify/require"
)

func TestPricingModule(t *testing.T) {
	stores := map[string]PricingStore{
		"MemoryPricingStore":  NewMemoryPricingStore(),
		"StoragePricingStore": NewStoragePricingStore(newFileStorage(t), "pricing.json"),
	}

	for name, store := range stores {
		t.Run(name, testPricingModuleWithStore(store))
	}
}

func testPricingModuleWithStore(store PricingStore) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := t.Context()

		pm, err := NewBasicPricingModule(store)
		require.NoError(t, err)

		t.Run("DefaultPricing", func(t *testing.T) {
			testDefaultPricing(t, ctx, pm)
		})

		t.Run("SetCurrency", func(t *testing.T) {
			testSetCurrency(t, ctx, pm)
		})

		t.Run("SetPricePerCPUCoreHour", func(t *testing.T) {
			testSetPricePerCPUCoreHour(t, ctx, pm)
		})

		t.Run("SetPricePerRAMGiBHour", func(t *testing.T) {
			testSetPricePerRAMGiBHour(t, ctx, pm)
		})

		t.Run("SetPricePerGPUHour", func(t *testing.T) {
			testSetPricePerGPUHour(t, ctx, pm)
		})

		t.Run("ModulePersistence", func(t *testing.T) {
			// Create a new PricingModule with the same store
			pm2, err := NewBasicPricingModule(store)
			require.NoError(t, err)

			// Verify that pricing persists
			np, err := pm2.getNodePricing(ctx)
			if err != nil {
				t.Fatalf("Failed to get node pricing: %v", err)
			}

			if np == nil {
				t.Fatal("Expected node pricing to be persisted")
			}
		})
	}
}

// testDefaultPricing verifies that a freshly created PricingModule contains default pricing
func testDefaultPricing(t *testing.T, ctx context.Context, pm *PricingModule) {
	// Test default currency
	currency := pm.GetCurrency()
	if currency != unit.USD {
		t.Errorf("Expected default currency to be USD, got %s", currency)
	}

	// Test default node pricing
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get node pricing: %v", err)
	}

	if np == nil {
		t.Fatal("Expected node pricing to exist")
	}

	prices, err := np.Prices.GetPricesInCurrency(unit.USD)
	if err != nil {
		t.Fatalf("Failed to get prices in USD: %v", err)
	}

	// Verify default prices exist
	foundCPU := false
	foundRAM := false
	foundGPU := false

	for _, price := range prices {
		switch price.Unit {
		case unit.VCPUHour:
			foundCPU = true
			if price.Price != DefaultNodePricePerVCPUHour {
				t.Errorf("Expected CPU price to be %f, got %f", DefaultNodePricePerVCPUHour, price.Price)
			}
		case unit.GiBHour:
			foundRAM = true
			if price.Price != DefaultNodePricePerRAMGiBHour {
				t.Errorf("Expected RAM price to be %f, got %f", DefaultNodePricePerRAMGiBHour, price.Price)
			}
		case unit.GPUHour:
			foundGPU = true
			if price.Price != DefaultNodePricePerGPUHour {
				t.Errorf("Expected GPU price to be %f, got %f", DefaultNodePricePerGPUHour, price.Price)
			}
		}
	}

	if !foundCPU {
		t.Error("Expected to find CPU pricing")
	}
	if !foundRAM {
		t.Error("Expected to find RAM pricing")
	}
	if !foundGPU {
		t.Error("Expected to find GPU pricing")
	}

	// Test default volume pricing
	vp, err := pm.getVolumePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get volume pricing: %v", err)
	}

	if vp == nil {
		t.Fatal("Expected volume pricing to exist")
	}

	volumePrices, err := vp.Prices.GetPricesInCurrency(unit.USD)
	if err != nil {
		t.Fatalf("Failed to get volume prices in USD: %v", err)
	}

	foundVolume := false
	for _, price := range volumePrices {
		if price.Unit == unit.GiBHour {
			foundVolume = true
			if price.Price != DefaultVolumePricePerGiBHour {
				t.Errorf("Expected volume price to be %f, got %f", DefaultVolumePricePerGiBHour, price.Price)
			}
		}
	}

	if !foundVolume {
		t.Error("Expected to find volume pricing")
	}
}

// testSetCurrency tests the SetCurrency function
func testSetCurrency(t *testing.T, ctx context.Context, pm *PricingModule) {
	// Get current pricing to compare later
	npBefore, err := pm.getNodePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get node pricing before currency change: %v", err)
	}

	pricesBefore, err := npBefore.Prices.GetPricesInCurrency(pm.GetCurrency())
	if err != nil {
		t.Fatalf("Failed to get prices before currency change: %v", err)
	}

	vpBefore, err := pm.getVolumePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get volume pricing before currency change: %v", err)
	}

	volumePricesBefore, err := vpBefore.Prices.GetPricesInCurrency(pm.GetCurrency())
	if err != nil {
		t.Fatalf("Failed to get volume prices before currency change: %v", err)
	}

	// Change currency to EUR
	err = pm.SetCurrency(ctx, unit.EUR)
	if err != nil {
		t.Fatalf("Failed to set currency: %v", err)
	}

	// Verify currency changed
	currency := pm.GetCurrency()
	if currency != unit.EUR {
		t.Errorf("Expected currency to be EUR, got %s", currency)
	}

	// Verify node pricing units and prices remain the same, only currency changed
	npAfter, err := pm.getNodePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get node pricing after currency change: %v", err)
	}

	pricesAfter, err := npAfter.Prices.GetPricesInCurrency(unit.EUR)
	if err != nil {
		t.Fatalf("Failed to get prices after currency change: %v", err)
	}

	if len(pricesBefore) != len(pricesAfter) {
		t.Errorf("Expected same number of prices, got %d before and %d after", len(pricesBefore), len(pricesAfter))
	}

	// Create maps for easier comparison
	beforeMap := make(map[unit.Unit]float64)
	for _, p := range pricesBefore {
		beforeMap[p.Unit] = p.Price
	}

	afterMap := make(map[unit.Unit]float64)
	for _, p := range pricesAfter {
		afterMap[p.Unit] = p.Price
		if p.Currency != unit.EUR {
			t.Errorf("Expected currency to be EUR, got %s", p.Currency)
		}
	}

	// Verify units and prices match
	for unit, priceBefore := range beforeMap {
		priceAfter, ok := afterMap[unit]
		if !ok {
			t.Errorf("Unit %s not found after currency change", unit)
			continue
		}
		if priceBefore != priceAfter {
			t.Errorf("Price for unit %s changed from %f to %f", unit, priceBefore, priceAfter)
		}
	}

	// Verify volume pricing units and prices remain the same
	vpAfter, err := pm.getVolumePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get volume pricing after currency change: %v", err)
	}

	volumePricesAfter, err := vpAfter.Prices.GetPricesInCurrency(unit.EUR)
	if err != nil {
		t.Fatalf("Failed to get volume prices after currency change: %v", err)
	}

	if len(volumePricesBefore) != len(volumePricesAfter) {
		t.Errorf("Expected same number of volume prices, got %d before and %d after", len(volumePricesBefore), len(volumePricesAfter))
	}

	for i, priceBefore := range volumePricesBefore {
		priceAfter := volumePricesAfter[i]
		if priceAfter.Currency != unit.EUR {
			t.Errorf("Expected currency to be EUR, got %s", priceAfter.Currency)
		}
		if priceBefore.Unit != priceAfter.Unit {
			t.Errorf("Unit changed from %s to %s", priceBefore.Unit, priceAfter.Unit)
		}
		if priceBefore.Price != priceAfter.Price {
			t.Errorf("Price changed from %f to %f", priceBefore.Price, priceAfter.Price)
		}
	}

	// Change back to USD for other tests
	err = pm.SetCurrency(ctx, unit.USD)
	if err != nil {
		t.Fatalf("Failed to set currency back to USD: %v", err)
	}
}

// testSetPricePerCPUCoreHour tests the SetPricePerCPUCoreHour function
func testSetPricePerCPUCoreHour(t *testing.T, ctx context.Context, pm *PricingModule) {
	newPrice := 0.075

	err := pm.SetPricePerCPUCoreHour(ctx, newPrice)
	if err != nil {
		t.Fatalf("Failed to set CPU price: %v", err)
	}

	// Verify the price was set
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get node pricing: %v", err)
	}

	prices, err := np.Prices.GetPricesInCurrency(pm.GetCurrency())
	if err != nil {
		t.Fatalf("Failed to get prices: %v", err)
	}

	found := false
	for _, price := range prices {
		if price.Unit == unit.VCPUHour {
			found = true
			if price.Price != newPrice {
				t.Errorf("Expected CPU price to be %f, got %f", newPrice, price.Price)
			}
		}
	}

	if !found {
		t.Error("Expected to find CPU pricing")
	}
}

// testSetPricePerRAMGiBHour tests the SetPricePerRAMGiBHour function
func testSetPricePerRAMGiBHour(t *testing.T, ctx context.Context, pm *PricingModule) {
	newPrice := 0.008

	err := pm.SetPricePerRAMGiBHour(ctx, newPrice)
	if err != nil {
		t.Fatalf("Failed to set RAM price: %v", err)
	}

	// Verify the price was set
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get node pricing: %v", err)
	}

	prices, err := np.Prices.GetPricesInCurrency(pm.GetCurrency())
	if err != nil {
		t.Fatalf("Failed to get prices: %v", err)
	}

	found := false
	for _, price := range prices {
		if price.Unit == unit.GiBHour {
			found = true
			if price.Price != newPrice {
				t.Errorf("Expected RAM price to be %f, got %f", newPrice, price.Price)
			}
		}
	}

	if !found {
		t.Error("Expected to find RAM pricing")
	}
}

// testSetPricePerGPUHour tests the SetPricePerGPUHour function
func testSetPricePerGPUHour(t *testing.T, ctx context.Context, pm *PricingModule) {
	newPrice := 2.0

	err := pm.SetPricePerGPUHour(ctx, newPrice)
	if err != nil {
		t.Fatalf("Failed to set GPU price: %v", err)
	}

	// Verify the price was set
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get node pricing: %v", err)
	}

	prices, err := np.Prices.GetPricesInCurrency(pm.GetCurrency())
	if err != nil {
		t.Fatalf("Failed to get prices: %v", err)
	}

	found := false
	for _, price := range prices {
		if price.Unit == unit.GPUHour {
			found = true
			if price.Price != newPrice {
				t.Errorf("Expected GPU price to be %f, got %f", newPrice, price.Price)
			}
		}
	}

	if !found {
		t.Error("Expected to find GPU pricing")
	}
}

func newFileStorage(t *testing.T) storage.Storage {
	tempDir, err := os.MkdirTemp("", "pricing-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	return storage.NewFileStorage(tempDir)
}
