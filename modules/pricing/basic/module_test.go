package basic

import (
	"context"
	"os"
	"testing"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/reader"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestNewBasicPricingModuleEmptyStore verifies the constructor populates a
// default pricing set when given an empty store.
func TestNewBasicPricingModuleEmptyStore(t *testing.T) {
	store := pricing.NewMemoryPricingStore()

	pm, err := NewBasicPricingModule(store)
	require.NoError(t, err)

	ps, err := store.GetPricingSet(t.Context())
	require.NoError(t, err)
	require.False(t, ps.IsEmpty())

	np, err := pm.getNodePricing(t.Context())
	require.NoError(t, err)
	require.NotNil(t, np)
}

func TestPricingModule(t *testing.T) {
	memoryPricingStore := pricing.NewMemoryPricingStore()

	filePricingStore, err := pricing.NewStoragePricingStore(t.Context(), newFileStorage(t), "pricing.json")
	require.NoError(t, err)

	stores := map[string]pricing.PricingStore{
		"MemoryPricingStore":  memoryPricingStore,
		"StoragePricingStore": filePricingStore,
	}

	for name, store := range stores {
		t.Run(name, testPricingModuleWithStore(store))
	}
}

func testPricingModuleWithStore(store pricing.PricingStore) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := t.Context()

		pm, err := NewBasicPricingModule(store)
		require.NoError(t, err)

		t.Run("DefaultPricing", func(t *testing.T) {
			testDefaultPricing(t, ctx, pm)
		})

		t.Run("SetNodePricePerCPUCoreHour", func(t *testing.T) {
			testSetNodePricePerCPUCoreHour(t, ctx, pm)
		})

		t.Run("SetNodePricePerRAMGiBHour", func(t *testing.T) {
			testSetNodePricePerRAMGiBHour(t, ctx, pm)
		})

		t.Run("SetNodePricePerGPUHour", func(t *testing.T) {
			testSetNodePricePerGPUHour(t, ctx, pm)
		})

		t.Run("SetNodePricePerLocalDiskGiBHour", func(t *testing.T) {
			testSetNodePricePerLocalDiskGiBHour(t, ctx, pm)
		})

		t.Run("SetVolumePricePerStorageGiBHour", func(t *testing.T) {
			testSetVolumePricePerStorageGiBHour(t, ctx, pm)
		})

		t.Run("NewNodePricingReader", func(t *testing.T) {
			testNewNodePricingReader(t, ctx, pm)
		})

		t.Run("NewVolumePricingReader", func(t *testing.T) {
			testNewVolumePricingReader(t, ctx, pm)
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
	// Test default node pricing
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get node pricing: %v", err)
	}

	if np == nil {
		t.Fatal("Expected node pricing to exist")
	}

	// Prices are keyed by Resource. RAM and local disk share the GiB-hr unit,
	// so the Resource key is what distinguishes them.
	nodeChecks := []struct {
		resource pricing.Resource
		want     float64
	}{
		{pricing.ResourceCPU, DefaultNodePricePerVCPUHour},
		{pricing.ResourceRAM, DefaultNodePricePerRAMGiBHour},
		{pricing.ResourceGPU, DefaultNodePricePerGPUHour},
		{pricing.ResourceStorage, DefaultNodePricePerLocalDiskGiBHour},
	}
	for _, c := range nodeChecks {
		price, ok := np.Prices[c.resource]
		if !ok {
			t.Errorf("Expected to find %s pricing", c.resource)
			continue
		}
		if price.Price != c.want {
			t.Errorf("Expected %s price to be %f, got %f", c.resource, c.want, price.Price)
		}
	}

	// Test default volume pricing
	vp, err := pm.getPersistentVolumePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get volume pricing: %v", err)
	}

	if vp == nil {
		t.Fatal("Expected volume pricing to exist")
	}

	volumePrice, ok := vp.Prices[pricing.ResourceStorage]
	if !ok {
		t.Fatal("Expected to find volume storage pricing")
	}
	if volumePrice.Price != DefaultPersistentVolumePricePerGiBHour {
		t.Errorf("Expected volume price to be %f, got %f", DefaultPersistentVolumePricePerGiBHour, volumePrice.Price)
	}
}

// testSetNodePricePerCPUCoreHour tests the SetNodePricePerCPUCoreHour function
func testSetNodePricePerCPUCoreHour(t *testing.T, ctx context.Context, pm *PricingModule) {
	newPrice := 0.075

	err := pm.SetNodePricePerCPUCoreHour(ctx, newPrice)
	if err != nil {
		t.Fatalf("Failed to set CPU price: %v", err)
	}

	// Verify the price was set
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get node pricing: %v", err)
	}

	price, ok := np.Prices[pricing.ResourceCPU]
	if !ok {
		t.Fatal("Expected to find CPU pricing")
	}
	if price.Price != newPrice {
		t.Errorf("Expected CPU price to be %f, got %f", newPrice, price.Price)
	}
}

// testSetNodePricePerRAMGiBHour tests the SetNodePricePerRAMGiBHour function
func testSetNodePricePerRAMGiBHour(t *testing.T, ctx context.Context, pm *PricingModule) {
	newPrice := 0.008

	err := pm.SetNodePricePerRAMGiBHour(ctx, newPrice)
	if err != nil {
		t.Fatalf("Failed to set RAM price: %v", err)
	}

	// Verify the price was set
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get node pricing: %v", err)
	}

	price, ok := np.Prices[pricing.ResourceRAM]
	if !ok {
		t.Fatal("Expected to find RAM pricing")
	}
	if price.Price != newPrice {
		t.Errorf("Expected RAM price to be %f, got %f", newPrice, price.Price)
	}
}

// testSetNodePricePerGPUHour tests the SetNodePricePerGPUHour function
func testSetNodePricePerGPUHour(t *testing.T, ctx context.Context, pm *PricingModule) {
	newPrice := 2.0

	err := pm.SetNodePricePerGPUHour(ctx, newPrice)
	if err != nil {
		t.Fatalf("Failed to set GPU price: %v", err)
	}

	// Verify the price was set
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get node pricing: %v", err)
	}

	price, ok := np.Prices[pricing.ResourceGPU]
	if !ok {
		t.Fatal("Expected to find GPU pricing")
	}
	if price.Price != newPrice {
		t.Errorf("Expected GPU price to be %f, got %f", newPrice, price.Price)
	}
}

// testSetNodePricePerLocalDiskGiBHour tests the SetNodePricePerLocalDiskGiBHour function
func testSetNodePricePerLocalDiskGiBHour(t *testing.T, ctx context.Context, pm *PricingModule) {
	newPrice := 0.0007

	err := pm.SetNodePricePerLocalDiskGiBHour(ctx, newPrice)
	if err != nil {
		t.Fatalf("Failed to set local disk price: %v", err)
	}

	// Verify the price was set
	np, err := pm.getNodePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get node pricing: %v", err)
	}

	price, ok := np.Prices[pricing.ResourceStorage]
	if !ok {
		t.Fatal("Expected to find local disk pricing")
	}
	if price.Price != newPrice {
		t.Errorf("Expected local disk price to be %f, got %f", newPrice, price.Price)
	}
}

// testSetVolumePricePerStorageGiBHour tests the SetVolumePricePerStorageGiBHour function
func testSetVolumePricePerStorageGiBHour(t *testing.T, ctx context.Context, pm *PricingModule) {
	newPrice := 0.0003

	err := pm.SetVolumePricePerStorageGiBHour(ctx, newPrice)
	if err != nil {
		t.Fatalf("Failed to set volume storage price: %v", err)
	}

	// Verify the price was set
	vp, err := pm.getPersistentVolumePricing(ctx)
	if err != nil {
		t.Fatalf("Failed to get volume pricing: %v", err)
	}

	price, ok := vp.Prices[pricing.ResourceStorage]
	if !ok {
		t.Fatal("Expected to find volume storage pricing")
	}
	if price.Price != newPrice {
		t.Errorf("Expected volume storage price to be %f, got %f", newPrice, price.Price)
	}
}

// testNewNodePricingReader tests the NewNodePricingReader function
func testNewNodePricingReader(t *testing.T, ctx context.Context, pm *PricingModule) {
	// Test that NewNodePricingReader always produces a reader
	rdr, err := pm.NewNodePricingReader(ctx)
	if err != nil {
		t.Fatalf("Failed to create node pricing reader: %v", err)
	}

	if rdr == nil {
		t.Fatal("Expected reader to be non-nil")
	}

	// Test that the reader produces precisely one *NodePricing struct
	dst := make([]*pricing.NodePricing, 10) // Buffer larger than expected
	count := 0

	for {
		n, err := rdr.Read(ctx, dst)
		count += n

		// Verify all read items are non-nil
		for i := 0; i < n; i++ {
			if dst[i] == nil {
				t.Error("Expected non-nil NodePricing")
			}
		}

		if err == reader.Done {
			break
		}
		if err != nil {
			t.Fatalf("Reader error: %v", err)
		}
	}

	if count != 1 {
		t.Errorf("Expected reader to produce exactly 1 NodePricing, got %d", count)
	}

	// Clean up
	if err := rdr.Close(); err != nil {
		t.Errorf("Failed to close reader: %v", err)
	}
}

// testNewVolumePricingReader tests the NewVolumePricingReader function
func testNewVolumePricingReader(t *testing.T, ctx context.Context, pm *PricingModule) {
	// Test that NewVolumePricingReader always produces a reader
	rdr, err := pm.NewPersistentVolumePricingReader(ctx)
	if err != nil {
		t.Fatalf("Failed to create volume pricing reader: %v", err)
	}

	if rdr == nil {
		t.Fatal("Expected reader to be non-nil")
	}

	// Test that the reader produces precisely one *VolumePricing struct
	dst := make([]*pricing.PersistentVolumePricing, 10) // Buffer larger than expected
	count := 0

	for {
		n, err := rdr.Read(ctx, dst)
		count += n

		// Verify all read items are non-nil
		for i := 0; i < n; i++ {
			if dst[i] == nil {
				t.Error("Expected non-nil VolumePricing")
			}
		}

		if err == reader.Done {
			break
		}
		if err != nil {
			t.Fatalf("Reader error: %v", err)
		}
	}

	if count != 1 {
		t.Errorf("Expected reader to produce exactly 1 VolumePricing, got %d", count)
	}

	// Clean up
	if err := rdr.Close(); err != nil {
		t.Errorf("Failed to close reader: %v", err)
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
