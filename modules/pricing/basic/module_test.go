package basic

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/unit"
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

		t.Run("Metadata", func(t *testing.T) {
			testMetadata(t, pm)
		})

		t.Run("GetPricingSet", func(t *testing.T) {
			testGetPricingSet(t, ctx, pm)
		})

		t.Run("PublicGetters", func(t *testing.T) {
			testPublicGetters(t, ctx, pm)
		})

		t.Run("SetClusterPricePerHour", func(t *testing.T) {
			testSetClusterPricePerHour(t, ctx, pm)
		})

		t.Run("SetNetworkPrices", func(t *testing.T) {
			testSetNetworkPrices(t, ctx, pm)
		})

		t.Run("SetServicePricePerHour", func(t *testing.T) {
			testSetServicePricePerHour(t, ctx, pm)
		})

		t.Run("Checksum", func(t *testing.T) {
			testChecksum(t, ctx, pm)
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

		t.Run("NewClusterPricingReader", func(t *testing.T) {
			testNewClusterPricingReader(t, ctx, pm)
		})

		t.Run("NewNetworkPricingReader", func(t *testing.T) {
			testNewNetworkPricingReader(t, ctx, pm)
		})

		t.Run("NewServicePricingReader", func(t *testing.T) {
			testNewServicePricingReader(t, ctx, pm)
		})

		t.Run("ModulePersistence", func(t *testing.T) {
			// Create a new PricingModule with the same store
			pm2, err := NewBasicPricingModule(store)
			require.NoError(t, err)

			// Verify that pricing persists across all resource kinds.
			np, err := pm2.getNodePricing(ctx)
			require.NoError(t, err, "getting node pricing")
			require.NotNil(t, np, "expected node pricing to be persisted")

			cp, err := pm2.getClusterPricing(ctx)
			require.NoError(t, err, "getting cluster pricing")
			require.NotNil(t, cp, "expected cluster pricing to be persisted")

			netp, err := pm2.getNetworkPricing(ctx)
			require.NoError(t, err, "getting network pricing")
			require.NotNil(t, netp, "expected network pricing to be persisted")

			vp, err := pm2.getPersistentVolumePricing(ctx)
			require.NoError(t, err, "getting volume pricing")
			require.NotNil(t, vp, "expected volume pricing to be persisted")

			sp, err := pm2.getServicePricing(ctx)
			require.NoError(t, err, "getting service pricing")
			require.NotNil(t, sp, "expected service pricing to be persisted")
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

	// Test default cluster pricing
	cp, err := pm.getClusterPricing(ctx)
	require.NoError(t, err, "getting cluster pricing")
	require.NotNil(t, cp, "expected cluster pricing to exist")

	clusterPrice, ok := cp.Prices[pricing.ResourceCluster]
	require.True(t, ok, "expected to find cluster pricing")
	require.Equal(t, DefaultClusterPricePerHour, clusterPrice.Price)
	require.Equal(t, unit.Hour, clusterPrice.Unit)

	// Test default network pricing. RAM, egress types, etc. all share the GiB
	// unit, so the Resource key distinguishes them.
	netp, err := pm.getNetworkPricing(ctx)
	require.NoError(t, err, "getting network pricing")
	require.NotNil(t, netp, "expected network pricing to exist")

	networkChecks := []struct {
		resource pricing.Resource
		want     float64
	}{
		{pricing.ResourceLocalEgress, DefaultNetworkLocalEgressPricePerGiB},
		{pricing.ResourceCrossZoneEgress, DefaultNetworkCrossZoneEgressPricePerGiB},
		{pricing.ResourceCrossRegionEgress, DefaultNetworkCrossRegionEgressPricePerGiB},
		{pricing.ResourceInternetEgress, DefaultNetworkInternetEgressPricePerGiB},
		{pricing.ResourceNATGatewayEgress, DefaultNetworkNATGatewayEgressPricePerGiB},
		{pricing.ResourceNATGatewayIngress, DefaultNetworkNATGatewayIngressPricePerGiB},
	}
	for _, c := range networkChecks {
		price, ok := netp.Prices[c.resource]
		require.Truef(t, ok, "expected to find %s pricing", c.resource)
		require.Equalf(t, c.want, price.Price, "price for %s", c.resource)
		require.Equalf(t, unit.GiB, price.Unit, "unit for %s", c.resource)
	}

	// Test default service pricing
	sp, err := pm.getServicePricing(ctx)
	require.NoError(t, err, "getting service pricing")
	require.NotNil(t, sp, "expected service pricing to exist")

	servicePrice, ok := sp.Prices[pricing.ResourceService]
	require.True(t, ok, "expected to find service pricing")
	require.Equal(t, DefaultServicePricePerHour, servicePrice.Price)
	require.Equal(t, unit.Hour, servicePrice.Unit)
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

	err := pm.SetPersistentVolumePricePerStorageGiBHour(ctx, newPrice)
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

		if errors.Is(err, io.EOF) {
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

		if errors.Is(err, io.EOF) {
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

// testMetadata verifies the source identity accessors.
func testMetadata(t *testing.T, pm *PricingModule) {
	require.Equal(t, SourceKind, pm.SourceKind())
	require.Equal(t, SourceName, pm.SourceName())
}

// testGetPricingSet verifies GetPricingSet returns a populated set.
func testGetPricingSet(t *testing.T, ctx context.Context, pm *PricingModule) {
	ps, err := pm.GetPricingSet(ctx)
	require.NoError(t, err)
	require.NotNil(t, ps)
	require.False(t, ps.IsEmpty())
}

// testPublicGetters exercises the public Get*Pricing wrappers, which apply a
// nil-check around the private getters.
func testPublicGetters(t *testing.T, ctx context.Context, pm *PricingModule) {
	cp, err := pm.GetClusterPricing(ctx, pricing.ClusterPricingProperties{})
	require.NoError(t, err)
	require.NotNil(t, cp)

	netp, err := pm.GetNetworkPricing(ctx, pricing.NetworkPricingProperties{})
	require.NoError(t, err)
	require.NotNil(t, netp)

	np, err := pm.GetNodePricing(ctx, pricing.NodePricingProperties{})
	require.NoError(t, err)
	require.NotNil(t, np)

	vp, err := pm.GetPersistentVolumePricing(ctx, pricing.PersistentVolumePricingProperties{})
	require.NoError(t, err)
	require.NotNil(t, vp)

	sp, err := pm.GetServicePricing(ctx, pricing.ServicePricingProperties{})
	require.NoError(t, err)
	require.NotNil(t, sp)
}

// testSetClusterPricePerHour tests the SetClusterPricePerHour function.
func testSetClusterPricePerHour(t *testing.T, ctx context.Context, pm *PricingModule) {
	newPrice := 1.25

	err := pm.SetClusterPricePerHour(ctx, newPrice)
	require.NoError(t, err)

	cp, err := pm.getClusterPricing(ctx)
	require.NoError(t, err)

	price, ok := cp.Prices[pricing.ResourceCluster]
	require.True(t, ok, "expected to find cluster pricing")
	require.Equal(t, newPrice, price.Price)
	require.Equal(t, unit.Hour, price.Unit)
}

// testSetNetworkPrices tests each network setter. Each case verifies that only
// the targeted Resource changes and that the other network prices are left
// intact, guarding against a setter writing the wrong Resource key.
func testSetNetworkPrices(t *testing.T, ctx context.Context, pm *PricingModule) {
	cases := []struct {
		name     string
		resource pricing.Resource
		newPrice float64
		set      func(context.Context, float64) error
	}{
		{"LocalEgress", pricing.ResourceLocalEgress, 0.002, pm.SetNetworkLocalEgressPricePerGiB},
		{"CrossZoneEgress", pricing.ResourceCrossZoneEgress, 0.012, pm.SetNetworkCrossZoneEgressPricePerGiB},
		{"CrossRegionEgress", pricing.ResourceCrossRegionEgress, 0.022, pm.SetNetworkCrossRegionEgressPricePerGiB},
		{"InternetEgress", pricing.ResourceInternetEgress, 0.111, pm.SetNetworkInternetEgressPricePerGiB},
		{"NATGatewayEgress", pricing.ResourceNATGatewayEgress, 0.055, pm.SetNetworkNATGatewayEgressPricePerGiB},
		{"NATGatewayIngress", pricing.ResourceNATGatewayIngress, 0.066, pm.SetNetworkNATGatewayIngressPricePerGiB},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Snapshot existing prices so we can verify non-targeted
			// resources are untouched.
			before, err := pm.getNetworkPricing(ctx)
			require.NoError(t, err)
			prev := make(map[pricing.Resource]float64, len(before.Prices))
			for r, p := range before.Prices {
				prev[r] = p.Price
			}

			require.NoError(t, c.set(ctx, c.newPrice))

			after, err := pm.getNetworkPricing(ctx)
			require.NoError(t, err)

			price, ok := after.Prices[c.resource]
			require.Truef(t, ok, "expected to find %s pricing", c.resource)
			require.Equal(t, c.newPrice, price.Price)
			require.Equal(t, unit.GiB, price.Unit)

			// All other network resources must be unchanged.
			for r, want := range prev {
				if r == c.resource {
					continue
				}
				require.Equalf(t, want, after.Prices[r].Price, "unexpected change to %s", r)
			}
		})
	}
}

// testSetServicePricePerHour tests the SetServicePricePerHour function.
func testSetServicePricePerHour(t *testing.T, ctx context.Context, pm *PricingModule) {
	newPrice := 0.05

	err := pm.SetServicePricePerHour(ctx, newPrice)
	require.NoError(t, err)

	sp, err := pm.getServicePricing(ctx)
	require.NoError(t, err)

	price, ok := sp.Prices[pricing.ResourceService]
	require.True(t, ok, "expected to find service pricing")
	require.Equal(t, newPrice, price.Price)
	require.Equal(t, unit.Hour, price.Unit)
}

// testChecksum verifies the checksum is non-empty, deterministic, and sensitive
// to pricing mutations.
func testChecksum(t *testing.T, ctx context.Context, pm *PricingModule) {
	sum1, err := pm.Checksum(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, sum1)

	// Deterministic: recomputing without mutation yields the same checksum.
	sum2, err := pm.Checksum(ctx)
	require.NoError(t, err)
	require.Equal(t, sum1, sum2)

	// Sensitive: mutating pricing changes the checksum.
	require.NoError(t, pm.SetServicePricePerHour(ctx, sum1Sentinel))

	sum3, err := pm.Checksum(ctx)
	require.NoError(t, err)
	require.NotEqual(t, sum1, sum3, "expected checksum to change after a price mutation")
}

// sum1Sentinel is an arbitrary price unlikely to match the existing service
// price, used to force a checksum change.
const sum1Sentinel = 0.0419

// testNewClusterPricingReader verifies the cluster pricing reader yields exactly
// one non-nil element.
func testNewClusterPricingReader(t *testing.T, ctx context.Context, pm *PricingModule) {
	rdr, err := pm.NewClusterPricingReader(ctx)
	require.NoError(t, err)
	require.NotNil(t, rdr)

	dst := make([]*pricing.ClusterPricing, 10)
	count := 0
	for {
		n, err := rdr.Read(ctx, dst)
		count += n
		for i := 0; i < n; i++ {
			require.NotNil(t, dst[i], "expected non-nil ClusterPricing")
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}
	require.Equal(t, 1, count, "expected exactly 1 ClusterPricing")
	require.NoError(t, rdr.Close())
}

// testNewNetworkPricingReader verifies the network pricing reader yields exactly
// one non-nil element.
func testNewNetworkPricingReader(t *testing.T, ctx context.Context, pm *PricingModule) {
	rdr, err := pm.NewNetworkPricingReader(ctx)
	require.NoError(t, err)
	require.NotNil(t, rdr)

	dst := make([]*pricing.NetworkPricing, 10)
	count := 0
	for {
		n, err := rdr.Read(ctx, dst)
		count += n
		for i := 0; i < n; i++ {
			require.NotNil(t, dst[i], "expected non-nil NetworkPricing")
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}
	require.Equal(t, 1, count, "expected exactly 1 NetworkPricing")
	require.NoError(t, rdr.Close())
}

// testNewServicePricingReader verifies the service pricing reader yields exactly
// one non-nil element.
func testNewServicePricingReader(t *testing.T, ctx context.Context, pm *PricingModule) {
	rdr, err := pm.NewServicePricingReader(ctx)
	require.NoError(t, err)
	require.NotNil(t, rdr)

	dst := make([]*pricing.ServicePricing, 10)
	count := 0
	for {
		n, err := rdr.Read(ctx, dst)
		count += n
		for i := 0; i < n; i++ {
			require.NotNil(t, dst[i], "expected non-nil ServicePricing")
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}
	require.Equal(t, 1, count, "expected exactly 1 ServicePricing")
	require.NoError(t, rdr.Close())
}

func newFileStorage(t *testing.T) storage.Storage {
	tempDir, err := os.MkdirTemp("", "pricing-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	return storage.NewFileStorage(tempDir)
}
