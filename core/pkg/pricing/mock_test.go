package pricing

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/model/shared"
	"github.com/opencost/opencost/core/pkg/reader"
)

func TestMockPricingModule(t *testing.T) {
	var source PricingSource

	pricingModule, err := NewMockPricingModule()
	if err != nil {
		t.Fatalf("unexpected error initializing mock repository: %s", err)
	}

	source = pricingModule

	// Simple example of a sink for pricing data (will be database tables in reality)
	bufferSize := 10
	ingestor := newMockIngestor(bufferSize)

	// Test ingestion of mock node reader

	nodePricingReader, err := source.NewNodePricingReader(t.Context())
	if err != nil {
		t.Errorf("unexpected error initializing node reader: %s", err)
	}

	n, err := ingestor.ingestNodePricing(context.Background(), nodePricingReader)
	if err != nil {
		t.Errorf("unexpected error ingesting node pricing: %s", err)
	}
	if n != 39 {
		t.Errorf("expected to ingest %d node pricing records; ingested %d", 39, n)
	}

	nodePricingCount := ingestor.countNodePricing()
	if nodePricingCount != 39 {
		t.Errorf("expected %d node pricing records; received %d", 39, nodePricingCount)
	}

	// Test ingestion of mock persistent volume reader

	volumePricingReader, err := source.NewPersistentVolumePricingReader(t.Context())
	if err != nil {
		t.Errorf("unexpected error initializing volume reader: %s", err)
	}

	n, err = ingestor.ingestPersistentVolumePricing(context.Background(), volumePricingReader)
	if err != nil {
		t.Errorf("unexpected error ingesting volume pricing: %s", err)
	}
	if n != 20 {
		t.Errorf("expected to ingest %d volume pricing records; ingested %d", 20, n)
	}

	volumePricingCount := ingestor.countVolumePricing()
	if volumePricingCount != 20 {
		t.Errorf("expected %d volume pricing records; received %d", 20, volumePricingCount)
	}
}

// TestMockGetNodePricing verifies node lookup by properties, that the matching
// entry carries the prices loaded from YAML, and that a missing entry errors.
func TestMockGetNodePricing(t *testing.T) {
	mpm := newMock(t)

	np, err := mpm.GetNodePricing(t.Context(), NodePricingProperties{
		Provider:     shared.ProviderAWS,
		Region:       "us-east-1",
		InstanceType: "m5.large",
		Provisioning: ProvisioningOnDemand,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Guards against the YAML tag regression: prices must actually load.
	price, ok := np.Prices[ResourceNode]
	if !ok {
		t.Fatalf("expected node price to be present, prices=%v", np.Prices)
	}
	if price.Price != 0.096 {
		t.Errorf("expected on-demand price 0.096, got %v", price.Price)
	}

	// Missing entry should error rather than return a zero value.
	if _, err := mpm.GetNodePricing(t.Context(), NodePricingProperties{
		Provider:     shared.ProviderAWS,
		Region:       "eu-west-1",
		InstanceType: "m5.large",
		Provisioning: ProvisioningOnDemand,
	}); err == nil {
		t.Errorf("expected error for unknown region, got nil")
	}
}

// TestMockGetNodePricingProvisioningDiscriminates verifies that on-demand and
// spot entries with otherwise identical properties are not conflated.
func TestMockGetNodePricingProvisioningDiscriminates(t *testing.T) {
	mpm := newMock(t)

	base := NodePricingProperties{
		Provider:     shared.ProviderAWS,
		Region:       "us-east-1",
		InstanceType: "m5.large",
	}

	onDemand := base
	onDemand.Provisioning = ProvisioningOnDemand
	spot := base
	spot.Provisioning = ProvisioningSpot

	od, err := mpm.GetNodePricing(t.Context(), onDemand)
	if err != nil {
		t.Fatalf("unexpected error (on-demand): %v", err)
	}
	sp, err := mpm.GetNodePricing(t.Context(), spot)
	if err != nil {
		t.Fatalf("unexpected error (spot): %v", err)
	}

	if od.Prices[ResourceNode].Price == sp.Prices[ResourceNode].Price {
		t.Errorf("expected on-demand and spot to differ, both = %v", od.Prices[ResourceNode].Price)
	}
	if od.Prices[ResourceNode].Price != 0.096 {
		t.Errorf("expected on-demand 0.096, got %v", od.Prices[ResourceNode].Price)
	}
	if sp.Prices[ResourceNode].Price != 0.043 {
		t.Errorf("expected spot 0.043, got %v", sp.Prices[ResourceNode].Price)
	}
}

// TestMockGetPersistentVolumePricing verifies volume lookup, that prices load,
// and that a missing entry errors.
func TestMockGetPersistentVolumePricing(t *testing.T) {
	mpm := newMock(t)

	pv, err := mpm.GetPersistentVolumePricing(t.Context(), PersistentVolumePricingProperties{
		Provider:   shared.ProviderAWS,
		Region:     "us-east-1",
		VolumeType: VolumeTypeGP3,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if price, ok := pv.Prices[ResourceStorage]; !ok || price.Price != 0.0001096 {
		t.Errorf("expected gp3 storage price 0.0001096, got %v (ok=%t)", pv.Prices[ResourceStorage].Price, ok)
	}

	if _, err := mpm.GetPersistentVolumePricing(t.Context(), PersistentVolumePricingProperties{
		Provider:   shared.ProviderAWS,
		Region:     "us-east-1",
		VolumeType: VolumeTypeIO2,
	}); err == nil {
		t.Errorf("expected error for unknown volume type, got nil")
	}
}

// TestMockGetClusterPricing verifies cluster lookup by provider.
func TestMockGetClusterPricing(t *testing.T) {
	mpm := newMock(t)

	cp, err := mpm.GetClusterPricing(t.Context(), ClusterPricingProperties{Provider: shared.ProviderAWS})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if price, ok := cp.Prices[ResourceCluster]; !ok || price.Price != 0.10 {
		t.Errorf("expected cluster price 0.10, got %v (ok=%t)", cp.Prices[ResourceCluster].Price, ok)
	}

	if _, err := mpm.GetClusterPricing(t.Context(), ClusterPricingProperties{Provider: shared.ProviderOracle}); err == nil {
		t.Errorf("expected error for unknown provider, got nil")
	}
}

// TestMockGetNetworkPricing verifies network lookup by provider and that the
// per-egress-type resource prices load from YAML.
func TestMockGetNetworkPricing(t *testing.T) {
	mpm := newMock(t)

	np, err := mpm.GetNetworkPricing(t.Context(), NetworkPricingProperties{
		Provider: shared.ProviderAWS,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	internet, ok := np.Prices[ResourceInternetEgress]
	if !ok || internet.Price != 0.09 {
		t.Errorf("expected internet egress price 0.09, got %v (ok=%t)", internet.Price, ok)
	}

	nat, ok := np.Prices[ResourceNATGatewayEgress]
	if !ok || nat.Price != 0.045 {
		t.Errorf("expected NAT gateway egress price 0.045, got %v (ok=%t)", nat.Price, ok)
	}

	if internet.Price == nat.Price {
		t.Errorf("expected internet egress and NAT gateway egress prices to differ")
	}

	// Missing provider should error rather than return a zero value.
	if _, err := mpm.GetNetworkPricing(t.Context(), NetworkPricingProperties{
		Provider: shared.ProviderOracle,
	}); err == nil {
		t.Errorf("expected error for unknown provider, got nil")
	}
}

// TestMockGetServicePricing verifies service lookup by provider and region.
func TestMockGetServicePricing(t *testing.T) {
	mpm := newMock(t)

	sp, err := mpm.GetServicePricing(t.Context(), ServicePricingProperties{
		Provider: shared.ProviderAWS,
		Region:   "us-east-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if price, ok := sp.Prices[ResourceService]; !ok || price.Price != 0.025 {
		t.Errorf("expected service price 0.025, got %v (ok=%t)", sp.Prices[ResourceService].Price, ok)
	}

	if _, err := mpm.GetServicePricing(t.Context(), ServicePricingProperties{
		Provider: shared.ProviderAWS,
		Region:   "us-west-2",
	}); err == nil {
		t.Errorf("expected error for unknown region, got nil")
	}
}

// newMock is a helper that constructs a fresh MockPricingModule and fails the
// test if construction errors.
func newMock(t *testing.T) *MockPricingModule {
	t.Helper()
	mpm, err := NewMockPricingModule()
	if err != nil {
		t.Fatalf("unexpected error initializing mock pricing module: %v", err)
	}
	return mpm
}

type mockPricingIngestor struct {
	bufferSize              int
	clusterPricing          []*ClusterPricing
	networkPricing          []*NetworkPricing
	nodePricing             []*NodePricing
	persistentVolumePricing []*PersistentVolumePricing
	servicePricing          []*ServicePricing
}

func newMockIngestor(bufferSize int) *mockPricingIngestor {
	if bufferSize == 0 {
		bufferSize = 100
	}

	return &mockPricingIngestor{
		bufferSize:              bufferSize,
		clusterPricing:          []*ClusterPricing{},
		networkPricing:          []*NetworkPricing{},
		nodePricing:             []*NodePricing{},
		persistentVolumePricing: []*PersistentVolumePricing{},
		servicePricing:          []*ServicePricing{},
	}
}

func (ing *mockPricingIngestor) countNodePricing() int {
	return len(ing.nodePricing)
}

func (ing *mockPricingIngestor) ingestNodePricing(ctx context.Context, pricingReader reader.Reader[*NodePricing]) (int, error) {
	defer pricingReader.Close()

	nodeBuf := make([]*NodePricing, ing.bufferSize)

	totalCount := 0

	for {
		n, err := pricingReader.Read(ctx, nodeBuf)

		if n > 0 {
			ing.nodePricing = append(ing.nodePricing, nodeBuf[:n]...)
		}

		if errors.Is(err, reader.Done) {
			break
		}

		if err != nil {
			return totalCount, fmt.Errorf("unexpected error reading node pricing: %s", err)
		}

		totalCount += n
	}

	return totalCount, nil
}

func (ing *mockPricingIngestor) countVolumePricing() int {
	return len(ing.persistentVolumePricing)
}

func (ing *mockPricingIngestor) ingestPersistentVolumePricing(ctx context.Context, pricingReader reader.Reader[*PersistentVolumePricing]) (int, error) {
	defer pricingReader.Close()

	volBuf := make([]*PersistentVolumePricing, ing.bufferSize)

	totalCount := 0

	for {
		n, err := pricingReader.Read(ctx, volBuf)

		if n > 0 {
			ing.persistentVolumePricing = append(ing.persistentVolumePricing, volBuf[:n]...)
		}

		if errors.Is(err, reader.Done) {
			break
		}

		if err != nil {
			return totalCount, fmt.Errorf("unexpected error reading volume pricing: %s", err)
		}

		totalCount += n
	}

	return totalCount, nil
}
