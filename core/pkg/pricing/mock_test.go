package pricing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

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
			totalCount += n
		}

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return totalCount, fmt.Errorf("unexpected error reading node pricing: %s", err)
		}
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
			totalCount += n
		}

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return totalCount, fmt.Errorf("unexpected error reading volume pricing: %s", err)
		}
	}

	return totalCount, nil
}
