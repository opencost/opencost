package pricing

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/reader"
)

func TestMockPricingRepository(t *testing.T) {
	var repo PricingRepository

	mockRepo, err := NewMockPricingRepository()
	if err != nil {
		t.Fatalf("unexpected error initializing mock repository: %s", err)
	}

	repo = mockRepo

	// Simple example of a sink for pricing data (will be database tables in reality)
	bufferSize := 10
	ingestor := newMockIngestor(bufferSize)

	// Test ingestion of mock node reader

	nodePricingReader, err := repo.NewNodePricingReader(t.Context())
	if err != nil {
		t.Errorf("unexpected error initializing node reader: %s", err)
	}

	n, err := ingestor.IngestNodePricing(context.Background(), nodePricingReader)
	if err != nil {
		t.Errorf("unexpected error ingesting node pricing: %s", err)
	}
	if n != 39 {
		t.Errorf("expected to ingest %d node pricing records; ingested %d", 39, n)
	}

	nodePricingCount := ingestor.CountNodePricing()
	if nodePricingCount != 39 {
		t.Errorf("expected %d node pricing records; received %d", 39, nodePricingCount)
	}

	// Test ingestion of mock volume reader

	volumePricingReader, err := repo.NewVolumePricingReader(t.Context())
	if err != nil {
		t.Errorf("unexpected error initializing volume reader: %s", err)
	}

	n, err = ingestor.IngestVolumePricing(context.Background(), volumePricingReader)
	if err != nil {
		t.Errorf("unexpected error ingesting volume pricing: %s", err)
	}
	if n != 20 {
		t.Errorf("expected to ingest %d volume pricing records; ingested %d", 20, n)
	}

	volumePricingCount := ingestor.CountVolumePricing()
	if volumePricingCount != 20 {
		t.Errorf("expected %d volume pricing records; received %d", 20, volumePricingCount)
	}
}

type mockPricingIngestor struct {
	bufferSize    int
	nodePricing   []*NodePricing
	volumePricing []*VolumePricing
}

func newMockIngestor(bufferSize int) *mockPricingIngestor {
	if bufferSize == 0 {
		bufferSize = 100
	}

	return &mockPricingIngestor{
		bufferSize:    bufferSize,
		nodePricing:   []*NodePricing{},
		volumePricing: []*VolumePricing{},
	}
}

func (ing *mockPricingIngestor) CountNodePricing() int {
	return len(ing.nodePricing)
}

func (ing *mockPricingIngestor) IngestNodePricing(ctx context.Context, pricingReader reader.Reader[*NodePricing]) (int, error) {
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

func (ing *mockPricingIngestor) CountVolumePricing() int {
	return len(ing.volumePricing)
}

func (ing *mockPricingIngestor) IngestVolumePricing(ctx context.Context, pricingReader reader.Reader[*VolumePricing]) (int, error) {
	defer pricingReader.Close()

	volBuf := make([]*VolumePricing, ing.bufferSize)

	totalCount := 0

	for {
		n, err := pricingReader.Read(ctx, volBuf)

		if n > 0 {
			ing.volumePricing = append(ing.volumePricing, volBuf[:n]...)
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
