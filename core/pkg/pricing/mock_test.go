package pricing

import (
	"errors"
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
	nodePricing := []*NodePricing{}
	volumePricing := []*VolumePricing{}

	bufferSize := 100

	// Test mock node reader

	nodeReader, err := repo.NewNodePricingReader(t.Context())
	if err != nil {
		t.Errorf("unexpected error initializing node reader: %s", err)
	}
	defer nodeReader.Close()

	nodeBuf := make([]*NodePricing, bufferSize)

	for {
		n, err := nodeReader.Read(t.Context(), nodeBuf)

		if n > 0 {
			nodePricing = append(nodePricing, nodeBuf[:n]...)
		}

		if errors.Is(err, reader.Done) {
			break
		}

		if err != nil {
			t.Errorf("unexpected error reading node pricing: %s", err)
		}
	}

	if len(nodePricing) != 12 {
		t.Errorf("expected %d node pricing records; received %d", 12, len(nodePricing))
	}

	// Test mock volume reader

	volumeReader, err := repo.NewVolumePricingReader(t.Context())
	if err != nil {
		t.Errorf("unexpected error initializing volume reader: %s", err)
	}
	defer volumeReader.Close()

	volumeBuf := make([]*VolumePricing, bufferSize)

	for {
		n, err := volumeReader.Read(t.Context(), volumeBuf)

		if n > 0 {
			volumePricing = append(volumePricing, volumeBuf[:n]...)
		}

		if errors.Is(err, reader.Done) {
			break
		}

		if err != nil {
			t.Errorf("unexpected error reading volume pricing: %s", err)
		}
	}

	if len(volumePricing) != 6 {
		t.Errorf("expected %d volume pricing records; received %d", 6, len(volumePricing))
	}
}
