package pricing

import (
	"sync"
	"testing"

	"github.com/opencost/opencost/core/pkg/unit"
)

// TestMemoryGetEmpty verifies that a fresh store returns a non-nil, empty set.
func TestMemoryGetEmpty(t *testing.T) {
	mps := NewMemoryPricingStore()

	ps, err := mps.GetPricingSet(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps == nil {
		t.Fatal("expected non-nil pricing set")
	}
	if !ps.IsEmpty() {
		t.Errorf("expected empty pricing set, got %+v", ps)
	}
}

// TestMemoryRoundTrip verifies that a set survives a Set/Get round trip while
// being returned as a distinct copy, not the stored pointer.
func TestMemoryRoundTrip(t *testing.T) {
	mps := NewMemoryPricingStore()
	in := fullPricingSet()

	if err := mps.SetPricingSet(t.Context(), in); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out, err := mps.GetPricingSet(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	csIn, err := in.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	csOut, err := out.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if csIn != csOut {
		t.Errorf("round-trip changed contents: %q -> %q", csIn, csOut)
	}

	if out == in {
		t.Error("expected Get to return a copy, not the set pointer")
	}
}

// TestMemorySetIsolation verifies that mutating the value passed to Set after
// the call does not affect what the store returns.
func TestMemorySetIsolation(t *testing.T) {
	mps := NewMemoryPricingStore()
	in := fullPricingSet()

	if err := mps.SetPricingSet(t.Context(), in); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	before, err := mps.GetPricingSet(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	csBefore, err := before.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Mutate the caller's copy after setting it.
	in.NodePricing[0].Properties.Labels["team"] = "mutated"
	in.NodePricing[0].Prices[ResourceNode] = Price{Unit: unit.Hour, Price: 99}
	in.NodePricing = append(in.NodePricing, nodePricing("m5.xlarge", 0.192))

	after, err := mps.GetPricingSet(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	csAfter, err := after.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if csBefore != csAfter {
		t.Errorf("mutating the set argument changed store contents: %q -> %q", csBefore, csAfter)
	}
}

// TestMemoryGetIsolation verifies that mutating a value returned by Get does not
// affect what subsequent Get calls return.
func TestMemoryGetIsolation(t *testing.T) {
	mps := NewMemoryPricingStore()
	if err := mps.SetPricingSet(t.Context(), fullPricingSet()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := mps.GetPricingSet(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	csBefore, err := got.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Mutate the returned copy.
	got.NodePricing[0].Properties.Labels["team"] = "mutated"
	got.NodePricing[0].Prices[ResourceNode] = Price{Unit: unit.Hour, Price: 99}

	again, err := mps.GetPricingSet(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	csAfter, err := again.Checksum()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if csBefore != csAfter {
		t.Errorf("mutating a returned set changed store contents: %q -> %q", csBefore, csAfter)
	}
}

// TestMemorySetNil verifies that Set rejects a nil pricing set.
func TestMemorySetNil(t *testing.T) {
	mps := NewMemoryPricingStore()
	if err := mps.SetPricingSet(t.Context(), nil); err == nil {
		t.Error("expected error for nil pricing set")
	}
}

// TestMemoryConcurrentAccess exercises the store under concurrent readers and
// writers; run with -race to catch data races.
func TestMemoryConcurrentAccess(t *testing.T) {
	mps := NewMemoryPricingStore()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = mps.SetPricingSet(t.Context(), fullPricingSet())
		}()
		go func() {
			defer wg.Done()
			if _, err := mps.GetPricingSet(t.Context()); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
}
