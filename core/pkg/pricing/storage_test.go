package pricing

import (
	"sync"
	"testing"

	"github.com/opencost/opencost/core/pkg/storage"
)

// TestStorageNewValidation verifies the constructor's argument validation and
// that a non-existent path is auto-initialized with an empty pricing set.
func TestStorageNewValidation(t *testing.T) {
	if _, err := NewStoragePricingStore(t.Context(), nil, "pricing.json"); err == nil {
		t.Error("expected error for nil storage")
	}

	if _, err := NewStoragePricingStore(t.Context(), storage.NewMemoryStorage(), ""); err == nil {
		t.Error("expected error for empty path")
	}

	store := storage.NewMemoryStorage()
	sps, err := NewStoragePricingStore(t.Context(), store, "pricing.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	exists, err := store.Exists("pricing.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Error("expected constructor to initialize the pricing path")
	}

	ps, err := sps.GetPricingSet(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ps.IsEmpty() {
		t.Errorf("expected auto-initialized set to be empty, got %+v", ps)
	}
}

// TestStorageRoundTrip verifies that a set survives a Set/Get round trip through
// the backing storage.
func TestStorageRoundTrip(t *testing.T) {
	sps, err := NewStoragePricingStore(t.Context(), storage.NewMemoryStorage(), "pricing.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	in := fullPricingSet()
	if err := sps.SetPricingSet(t.Context(), in); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out, err := sps.GetPricingSet(t.Context())
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
}

// TestStorageSetNil verifies that Set rejects a nil pricing set.
func TestStorageSetNil(t *testing.T) {
	sps, err := NewStoragePricingStore(t.Context(), storage.NewMemoryStorage(), "pricing.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := sps.SetPricingSet(t.Context(), nil); err == nil {
		t.Error("expected error for nil pricing set")
	}
}

// TestStorageGetMissingPath verifies that Get surfaces a read error when the
// backing path does not exist. The store is constructed directly to bypass the
// constructor's auto-initialization.
func TestStorageGetMissingPath(t *testing.T) {
	sps := &StoragePricingStore{
		store: storage.NewMemoryStorage(),
		path:  "missing.json",
	}

	if _, err := sps.GetPricingSet(t.Context()); err == nil {
		t.Error("expected error reading a missing path")
	}
}

// TestStorageGetMalformedData verifies that Get surfaces a decode error when the
// backing data is not valid JSON.
func TestStorageGetMalformedData(t *testing.T) {
	store := storage.NewMemoryStorage()
	if err := store.Write("pricing.json", []byte("not json")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sps := &StoragePricingStore{store: store, path: "pricing.json"}

	if _, err := sps.GetPricingSet(t.Context()); err == nil {
		t.Error("expected error decoding malformed pricing data")
	}
}

// TestStorageConcurrentAccess exercises the store under concurrent readers and
// writers; run with -race to catch data races.
func TestStorageConcurrentAccess(t *testing.T) {
	sps, err := NewStoragePricingStore(t.Context(), storage.NewMemoryStorage(), "pricing.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = sps.SetPricingSet(t.Context(), fullPricingSet())
		}()
		go func() {
			defer wg.Done()
			if _, err := sps.GetPricingSet(t.Context()); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
}
