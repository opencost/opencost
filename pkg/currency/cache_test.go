package currency

import (
	"testing"
	"time"
)

func TestMemoryCache_SetAndGet(t *testing.T) {
	cache := newMemoryCache(1 * time.Hour)
	defer cache.stop()

	// Test setting and getting rates
	rates := &cachedRates{
		rates: map[string]float64{
			"EUR": 0.85,
			"GBP": 0.73,
		},
		baseCode:  "USD",
		fetchedAt: time.Now(),
	}

	cache.set("USD", rates)

	// Test successful get
	retrieved, found := cache.get("USD")
	if !found {
		t.Error("expected to find cached rates")
	}

	if retrieved.baseCode != "USD" {
		t.Errorf("expected base code USD, got %s", retrieved.baseCode)
	}

	if len(retrieved.rates) != 2 {
		t.Errorf("expected 2 rates, got %d", len(retrieved.rates))
	}

	// Test non-existent key
	_, found = cache.get("EUR")
	if found {
		t.Error("expected not to find rates for EUR")
	}
}

func TestMemoryCache_Expiration(t *testing.T) {
	// Use short TTL for testing
	cache := newMemoryCache(100 * time.Millisecond)
	defer cache.stop()

	rates := &cachedRates{
		rates: map[string]float64{
			"EUR": 0.85,
		},
		baseCode:  "USD",
		fetchedAt: time.Now(),
	}

	cache.set("USD", rates)

	// Should find it immediately
	_, found := cache.get("USD")
	if !found {
		t.Error("expected to find cached rates immediately")
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Should not find it after expiration
	_, found = cache.get("USD")
	if found {
		t.Error("expected rates to be expired")
	}
}

func TestMemoryCache_Clear(t *testing.T) {
	cache := newMemoryCache(1 * time.Hour)
	defer cache.stop()

	// Add multiple entries
	for _, base := range []string{"USD", "EUR", "GBP"} {
		rates := &cachedRates{
			rates:     map[string]float64{"TEST": 1.0},
			baseCode:  base,
			fetchedAt: time.Now(),
		}
		cache.set(base, rates)
	}

	// Verify all entries exist
	for _, base := range []string{"USD", "EUR", "GBP"} {
		_, found := cache.get(base)
		if !found {
			t.Errorf("expected to find rates for %s", base)
		}
	}

	// Clear cache
	cache.clear()

	// Verify all entries are gone
	for _, base := range []string{"USD", "EUR", "GBP"} {
		_, found := cache.get(base)
		if found {
			t.Errorf("expected not to find rates for %s after clear", base)
		}
	}
}

func TestMemoryCache_Stats(t *testing.T) {
	cache := newMemoryCache(1 * time.Hour)
	defer cache.stop()

	// Initially empty
	entries, _ := cache.stats()
	if entries != 0 {
		t.Errorf("expected 0 entries, got %d", entries)
	}

	// Add entries
	now := time.Now()
	for i, base := range []string{"USD", "EUR", "GBP"} {
		rates := &cachedRates{
			rates:     map[string]float64{"TEST": 1.0},
			baseCode:  base,
			fetchedAt: now.Add(time.Duration(i) * time.Minute),
		}
		cache.set(base, rates)
	}

	entries, oldest := cache.stats()
	if entries != 3 {
		t.Errorf("expected 3 entries, got %d", entries)
	}

	// The oldest should be the first one we added (USD)
	if !oldest.Equal(now) {
		t.Errorf("expected oldest entry to be %v, got %v", now, oldest)
	}
}

func TestMemoryCache_Cleanup(t *testing.T) {
	// Use very short TTL for testing
	cache := newMemoryCache(50 * time.Millisecond)
	defer cache.stop()

	// Add entry
	rates := &cachedRates{
		rates:     map[string]float64{"EUR": 0.85},
		baseCode:  "USD",
		fetchedAt: time.Now(),
	}
	cache.set("USD", rates)

	// Verify it exists
	entries, _ := cache.stats()
	if entries != 1 {
		t.Errorf("expected 1 entry, got %d", entries)
	}

	// Wait for cleanup cycle (janitor runs every TTL/2 = 25ms)
	// Wait a bit longer to ensure cleanup has run
	time.Sleep(100 * time.Millisecond)

	// Verify it's been cleaned up
	entries, _ = cache.stats()
	if entries != 0 {
		t.Errorf("expected 0 entries after cleanup, got %d", entries)
	}
}
