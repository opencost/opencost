package aws

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockSpotPriceHistoryFetcher struct {
	fetchFunc func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error)
}

func (m *mockSpotPriceHistoryFetcher) FetchSpotPrice(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
	if m.fetchFunc != nil {
		return m.fetchFunc(key)
	}
	return &SpotPriceHistoryEntry{
		SpotPrice:   0.05,
		Timestamp:   time.Now(),
		RetrievedAt: time.Now(),
	}, nil
}

func TestSpotPriceHistoryCache_GetSpotPrice_CacheHit(t *testing.T) {
	mockFetcher := &mockSpotPriceHistoryFetcher{}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	region := "us-west-2"
	instanceType := "m5.large"
	availabilityZone := "us-west-2a"

	key := SpotPriceHistoryKey{
		Region:           region,
		InstanceType:     instanceType,
		AvailabilityZone: availabilityZone,
	}

	cachedEntry := &SpotPriceHistoryEntry{
		SpotPrice:   0.08,
		Timestamp:   time.Now(),
		RetrievedAt: time.Now(),
	}
	cache.cache[key] = cachedEntry

	entry, err := cache.GetSpotPrice(region, instanceType, availabilityZone)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if entry.SpotPrice != 0.08 {
		t.Errorf("Expected spot price 0.08, got %f", entry.SpotPrice)
	}
}

func TestSpotPriceHistoryCache_GetSpotPrice_CacheMiss(t *testing.T) {
	fetchCalled := false
	mockFetcher := &mockSpotPriceHistoryFetcher{
		fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
			fetchCalled = true
			return &SpotPriceHistoryEntry{
				SpotPrice:   0.12,
				Timestamp:   time.Now(),
				RetrievedAt: time.Now(),
			}, nil
		},
	}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	entry, err := cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !fetchCalled {
		t.Error("Expected fetcher to be called for cache miss")
	}
	if entry.SpotPrice != 0.12 {
		t.Errorf("Expected spot price 0.12, got %f", entry.SpotPrice)
	}
}

func TestSpotPriceHistoryCache_GetSpotPrice_ConcurrentSameKey(t *testing.T) {
	var fetchCount atomic.Int32
	mockFetcher := &mockSpotPriceHistoryFetcher{
		fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
			fetchCount.Add(1)
			// Simulate slow API call to increase chance of concurrent access
			time.Sleep(50 * time.Millisecond)
			return &SpotPriceHistoryEntry{
				SpotPrice:   0.07,
				Timestamp:   time.Now(),
				RetrievedAt: time.Now(),
			}, nil
		},
	}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			entry, err := cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
			if entry.SpotPrice != 0.07 {
				t.Errorf("Expected spot price 0.07, got %f", entry.SpotPrice)
			}
		}()
	}
	wg.Wait()

	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected exactly 1 fetch call, got %d", count)
	}
}

func TestSpotPriceHistoryCache_GetSpotPrice_StaleEntry(t *testing.T) {
	fetchCalled := false
	mockFetcher := &mockSpotPriceHistoryFetcher{
		fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
			fetchCalled = true
			return &SpotPriceHistoryEntry{
				SpotPrice:   0.15,
				Timestamp:   time.Now(),
				RetrievedAt: time.Now(),
			}, nil
		},
	}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	key := SpotPriceHistoryKey{
		Region:           "us-west-2",
		InstanceType:     "m5.large",
		AvailabilityZone: "us-west-2a",
	}

	staleEntry := &SpotPriceHistoryEntry{
		SpotPrice:   0.08,
		Timestamp:   time.Now(),
		RetrievedAt: time.Now().Add(-2 * time.Hour),
	}
	cache.cache[key] = staleEntry

	entry, err := cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !fetchCalled {
		t.Error("Expected fetcher to be called for stale entry")
	}
	if entry.SpotPrice != 0.15 {
		t.Errorf("Expected refreshed spot price 0.15, got %f", entry.SpotPrice)
	}
}

func TestSpotPriceHistoryCache_GetSpotPrice_FetchError(t *testing.T) {
	expectedError := errors.New("fetch failed")
	mockFetcher := &mockSpotPriceHistoryFetcher{
		fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
			return nil, expectedError
		},
	}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	_, err := cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err == nil {
		t.Error("Expected error from failed fetch")
	}
	if !errors.Is(err, expectedError) {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}

	key := SpotPriceHistoryKey{
		Region:           "us-west-2",
		InstanceType:     "m5.large",
		AvailabilityZone: "us-west-2a",
	}
	cachedEntry := cache.cache[key]
	if !errors.Is(cachedEntry.Error, expectedError) {
		t.Errorf("Expected cached entry error %v, got %v", expectedError, cachedEntry.Error)
	}
}

func TestSpotPriceHistoryEntry_shouldRefresh(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name        string
		retrievedAt time.Time
		expected    bool
	}{
		{
			name:        "fresh entry",
			retrievedAt: now,
			expected:    false,
		},
		{
			name:        "stale entry",
			retrievedAt: now.Add(-2 * time.Hour),
			expected:    true,
		},
		{
			name:        "borderline entry",
			retrievedAt: now.Add(-SpotPriceHistoryCacheAge + 1*time.Minute),
			expected:    false,
		},
		{
			name:        "expired entry",
			retrievedAt: now.Add(-SpotPriceHistoryCacheAge - 1*time.Minute),
			expected:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := SpotPriceHistoryEntry{
				RetrievedAt: tt.retrievedAt,
			}
			if got := entry.shouldRefresh(); got != tt.expected {
				t.Errorf("shouldRefresh() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestSpotPriceHistoryKey_String(t *testing.T) {
	key := SpotPriceHistoryKey{
		Region:           "us-west-2",
		InstanceType:     "m5.large",
		AvailabilityZone: "us-west-2a",
	}
	expected := "us-west-2/m5.large/us-west-2a"
	if got := key.String(); got != expected {
		t.Errorf("String() = %v, want %v", got, expected)
	}
}
