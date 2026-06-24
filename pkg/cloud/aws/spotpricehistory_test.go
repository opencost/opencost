package aws

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockSpotPriceHistoryFetcher struct {
	fetchFunc func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error)
}

func (m *mockSpotPriceHistoryFetcher) FetchSpotPrice(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
	return m.fetchFunc(key)
}

func TestSpotPriceHistoryCache_GetSpotPrice_CacheHit(t *testing.T) {
	var fetchCount atomic.Int32
	mockFetcher := &mockSpotPriceHistoryFetcher{
		fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
			fetchCount.Add(1)
			return &SpotPriceHistoryEntry{
				SpotPrice: 0.05,
				Timestamp: time.Now(),
			}, nil
		},
	}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	// First call should fetch
	entry, err := cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if entry.SpotPrice != 0.05 {
		t.Errorf("Expected spot price 0.05, got %f", entry.SpotPrice)
	}
	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected 1 fetch call, got %d", count)
	}

	// Second call should use cache
	entry, err = cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if entry.SpotPrice != 0.05 {
		t.Errorf("Expected spot price 0.05, got %f", entry.SpotPrice)
	}
	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected still 1 fetch call (cached), got %d", count)
	}
}

func TestSpotPriceHistoryCache_GetSpotPrice_CacheMiss(t *testing.T) {
	var fetchCount atomic.Int32
	mockFetcher := &mockSpotPriceHistoryFetcher{
		fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
			fetchCount.Add(1)
			return &SpotPriceHistoryEntry{
				SpotPrice: 0.05,
				Timestamp: time.Now(),
			}, nil
		},
	}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	// Different keys should each fetch
	_, err := cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	_, err = cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2b")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if count := fetchCount.Load(); count != 2 {
		t.Errorf("Expected 2 fetch calls, got %d", count)
	}
}

func TestSpotPriceHistoryCache_GetSpotPrice_ConcurrentSameKey(t *testing.T) {
	var fetchCount atomic.Int32
	mockFetcher := &mockSpotPriceHistoryFetcher{
		fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
			fetchCount.Add(1)
			time.Sleep(50 * time.Millisecond) // Simulate slow fetch
			return &SpotPriceHistoryEntry{
				SpotPrice: 0.05,
				Timestamp: time.Now(),
			}, nil
		},
	}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	// Launch multiple concurrent requests for the same key
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	// Should only fetch once despite concurrent requests
	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected 1 fetch call, got %d", count)
	}
}

func TestSpotPriceHistoryCache_GetSpotPrice_StaleEntry(t *testing.T) {
	var fetchCount atomic.Int32
	mockFetcher := &mockSpotPriceHistoryFetcher{
		fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
			fetchCount.Add(1)
			return &SpotPriceHistoryEntry{
				SpotPrice: 0.05,
				Timestamp: time.Now(),
			}, nil
		},
	}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	// First call
	_, err := cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Manually make the entry stale
	key := SpotPriceHistoryKey{
		Region:           "us-west-2",
		InstanceType:     "m5.large",
		AvailabilityZone: "us-west-2a",
	}
	cache.mutex.Lock()
	cache.cache[key].RetrievedAt = time.Now().Add(-2 * time.Hour)
	cache.mutex.Unlock()

	// Second call should refresh
	_, err = cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if count := fetchCount.Load(); count != 2 {
		t.Errorf("Expected 2 fetch calls, got %d", count)
	}
}

func TestSpotPriceHistoryCache_GetSpotPrice_FetchError(t *testing.T) {
	var fetchCount atomic.Int32
	mockFetcher := &mockSpotPriceHistoryFetcher{
		fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
			fetchCount.Add(1)
			return nil, errors.New("network error")
		},
	}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	// First call should fetch and cache error
	_, err := cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err == nil {
		t.Error("Expected error")
	}
	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected 1 fetch call, got %d", count)
	}

	// Second call should return cached error
	_, err = cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err == nil {
		t.Error("Expected cached error")
	}
	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected still 1 fetch call (cached), got %d", count)
	}
}

func TestSpotPriceHistoryEntry_shouldRefresh(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name        string
		retrievedAt time.Time
		err         error
		expected    bool
	}{
		{
			name:        "fresh entry",
			retrievedAt: now,
			err:         nil,
			expected:    false,
		},
		{
			name:        "stale entry",
			retrievedAt: now.Add(-2 * time.Hour),
			err:         nil,
			expected:    true,
		},
		{
			name:        "borderline entry",
			retrievedAt: now.Add(-SpotPriceHistoryCacheAge + 1*time.Minute),
			err:         nil,
			expected:    false,
		},
		{
			name:        "expired entry",
			retrievedAt: now.Add(-SpotPriceHistoryCacheAge - 1*time.Minute),
			err:         nil,
			expected:    true,
		},
		{
			name:        "auth error - never refresh",
			retrievedAt: now.Add(-2 * time.Hour),
			err:         fmt.Errorf("%w", ErrSpotPriceAuthFailure),
			expected:    false,
		},
		{
			name:        "wrapped auth error - never refresh",
			retrievedAt: now.Add(-2 * time.Hour),
			err:         fmt.Errorf("additional context: %w", ErrSpotPriceAuthFailure),
			expected:    false,
		},
		{
			name:        "transient error - should refresh when stale",
			retrievedAt: now.Add(-2 * time.Hour),
			err:         errors.New("network timeout"),
			expected:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := SpotPriceHistoryEntry{
				RetrievedAt: tt.retrievedAt,
				Error:       tt.err,
			}
			if got := entry.shouldRefresh(); got != tt.expected {
				t.Errorf("shouldRefresh() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestSpotPriceHistoryCache_GetSpotPrice_AuthErrorCached(t *testing.T) {
	// Reset global flag before test
	globalSpotPriceAuthFailure.Store(false)

	var fetchCount atomic.Int32
	mockFetcher := &mockSpotPriceHistoryFetcher{
		fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
			fetchCount.Add(1)
			return nil, fmt.Errorf("%w", ErrSpotPriceAuthFailure)
		},
	}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	// First call should fetch and cache the auth error
	_, err := cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err == nil {
		t.Error("Expected auth error")
	}
	if !errors.Is(err, ErrSpotPriceAuthFailure) {
		t.Errorf("Expected ErrSpotPriceAuthFailure, got %v", err)
	}
	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected 1 fetch call, got %d", count)
	}

	// Second call should return cached auth error without fetching
	_, err = cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err == nil {
		t.Error("Expected cached auth error")
	}
	if !errors.Is(err, ErrSpotPriceAuthFailure) {
		t.Errorf("Expected ErrSpotPriceAuthFailure, got %v", err)
	}
	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected still 1 fetch call (cached), got %d", count)
	}

	// Wait for cache to become stale (if it were a normal error)
	key := SpotPriceHistoryKey{
		Region:           "us-west-2",
		InstanceType:     "m5.large",
		AvailabilityZone: "us-west-2a",
	}
	cache.mutex.Lock()
	cache.cache[key].RetrievedAt = time.Now().Add(-2 * time.Hour)
	cache.mutex.Unlock()

	// Third call should STILL return cached auth error without fetching
	_, err = cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err == nil {
		t.Error("Expected cached auth error even when stale")
	}
	if !errors.Is(err, ErrSpotPriceAuthFailure) {
		t.Errorf("Expected ErrSpotPriceAuthFailure, got %v", err)
	}
	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected still 1 fetch call (auth errors never refresh), got %d", count)
	}
}

func TestSpotPriceHistoryCache_GetSpotPrice_GlobalAuthFlag(t *testing.T) {
	// Reset global flag before test
	globalSpotPriceAuthFailure.Store(false)

	var fetchCount atomic.Int32
	mockFetcher := &mockSpotPriceHistoryFetcher{
		fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
			// Check global flag first (simulating AWSSpotPriceHistoryFetcher behavior)
			if globalSpotPriceAuthFailure.Load() {
				return nil, ErrSpotPriceAuthFailure
			}

			fetchCount.Add(1)
			// Simulate auth error on first call
			globalSpotPriceAuthFailure.Store(true)
			return nil, fmt.Errorf("%w", ErrSpotPriceAuthFailure)
		},
	}
	cache := NewSpotPriceHistoryCache(mockFetcher)

	// First call for instance type A - should fetch and set global flag
	_, err := cache.GetSpotPrice("us-west-2", "m5.large", "us-west-2a")
	if err == nil {
		t.Error("Expected auth error")
	}
	if !errors.Is(err, ErrSpotPriceAuthFailure) {
		t.Errorf("Expected ErrSpotPriceAuthFailure, got %v", err)
	}
	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected 1 fetch call, got %d", count)
	}

	// Second call for DIFFERENT instance type/AZ - should NOT fetch due to global flag
	_, err = cache.GetSpotPrice("us-west-2", "t3.micro", "us-west-2b")
	if err == nil {
		t.Error("Expected auth error from global flag")
	}
	if !errors.Is(err, ErrSpotPriceAuthFailure) {
		t.Errorf("Expected ErrSpotPriceAuthFailure, got %v", err)
	}
	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected still 1 fetch call (global flag prevents second fetch), got %d", count)
	}

	// Third call for yet ANOTHER instance type/AZ - should also NOT fetch
	_, err = cache.GetSpotPrice("us-east-1", "g6f.xlarge", "us-east-1a")
	if err == nil {
		t.Error("Expected auth error from global flag")
	}
	if !errors.Is(err, ErrSpotPriceAuthFailure) {
		t.Errorf("Expected ErrSpotPriceAuthFailure, got %v", err)
	}
	if count := fetchCount.Load(); count != 1 {
		t.Errorf("Expected still 1 fetch call (global flag prevents all fetches), got %d", count)
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
