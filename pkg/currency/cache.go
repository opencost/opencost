package currency

import (
	"sync"
	"time"
)

type memoryCache struct {
	mu      sync.RWMutex
	data    map[string]*cachedRates
	ttl     time.Duration
	janitor *time.Ticker
}

func newMemoryCache(ttl time.Duration) *memoryCache {
	if ttl == 0 {
		ttl = 24 * time.Hour
	}

	cache := &memoryCache{
		data:    make(map[string]*cachedRates),
		ttl:     ttl,
		janitor: time.NewTicker(ttl / 2),
	}

	go cache.cleanup()

	return cache
}

func (c *memoryCache) get(baseCurrency string) (*cachedRates, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	rates, exists := c.data[baseCurrency]
	if !exists {
		return nil, false
	}

	if time.Now().After(rates.validUntil) {
		return nil, false
	}

	return rates, true
}

func (c *memoryCache) set(baseCurrency string, rates *cachedRates) {
	c.mu.Lock()
	defer c.mu.Unlock()

	rates.validUntil = rates.fetchedAt.Add(c.ttl)
	c.data[baseCurrency] = rates
}

func (c *memoryCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = make(map[string]*cachedRates)
}

func (c *memoryCache) cleanup() {
	for range c.janitor.C {
		c.removeExpired()
	}
}

func (c *memoryCache) removeExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, rates := range c.data {
		if now.After(rates.validUntil) {
			delete(c.data, key)
		}
	}
}

func (c *memoryCache) stop() {
	if c.janitor != nil {
		c.janitor.Stop()
	}
}

func (c *memoryCache) stats() (entries int, oldestEntry time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entries = len(c.data)

	for _, rates := range c.data {
		if oldestEntry.IsZero() || rates.fetchedAt.Before(oldestEntry) {
			oldestEntry = rates.fetchedAt
		}
	}

	return entries, oldestEntry
}
