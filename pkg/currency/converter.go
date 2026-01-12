package currency

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type currencyConverter struct {
	client client
	cache  cache
	config Config
	mu     sync.RWMutex
}

func NewConverter(config Config) (Converter, error) {
	if config.APIKey == "" {
		return nil, fmt.Errorf("API key is required")
	}

	if config.CacheTTL == 0 {
		config.CacheTTL = 24 * time.Hour
	}

	if config.APITimeout == 0 {
		config.APITimeout = 10 * time.Second
	}

	client := newExchangeRateClient(config.APIKey, config.APITimeout)
	cache := newMemoryCache(config.CacheTTL)

	return &currencyConverter{
		client: client,
		cache:  cache,
		config: config,
	}, nil
}

func (c *currencyConverter) Convert(amount float64, from, to string) (float64, error) {
	from = strings.ToUpper(strings.TrimSpace(from))
	to = strings.ToUpper(strings.TrimSpace(to))

	if from == to {
		return amount, nil
	}

	rate, err := c.GetRate(from, to)
	if err != nil {
		return 0, fmt.Errorf("failed to get exchange rate from %s to %s: %w", from, to, err)
	}

	return amount * rate, nil
}

func (c *currencyConverter) GetRate(from, to string) (float64, error) {
	from = strings.ToUpper(strings.TrimSpace(from))
	to = strings.ToUpper(strings.TrimSpace(to))

	if from == to {
		return 1.0, nil
	}

	cachedRates, found := c.cache.get(from)
	if found && cachedRates.rates != nil {
		if rate, exists := cachedRates.rates[to]; exists {
			return rate, nil
		}
	}

	rates, err := c.fetchAndCacheRates(from)
	if err != nil {
		return 0, err
	}

	rate, exists := rates[to]
	if !exists {
		return 0, fmt.Errorf("currency %s not supported or not found in exchange rates", to)
	}

	return rate, nil
}

func (c *currencyConverter) fetchAndCacheRates(baseCurrency string) (map[string]float64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cachedRates, found := c.cache.get(baseCurrency); found {
		return cachedRates.rates, nil
	}

	response, err := c.client.fetchRates(baseCurrency)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch rates from API: %w", err)
	}

	cachedRates := &cachedRates{
		rates:     response.ConversionRates,
		baseCode:  response.BaseCode,
		fetchedAt: time.Now(),
	}
	c.cache.set(baseCurrency, cachedRates)

	return response.ConversionRates, nil
}
