package currency

import (
	"time"
)

// Config holds configuration for the currency converter
type Config struct {
	// APIKey is the exchangerate-api.com key used by the default provider.
	// It is ignored when Provider is set.
	APIKey     string
	CacheTTL   time.Duration
	APITimeout time.Duration

	// Provider supplies the exchange rates. If nil, the converter uses
	// exchangerate-api.com with APIKey.
	Provider RateProvider
}

// RateProvider fetches current exchange rates from an exchange rate source.
// Implementations must be safe for concurrent use.
type RateProvider interface {
	// FetchRates returns the rates for one unit of baseCurrency, keyed by
	// ISO 4217 currency code.
	FetchRates(baseCurrency string) (map[string]float64, error)
}

// Converter interface defines currency conversion operations
type Converter interface {
	// Convert converts an amount from one currency to another
	Convert(amount float64, from, to string) (float64, error)

	// GetRate returns the exchange rate between two currencies
	GetRate(from, to string) (float64, error)
}

// exchangeRateResponse represents the API response from exchangerate-api.com
type exchangeRateResponse struct {
	Result             string             `json:"result"`
	Documentation      string             `json:"documentation"`
	TermsOfUse         string             `json:"terms_of_use"`
	TimeLastUpdateUnix int64              `json:"time_last_update_unix"`
	TimeLastUpdateUTC  string             `json:"time_last_update_utc"`
	TimeNextUpdateUnix int64              `json:"time_next_update_unix"`
	TimeNextUpdateUTC  string             `json:"time_next_update_utc"`
	BaseCode           string             `json:"base_code"`
	ConversionRates    map[string]float64 `json:"conversion_rates"`
}

// cachedRates stores exchange rates with metadata
type cachedRates struct {
	rates      map[string]float64
	baseCode   string
	fetchedAt  time.Time
	validUntil time.Time
}

// cache interface for storing exchange rates
type cache interface {
	// get retrieves cached rates for a base currency
	get(baseCurrency string) (*cachedRates, bool)

	// set stores rates for a base currency with TTL
	set(baseCurrency string, rates *cachedRates)

	// clear removes all cached rates
	clear()
}
