# Currency Package

Convert costs between currencies in OpenCost using live exchange rates. This package provides a reusable currency conversion utility for OpenCost components and plugins.

## Quick Start

```go
import "github.com/opencost/opencost/pkg/currency"

config := currency.Config{
    APIKey:   "your-api-key",
    CacheTTL: 24 * time.Hour,
}

converter, err := currency.NewConverter(config)
if err != nil {
    log.Fatal(err)
}

// Convert 100 USD to EUR
amount, err := converter.Convert(100.0, "USD", "EUR")
```

## Setup

Get a free API key from [exchangerate-api.com](https://www.exchangerate-api.com/) (1,500 requests/month).

## Choosing a Rate Provider

Rates come from exchangerate-api.com by default. Pick another built-in provider by name, so the choice can live in a plugin's configuration instead of Go code:

```go
converter, err := currency.NewConverter(currency.Config{
    ProviderName: currency.ProviderAllRatesToday, // "allratestoday"
    APIKey:       "art_live_...",
    CacheTTL:     24 * time.Hour,
})
```

| `ProviderName` | Provider | Key |
|---|---|---|
| `exchangerate-api` (default when empty) | [exchangerate-api.com](https://www.exchangerate-api.com/) | free key, 1,500 requests/month |
| `allratestoday` | [allratestoday.com](https://allratestoday.com/) | free trial key; a [paid plan](https://allratestoday.com/pricing/) is needed for ongoing use |

An unknown name returns an error from `NewConverter`.

To use any other source, implement `RateProvider` and set it on the config. `APIKey` and `ProviderName` are then ignored.

```go
type RateProvider interface {
    // FetchRates returns the rates for one unit of baseCurrency, keyed by ISO 4217 code.
    FetchRates(baseCurrency string) (map[string]float64, error)
}

converter, err := currency.NewConverter(currency.Config{
    Provider: myProvider,
    CacheTTL: 24 * time.Hour,
})
```

Caching works the same with any provider.

The built-in providers are also exported as constructors, `currency.NewExchangeRateAPIProvider(apiKey, timeout)` and `currency.NewAllRatesTodayProvider(apiKey, timeout)`, for use as `Provider`.

## How it Works

The package fetches exchange rates and caches them for 24 hours. This keeps API usage low - most plugins use under 50 requests per month.

Supports all ISO 4217 currencies (161 total). Thread-safe with automatic cache cleanup.

## Example Usage in Plugins

```go
// Plugin config
type PluginConfig struct {
    TargetCurrency   string `json:"target_currency"`
    ExchangeProvider string `json:"exchange_provider"` // "exchangerate-api" (default) or "allratestoday"
    ExchangeAPIKey   string `json:"exchange_api_key"`
}

// Initialize converter
if config.ExchangeAPIKey != "" {
    converter, _ := currency.NewConverter(currency.Config{
        ProviderName: config.ExchangeProvider,
        APIKey:       config.ExchangeAPIKey,
        CacheTTL:     24 * time.Hour,
    })
}

// Convert costs
if converter != nil {
    cost, _ = converter.Convert(cost, "USD", targetCurrency)
}
```

## Testing

```bash
cd pkg/currency
go test -v
```

Tests use mocks - no API calls needed.