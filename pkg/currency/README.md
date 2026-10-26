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

## Using a Different Rate Provider

Rates come from exchangerate-api.com by default. To use another source, implement `RateProvider` and set it on the config. `APIKey` is then not required.

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

## How it Works

The package fetches exchange rates and caches them for 24 hours. This keeps API usage low - most plugins use under 50 requests per month.

Supports all ISO 4217 currencies (161 total). Thread-safe with automatic cache cleanup.

## Example Usage in Plugins

```go
// Plugin config
type PluginConfig struct {
    TargetCurrency  string `json:"target_currency"`
    ExchangeAPIKey  string `json:"exchange_api_key"`
}

// Initialize converter
if config.ExchangeAPIKey != "" {
    converter, _ := currency.NewConverter(currency.Config{
        APIKey:   config.ExchangeAPIKey,
        CacheTTL: 24 * time.Hour,
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