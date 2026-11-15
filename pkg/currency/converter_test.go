package currency

import (
	"fmt"
	"testing"
	"time"
)

type mockClient struct {
	rates map[string]map[string]float64
	err   error
}

func (m *mockClient) FetchRates(baseCurrency string) (map[string]float64, error) {
	if m.err != nil {
		return nil, m.err
	}

	rates, exists := m.rates[baseCurrency]
	if !exists {
		return nil, fmt.Errorf("no rates for base currency %s", baseCurrency)
	}

	return rates, nil
}

type mockCache struct {
	data map[string]*cachedRates
}

func newMockCache() *mockCache {
	return &mockCache{
		data: make(map[string]*cachedRates),
	}
}

func (m *mockCache) get(baseCurrency string) (*cachedRates, bool) {
	rates, exists := m.data[baseCurrency]
	if !exists || time.Now().After(rates.validUntil) {
		return nil, false
	}
	return rates, true
}

func (m *mockCache) set(baseCurrency string, rates *cachedRates) {
	m.data[baseCurrency] = rates
}

func (m *mockCache) clear() {
	m.data = make(map[string]*cachedRates)
}

func TestCurrencyConverter_Convert(t *testing.T) {
	mockClient := &mockClient{
		rates: map[string]map[string]float64{
			"USD": {
				"USD": 1.0,
				"EUR": 0.85,
				"GBP": 0.73,
				"JPY": 110.0,
			},
			"EUR": {
				"EUR": 1.0,
				"USD": 1.18,
				"GBP": 0.86,
				"JPY": 129.53,
			},
		},
	}

	converter := &currencyConverter{
		provider: mockClient,
		cache:    newMockCache(),
		config:   Config{APIKey: "test"},
	}

	tests := []struct {
		name        string
		amount      float64
		from        string
		to          string
		expected    float64
		expectError bool
	}{
		{
			name:     "USD to EUR",
			amount:   100,
			from:     "USD",
			to:       "EUR",
			expected: 85,
		},
		{
			name:     "USD to GBP",
			amount:   100,
			from:     "USD",
			to:       "GBP",
			expected: 73,
		},
		{
			name:     "EUR to USD",
			amount:   100,
			from:     "EUR",
			to:       "USD",
			expected: 118,
		},
		{
			name:     "Same currency",
			amount:   100,
			from:     "USD",
			to:       "USD",
			expected: 100,
		},
		{
			name:     "Case insensitive",
			amount:   100,
			from:     "usd",
			to:       "eur",
			expected: 85,
		},
		{
			name:        "Unsupported currency",
			amount:      100,
			from:        "USD",
			to:          "XYZ",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.amount, tt.from, tt.to)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("expected %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestCurrencyConverter_GetRate(t *testing.T) {
	mockClient := &mockClient{
		rates: map[string]map[string]float64{
			"USD": {
				"USD": 1.0,
				"EUR": 0.85,
				"GBP": 0.73,
			},
		},
	}

	converter := &currencyConverter{
		provider: mockClient,
		cache:    newMockCache(),
		config:   Config{APIKey: "test"},
	}

	// Test getting rate
	rate, err := converter.GetRate("USD", "EUR")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if rate != 0.85 {
		t.Errorf("expected rate 0.85, got %f", rate)
	}

	// Test same currency
	rate, err = converter.GetRate("USD", "USD")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if rate != 1.0 {
		t.Errorf("expected rate 1.0, got %f", rate)
	}

	// Test cache hit
	rate, err = converter.GetRate("USD", "EUR")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if rate != 0.85 {
		t.Errorf("expected cached rate 0.85, got %f", rate)
	}
}

func TestNewConverter(t *testing.T) {
	// Test with empty API key
	_, err := NewConverter(Config{})
	if err == nil {
		t.Error("expected error for empty API key")
	}

	// Test with valid config
	converter, err := NewConverter(Config{APIKey: "test-key"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Convert to concrete type to access internal fields
	cc, ok := converter.(*currencyConverter)
	if !ok {
		t.Error("expected converter to be of type *currencyConverter")
		return
	}

	if cc.config.CacheTTL != 24*time.Hour {
		t.Errorf("expected default cache TTL of 24h, got %v", cc.config.CacheTTL)
	}

	if cc.config.APITimeout != 10*time.Second {
		t.Errorf("expected default API timeout of 10s, got %v", cc.config.APITimeout)
	}
}

func TestNewConverter_ProviderName(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		wantType string
		wantErr  bool
	}{
		{name: "empty selects exchangerate-api", provider: "", wantType: "*currency.exchangeRateClient"},
		{name: "exchangerate-api", provider: ProviderExchangeRateAPI, wantType: "*currency.exchangeRateClient"},
		{name: "allratestoday", provider: ProviderAllRatesToday, wantType: "*currency.allRatesTodayClient"},
		{name: "case and whitespace insensitive", provider: "  AllRatesToday ", wantType: "*currency.allRatesTodayClient"},
		{name: "unknown name is an error", provider: "fixer", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter, err := NewConverter(Config{APIKey: "test-key", ProviderName: tt.provider})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for provider %q", tt.provider)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := fmt.Sprintf("%T", converter.(*currencyConverter).provider)
			if got != tt.wantType {
				t.Errorf("provider %q: got %s, want %s", tt.provider, got, tt.wantType)
			}
		})
	}
}

func TestNewConverter_ProviderOverridesName(t *testing.T) {
	provider := &mockClient{rates: map[string]map[string]float64{"USD": {"EUR": 0.5}}}
	converter, err := NewConverter(Config{Provider: provider, ProviderName: "not-a-provider"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := converter.(*currencyConverter).provider.(*mockClient); !ok {
		t.Errorf("Provider should take precedence over ProviderName")
	}
}

func TestNewConverter_CustomProvider(t *testing.T) {
	provider := &mockClient{
		rates: map[string]map[string]float64{
			"USD": {"EUR": 0.85},
		},
	}

	// No API key is needed when a provider is supplied
	converter, err := NewConverter(Config{Provider: provider})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rate, err := converter.GetRate("USD", "EUR")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if rate != 0.85 {
		t.Errorf("expected rate 0.85 from custom provider, got %f", rate)
	}
}
