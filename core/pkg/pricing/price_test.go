package pricing

import (
	"errors"
	"testing"

	"github.com/opencost/opencost/core/pkg/unit"
)

func TestGetPrices(t *testing.T) {
	testCases := []struct {
		name   string
		prices Prices
	}{
		{
			name:   "empty Prices",
			prices: Prices{},
		},
		{
			name: "single hourly price",
			prices: Prices{
				unit.USD: []Price{
					{
						Currency: unit.USD,
						Unit:     unit.Hour,
						Price:    0.096,
					},
				},
			},
		},
		{
			name: "single set of per-resource prices",
			prices: Prices{
				unit.USD: []Price{
					{
						Currency: unit.USD,
						Unit:     unit.VCPUHour,
						Price:    0.031611,
					},
					{
						Currency: unit.USD,
						Unit:     unit.RAMGiBHour,
						Price:    0.004237,
					},
				},
			},
		},
		{
			name: "prices for multiple currencies",
			prices: Prices{
				unit.USD: []Price{
					{
						Currency: unit.USD,
						Unit:     unit.VCPUHour,
						Price:    0.031611,
					},
					{
						Currency: unit.USD,
						Unit:     unit.RAMGiBHour,
						Price:    0.004237,
					},
				},
				unit.CNY: []Price{
					{
						Currency: unit.CNY,
						Unit:     unit.VCPUHour,
						Price:    3.1611,
					},
					{
						Currency: unit.CNY,
						Unit:     unit.RAMGiBHour,
						Price:    0.4237,
					},
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.prices.GetPrices()

			expectedLen := 0
			for _, prices := range tt.prices {
				expectedLen += len(prices)
			}

			if len(result) != expectedLen {
				t.Errorf("expected %d prices, got %d", expectedLen, len(result))
			}
		})
	}
}

func TestGetPricesInCurrency(t *testing.T) {
	tests := []struct {
		name          string
		prices        Prices
		currency      unit.Currency
		expectedCount int
		expectError   bool
		errorType     error
	}{
		{
			name:          "empty Prices - should return NotFound error",
			prices:        Prices{},
			currency:      unit.USD,
			expectedCount: 0,
			expectError:   true,
			errorType:     NotFound,
		},
		{
			name: "currency not found - should return NotFound error",
			prices: Prices{
				unit.EUR: []Price{
					{
						Currency: unit.EUR,
						Unit:     unit.VCPUHour,
						Price:    0.028,
					},
				},
			},
			currency:      unit.USD,
			expectedCount: 0,
			expectError:   true,
			errorType:     NotFound,
		},
		{
			name: "single matching currency",
			prices: Prices{
				unit.USD: []Price{
					{
						Currency: unit.USD,
						Unit:     unit.VCPUHour,
						Price:    0.031611,
					},
				},
			},
			currency:      unit.USD,
			expectedCount: 1,
			expectError:   false,
		},
		{
			name: "multiple currencies - find USD",
			prices: Prices{
				unit.USD: []Price{
					{
						Currency: unit.USD,
						Unit:     unit.VCPUHour,
						Price:    0.031611,
					},
				},
				unit.EUR: []Price{
					{
						Currency: unit.EUR,
						Unit:     unit.VCPUHour,
						Price:    0.028,
					},
				},
				unit.GBP: []Price{
					{
						Currency: unit.GBP,
						Unit:     unit.RAMGiBHour,
						Price:    0.025,
					},
				},
			},
			currency:      unit.USD,
			expectedCount: 1,
			expectError:   false,
		},
		{
			name: "multiple currencies - find EUR",
			prices: Prices{
				unit.USD: []Price{
					{
						Currency: unit.USD,
						Unit:     unit.VCPUHour,
						Price:    0.031611,
					},
				},
				unit.EUR: []Price{
					{
						Currency: unit.EUR,
						Unit:     unit.VCPUHour,
						Price:    0.028,
					},
				},
			},
			currency:      unit.EUR,
			expectedCount: 1,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.prices.GetPricesInCurrency(tt.currency)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				if !errors.Is(err, tt.errorType) {
					t.Errorf("expected error type %v, got %v", tt.errorType, err)
				}
				if len(result) != 0 {
					t.Errorf("expected nil or empty result on error, got %d prices", len(result))
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if len(result) != tt.expectedCount {
					t.Errorf("expected %d prices, got %d", tt.expectedCount, len(result))
				}

				// Verify all returned prices have the requested currency
				for _, price := range result {
					if price.Currency != tt.currency {
						t.Errorf("expected currency %v, got %v", tt.currency, price.Currency)
					}
				}
			}
		})
	}
}

func TestGetPricesInCurrencyWithDefault(t *testing.T) {
	testCases := []struct {
		name            string
		prices          Prices
		currency        unit.Currency
		defaultCurrency unit.Currency
		expectedCount   int
		expectError     bool
		expectedCurr    unit.Currency
	}{
		{
			name:            "empty Prices - should return NotFound error",
			prices:          Prices{},
			currency:        unit.USD,
			defaultCurrency: unit.EUR,
			expectedCount:   0,
			expectError:     true,
		},
		{
			name: "currency found - should return requested currency",
			prices: Prices{
				unit.USD: []Price{
					{
						Currency: unit.USD,
						Unit:     unit.VCPUHour,
						Price:    0.031611,
					},
				},
			},
			currency:        unit.USD,
			defaultCurrency: unit.EUR,
			expectedCount:   1,
			expectError:     false,
			expectedCurr:    unit.USD,
		},
		{
			name: "currency not found - should fallback to default",
			prices: Prices{
				unit.EUR: []Price{
					{
						Currency: unit.EUR,
						Unit:     unit.VCPUHour,
						Price:    0.028,
					},
				},
			},
			currency:        unit.USD,
			defaultCurrency: unit.EUR,
			expectedCount:   1,
			expectError:     false,
			expectedCurr:    unit.EUR,
		},
		{
			name: "neither currency nor default found - should return NotFound error",
			prices: Prices{
				unit.GBP: []Price{
					{
						Currency: unit.GBP,
						Unit:     unit.VCPUHour,
						Price:    0.025,
					},
				},
			},
			currency:        unit.USD,
			defaultCurrency: unit.EUR,
			expectedCount:   0,
			expectError:     true,
		},
		{
			name: "multiple currencies - prefer requested over default",
			prices: Prices{
				unit.USD: []Price{
					{
						Currency: unit.USD,
						Unit:     unit.VCPUHour,
						Price:    0.031611,
					},
				},
				unit.EUR: []Price{
					{
						Currency: unit.EUR,
						Unit:     unit.VCPUHour,
						Price:    0.028,
					},
				},
			},
			currency:        unit.USD,
			defaultCurrency: unit.EUR,
			expectedCount:   1,
			expectError:     false,
			expectedCurr:    unit.USD,
		},
		{
			name: "same currency and default - should return requested",
			prices: Prices{
				unit.USD: []Price{
					{
						Currency: unit.USD,
						Unit:     unit.VCPUHour,
						Price:    0.031611,
					},
				},
			},
			currency:        unit.USD,
			defaultCurrency: unit.USD,
			expectedCount:   1,
			expectError:     false,
			expectedCurr:    unit.USD,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.prices.GetPricesInCurrencyWithDefault(tt.currency, tt.defaultCurrency)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				if !errors.Is(err, NotFound) {
					t.Errorf("expected NotFound error, got %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if len(result) != tt.expectedCount {
					t.Errorf("expected %d prices, got %d", tt.expectedCount, len(result))
				}

				// Verify all returned prices have the expected currency
				for _, price := range result {
					if price.Currency != tt.expectedCurr {
						t.Errorf("expected currency %v, got %v", tt.expectedCurr, price.Currency)
					}
				}
			}
		})
	}
}
