package unit

import (
	"strings"
	"testing"
)

func TestParseCurrency(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expect    Currency
		expectErr bool
	}{
		// Valid currencies - exact case
		{name: "AUD", input: "AUD", expect: AUD, expectErr: false},
		{name: "BRL", input: "BRL", expect: BRL, expectErr: false},
		{name: "CAD", input: "CAD", expect: CAD, expectErr: false},
		{name: "CHF", input: "CHF", expect: CHF, expectErr: false},
		{name: "CNY", input: "CNY", expect: CNY, expectErr: false},
		{name: "DKK", input: "DKK", expect: DKK, expectErr: false},
		{name: "EUR", input: "EUR", expect: EUR, expectErr: false},
		{name: "GBP", input: "GBP", expect: GBP, expectErr: false},
		{name: "IDR", input: "IDR", expect: IDR, expectErr: false},
		{name: "INR", input: "INR", expect: INR, expectErr: false},
		{name: "JPY", input: "JPY", expect: JPY, expectErr: false},
		{name: "NOK", input: "NOK", expect: NOK, expectErr: false},
		{name: "PLN", input: "PLN", expect: PLN, expectErr: false},
		{name: "SEK", input: "SEK", expect: SEK, expectErr: false},
		{name: "USD", input: "USD", expect: USD, expectErr: false},

		// Case insensitive tests
		{name: "lowercase usd", input: "usd", expect: USD, expectErr: false},
		{name: "lowercase eur", input: "eur", expect: EUR, expectErr: false},
		{name: "lowercase gbp", input: "gbp", expect: GBP, expectErr: false},
		{name: "mixed case Usd", input: "Usd", expect: USD, expectErr: false},
		{name: "mixed case eUr", input: "eUr", expect: EUR, expectErr: false},

		// Invalid currencies
		{name: "invalid empty", input: "", expect: "", expectErr: true},
		{name: "invalid unknown", input: "XYZ", expect: "", expectErr: true},
		{name: "invalid number", input: "123", expect: "", expectErr: true},
		{name: "invalid partial", input: "US", expect: "", expectErr: true},
		{name: "invalid too long", input: "USDD", expect: "", expectErr: true},
		{name: "invalid with space", input: "U SD", expect: "", expectErr: true},
		{name: "invalid symbol", input: "$", expect: "", expectErr: true},
		{name: "invalid symbol euro", input: "€", expect: "", expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCurrency(tt.input)
			if (err != nil) != tt.expectErr {
				t.Errorf("ParseCurrency(%q) error = %v, expectErr %v", tt.input, err, tt.expectErr)
				return
			}
			if got != tt.expect {
				t.Errorf("ParseCurrency(%q) = %v, expect %v", tt.input, got, tt.expect)
			}
		})
	}
}

func TestParseCurrency_AllConstants(t *testing.T) {
	// Ensure all defined currency constants can be parsed
	allCurrencies := []Currency{
		AUD, BRL, CAD, CHF, CNY, DKK, EUR, GBP,
		IDR, INR, JPY, NOK, PLN, SEK, USD,
	}

	for _, currency := range allCurrencies {
		t.Run(string(currency), func(t *testing.T) {
			parsed, err := ParseCurrency(string(currency))
			if err != nil {
				t.Errorf("ParseCurrency(%q) unexpected error: %v", currency, err)
			}
			if parsed != currency {
				t.Errorf("ParseCurrency(%q) = %v, expected %v", currency, parsed, currency)
			}
		})
	}
}

func TestParseCurrency_CaseInsensitiveAllConstants(t *testing.T) {
	// Ensure all defined currency constants can be parsed in lowercase
	allCurrencies := []Currency{
		AUD, BRL, CAD, CHF, CNY, DKK, EUR, GBP,
		IDR, INR, JPY, NOK, PLN, SEK, USD,
	}

	for _, currency := range allCurrencies {
		lowercase := strings.ToLower(string(currency))
		t.Run(lowercase, func(t *testing.T) {
			parsed, err := ParseCurrency(lowercase)
			if err != nil {
				t.Errorf("ParseCurrency(%q) unexpected error: %v", lowercase, err)
			}
			if parsed != currency {
				t.Errorf("ParseCurrency(%q) = %v, expect %v", lowercase, parsed, currency)
			}
		})
	}
}
