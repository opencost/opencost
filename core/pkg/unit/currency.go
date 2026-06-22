package unit

import (
	"fmt"
	"strings"
)

type Currency string

// TODO which of these are supported in the pricing APIs??
// TODO if the configured currency is NOT available, default to USD!

const (
	AUD Currency = "AUD"
	BRL Currency = "BRL"
	CAD Currency = "CAD"
	CHF Currency = "CHF"
	CNY Currency = "CNY"
	DKK Currency = "DKK"
	EUR Currency = "EUR"
	GBP Currency = "GBP"
	IDR Currency = "IDR"
	INR Currency = "INR"
	JPY Currency = "JPY"
	NOK Currency = "NOK"
	PLN Currency = "PLN"
	SEK Currency = "SEK"
	USD Currency = "USD"
)

// validCurrencies is a map of all valid currency codes for quick lookup
var validCurrencies = map[string]Currency{
	string(AUD): AUD,
	string(BRL): BRL,
	string(CAD): CAD,
	string(CHF): CHF,
	string(CNY): CNY,
	string(DKK): DKK,
	string(EUR): EUR,
	string(GBP): GBP,
	string(IDR): IDR,
	string(INR): INR,
	string(JPY): JPY,
	string(NOK): NOK,
	string(PLN): PLN,
	string(SEK): SEK,
	string(USD): USD,
}

// ParseCurrency parses a string into a Currency type.
// It performs case-insensitive matching and returns an error if the string
// does not match any valid currency code.
func ParseCurrency(s string) (Currency, error) {
	upper := strings.ToUpper(s)
	if currency, ok := validCurrencies[upper]; ok {
		return currency, nil
	}

	return "", fmt.Errorf("invalid currency: %q", s)
}
