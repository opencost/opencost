package azure

import (
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/commerce/armcommerce"
)

// meterInfo is the part of a rate card or price sheet meter that pricing
// conversion reads.
type meterInfo struct {
	MeterName        string
	MeterCategory    string
	MeterSubCategory string
	MeterRegion      string
	Unit             string
	MeterRates       map[string]float64
}

func meterInfoFromRateCard(m *armcommerce.MeterInfo) meterInfo {
	rates := make(map[string]float64, len(m.MeterRates))
	for quantity, rate := range m.MeterRates {
		if rate != nil {
			rates[quantity] = widenRate(*rate)
		}
	}
	return meterInfo{
		MeterName:        deref(m.MeterName),
		MeterCategory:    deref(m.MeterCategory),
		MeterSubCategory: deref(m.MeterSubCategory),
		MeterRegion:      deref(m.MeterRegion),
		Unit:             deref(m.Unit),
		MeterRates:       rates,
	}
}

// widenRate converts a rate card price to float64. The SDK decodes rates as
// float32, and a plain conversion turns a price of 0.096 into
// 0.09600000083446503. Going through the shortest decimal that identifies the
// float32 recovers the price as published.
func widenRate(rate float32) float64 {
	widened, _ := strconv.ParseFloat(strconv.FormatFloat(float64(rate), 'g', -1, 32), 64)
	return widened
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

func derefAll[T any](ps []*T) []T {
	values := make([]T, 0, len(ps))
	for _, p := range ps {
		if p != nil {
			values = append(values, *p)
		}
	}
	return values
}
