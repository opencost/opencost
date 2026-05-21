package aws

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSelectOnDemandHourlyPrice validates the fix for KCM-5777 where AWS pricing
// dimensions can contain multiple hourly prices (e.g., "On Demand" vs "Unused Reservation").
func TestSelectOnDemandHourlyPrice(t *testing.T) {
	testCases := []struct {
		name          string
		dimensions    map[string]*AWSRateCode
		isCNY         bool
		expectedPrice string
	}{
		{
			name: "Multiple dimensions - prefer On Demand over Unused Reservation",
			dimensions: map[string]*AWSRateCode{
				"unused": {
					Description:  "$12.38 per Unused Reservation Linux r8g.48xlarge Instance Hour",
					Unit:         "Hrs",
					PricePerUnit: AWSCurrencyCode{USD: "12.38"},
				},
				"ondemand": {
					Description:  "$11.31 per On Demand Linux r8g.48xlarge Instance Hour",
					Unit:         "Hrs",
					PricePerUnit: AWSCurrencyCode{USD: "11.31"},
				},
			},
			expectedPrice: "11.31",
		},
		{
			name: "OnDemand (no space) variant",
			dimensions: map[string]*AWSRateCode{
				"unused": {
					Description:  "$12.38 per Unused Reservation Linux Instance Hour",
					Unit:         "Hrs",
					PricePerUnit: AWSCurrencyCode{USD: "12.38"},
				},
				"ondemand": {
					Description:  "$11.31 per OnDemand Linux Instance Hour",
					Unit:         "Hrs",
					PricePerUnit: AWSCurrencyCode{USD: "11.31"},
				},
			},
			expectedPrice: "11.31",
		},
		{
			name: "On-Demand (hyphenated) variant",
			dimensions: map[string]*AWSRateCode{
				"ondemand": {
					Description:  "$11.31 per On-Demand Linux Instance Hour",
					Unit:         "Hrs",
					PricePerUnit: AWSCurrencyCode{USD: "11.31"},
				},
			},
			expectedPrice: "11.31",
		},
		{
			name: "Single dimension - backward compatibility",
			dimensions: map[string]*AWSRateCode{
				"only": {
					Description:  "$11.31 per Linux Instance Hour",
					Unit:         "Hrs",
					PricePerUnit: AWSCurrencyCode{USD: "11.31"},
				},
			},
			expectedPrice: "11.31",
		},
		{
			name: "Mixed units - filter to hourly only",
			dimensions: map[string]*AWSRateCode{
				"storage": {
					Description:  "$0.10 per GB-Mo",
					Unit:         "GB-Mo",
					PricePerUnit: AWSCurrencyCode{USD: "0.10"},
				},
				"compute": {
					Description:  "$11.31 per On Demand Linux Instance Hour",
					Unit:         "Hrs",
					PricePerUnit: AWSCurrencyCode{USD: "11.31"},
				},
			},
			expectedPrice: "11.31",
		},
		{
			name: "Exclude 'unused reservation' even with 'on demand' in description",
			dimensions: map[string]*AWSRateCode{
				"weird": {
					Description:  "$11.31 per On Demand Unused Reservation Linux Instance Hour",
					Unit:         "Hrs",
					PricePerUnit: AWSCurrencyCode{USD: "11.31"},
				},
				"normal": {
					Description:  "$11.31 per On Demand Linux Instance Hour",
					Unit:         "Hrs",
					PricePerUnit: AWSCurrencyCode{USD: "11.31"},
				},
			},
			expectedPrice: "11.31",
		},
		{
			name:          "Empty dimensions",
			dimensions:    map[string]*AWSRateCode{},
			expectedPrice: "",
		},
		{
			name: "No hourly dimensions - multiple non-hourly",
			dimensions: map[string]*AWSRateCode{
				"storage1": {
					Description:  "Storage",
					Unit:         "GB-Mo",
					PricePerUnit: AWSCurrencyCode{USD: "0.10"},
				},
				"storage2": {
					Description:  "Storage",
					Unit:         "GB-Mo",
					PricePerUnit: AWSCurrencyCode{USD: "0.20"},
				},
			},
			expectedPrice: "",
		},
		{
			name: "CNY currency",
			dimensions: map[string]*AWSRateCode{
				"ondemand": {
					Description:  "按需 Linux Instance Hour",
					Unit:         "Hrs",
					PricePerUnit: AWSCurrencyCode{CNY: "75.50"},
				},
			},
			isCNY:         true,
			expectedPrice: "75.50",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cost := selectOnDemandHourlyPrice(tc.dimensions, "MISSING.KEY", tc.isCNY)
			assert.Equal(t, tc.expectedPrice, cost)
		})
	}
}
