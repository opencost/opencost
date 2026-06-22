package aws

import "testing"

func TestForCurrency(t *testing.T) {
	cases := []struct {
		name string
		unit PriceListEC2PricePerUnit
		code string
		want string
	}{
		{
			name: "USD explicit",
			unit: PriceListEC2PricePerUnit{USD: "0.096", CNY: "0.62"},
			code: "USD",
			want: "0.096",
		},
		{
			name: "USD default when code is empty",
			unit: PriceListEC2PricePerUnit{USD: "0.096"},
			code: "",
			want: "0.096",
		},
		{
			name: "USD default for unrecognized code",
			unit: PriceListEC2PricePerUnit{USD: "0.096"},
			code: "EUR",
			want: "0.096",
		},
		{
			name: "CNY uppercase",
			unit: PriceListEC2PricePerUnit{USD: "0.096", CNY: "0.62"},
			code: "CNY",
			want: "0.62",
		},
		{
			name: "CNY lowercase",
			unit: PriceListEC2PricePerUnit{USD: "0.096", CNY: "0.62"},
			code: "cny",
			want: "0.62",
		},
		{
			name: "CNY empty falls back to USD",
			unit: PriceListEC2PricePerUnit{USD: "0.096", CNY: ""},
			code: "CNY",
			want: "0.096",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.unit.ForCurrency(tc.code)
			if got != tc.want {
				t.Errorf("ForCurrency(%q) = %q, want %q", tc.code, got, tc.want)
			}
		})
	}
}
