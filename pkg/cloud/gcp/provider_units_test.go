package gcp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The conversion must follow the catalog's own byte-second factor, not a list
// of family names, so a new per-GB family (or a unit Google has not used yet)
// is handled without a code change.
func TestRamRatePerGiB(t *testing.T) {
	pe := func(unit, base string, factor float64) *GCPPricing {
		return &GCPPricing{PricingInfo: []*PricingInfo{{PricingExpression: &PricingExpression{
			UsageUnit: unit, BaseUnit: base, BaseUnitConversionFactor: flexFloat(factor),
		}}}}
	}
	cases := []struct {
		name string
		p    *GCPPricing
		want float64
	}{
		{"GiBy.h by factor is identity", pe("GiBy.h", "By.s", 3865470566400), 1},
		{"GBy.h by factor (C4D, M4 Ultramem 224)", pe("GBy.h", "By.s", 3600000000000), 1.073741824},
		{"unit never seen before, factor still decides (MBy.h)", pe("MBy.h", "By.s", 3600000000), 1073.741824},
		{"GBy.h without a factor falls back to the unit string", pe("GBy.h", "", 0), 1.073741824},
		{"GiBy.h without a factor", pe("GiBy.h", "", 0), 1},
		{"factor ignored when the base unit is not byte-seconds", pe("GBy.h", "s", 3600), 1.073741824},
		{"no pricing info at all", &GCPPricing{}, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.InDelta(t, c.want, ramRatePerGiB(c.p, 1), 1e-9)
		})
	}
}

// baseUnitConversionFactor is advisory: a number, a quoted number, null or
// garbage must never fail the catalog page.
func TestFlexFloat(t *testing.T) {
	for in, want := range map[string]float64{
		`{"baseUnitConversionFactor":3600000000000}`:   3.6e12,
		`{"baseUnitConversionFactor":"3865470566400"}`: 3.865470566400e12,
		`{"baseUnitConversionFactor":3.6e12}`:          3.6e12,
		`{"baseUnitConversionFactor":null}`:            0,
		`{"baseUnitConversionFactor":"n/a"}`:           0,
		`{}`:                                           0,
	} {
		var pe PricingExpression
		require.NoError(t, json.Unmarshal([]byte(in), &pe), in)
		assert.Equal(t, want, float64(pe.BaseUnitConversionFactor), in)
	}
}
