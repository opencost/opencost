package stats

import (
	"errors"
	"math"
	"testing"
)

func TestStats_Sanitize(t *testing.T) {
	type testCase struct {
		stats Stats
		exp   error
	}

	testCases := []testCase{
		{
			nil,
			nil,
		},
		{
			Stats{},
			nil,
		},
		{
			Stats{
				Val: 1.0,
			},
			nil,
		},
		{
			Stats{
				Avg: 0.1,
				Max: 1.0,
			},
			nil,
		},
		{
			Stats{
				Avg: math.Inf(0),
				Max: 1.0,
			},
			errors.New("1 errors: [avg is Inf]"),
		},
		{
			Stats{
				Avg: math.Inf(0),
				Max: math.NaN(),
			},
			errors.New("2 errors: [avg is Inf] [max is NaN]"),
		},
	}

	for _, tc := range testCases {
		err := tc.stats.Sanitize()
		if err != nil && tc.exp == nil {
			t.Errorf("unexpected error: %s", err)
		}
		if err == nil && tc.exp != nil {
			t.Errorf("expected error: %s", tc.exp)
		}
		if err != nil && tc.exp != nil && err.Error()[0] != tc.exp.Error()[0] {
			t.Errorf("expected error: %s; received error: %s", tc.exp, err)
		}
	}
}
