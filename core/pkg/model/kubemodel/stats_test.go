package kubemodel

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
				StatAvg: 0.1,
				StatMax: 1.0,
			},
			nil,
		},
		{
			Stats{
				StatAvg: math.Inf(0),
				StatMax: 1.0,
			},
			errors.New("1 errors: [avg is Inf]"),
		},
		{
			Stats{
				StatAvg: math.Inf(0),
				StatMax: math.NaN(),
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
