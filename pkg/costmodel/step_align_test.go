package costmodel

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestAlignStepStart(t *testing.T) {
	// Reference dates all in April 2026:
	//   Mon Apr 6, Tue Apr 7, Wed Apr 8, ..., Sun Apr 12
	//   Mon Apr 13, Tue Apr 14
	tuesdayApr7 := time.Date(2026, time.April, 7, 0, 0, 0, 0, time.UTC)
	mondayApr6 := time.Date(2026, time.April, 6, 0, 0, 0, 0, time.UTC)
	mondayApr13 := time.Date(2026, time.April, 13, 0, 0, 0, 0, time.UTC)
	tuesdayApr14MidDay := time.Date(2026, time.April, 14, 12, 30, 0, 0, time.UTC)

	tests := map[string]struct {
		start    time.Time
		step     time.Duration
		expected time.Time
	}{
		"weekly step from Tuesday aligns back to preceding Monday": {
			start:    tuesdayApr7,
			step:     timeutil.Week,
			expected: mondayApr6,
		},
		"weekly step from Monday is unchanged": {
			start:    mondayApr13,
			step:     timeutil.Week,
			expected: mondayApr13,
		},
		"weekly step from Tuesday mid-day aligns to preceding Monday midnight": {
			start:    tuesdayApr14MidDay,
			step:     timeutil.Week,
			expected: mondayApr13,
		},
		"daily step is unchanged on a Tuesday": {
			start:    tuesdayApr7,
			step:     24 * time.Hour,
			expected: tuesdayApr7,
		},
		"hourly step is unchanged": {
			start:    tuesdayApr14MidDay,
			step:     time.Hour,
			expected: tuesdayApr14MidDay,
		},
		"arbitrary step close to but not equal to a week is unchanged": {
			start:    tuesdayApr7,
			step:     7*24*time.Hour - time.Hour,
			expected: tuesdayApr7,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := alignStepStart(tc.start, tc.step)
			if !got.Equal(tc.expected) {
				t.Errorf("alignStepStart(%s, %s) = %s; want %s", tc.start, tc.step, got, tc.expected)
			}
		})
	}
}
