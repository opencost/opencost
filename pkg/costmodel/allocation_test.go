package costmodel

import (
	"fmt"
	"testing"
	"time"
)

func TestWindowOverlap(t *testing.T) {
	loc, _ := time.LoadLocation("UTC")
	cases := map[string]struct {
		start1 time.Time
		end1 time.Time
		start2 time.Time
		end2 time.Time
		expected time.Duration
	}{
		"Same timeframe": {
			start1: time.Date(2020, 3, 15, 1, 0, 0, 0, loc),
			end1: time.Date(2020, 3, 15, 2, 0, 0, 0, loc),
			start2: time.Date(2020, 3, 15, 1, 0, 0, 0, loc),
			end2: time.Date(2020, 3, 15, 2, 0, 0, 0, loc),
			expected: time.Hour,
		},
		"sub-timeframe": {
			start1: time.Date(2020, 3, 15, 1, 0, 0, 0, loc),
			end1: time.Date(2020, 3, 15, 2, 0, 0, 0, loc),
			start2: time.Date(2020, 3, 15, 1, 15, 0, 0, loc),
			end2: time.Date(2020, 3, 15, 1, 30, 0, 0, loc),
			expected: time.Hour / 2,
		},
		"sub-timeframe 2": {
			start1: time.Date(2020, 3, 15, 1, 0, 0, 0, loc),
			end1: time.Date(2020, 3, 15, 2, 0, 0, 0, loc),
			start2: time.Date(2020, 3, 15, 0, 0, 0, 0, loc),
			end2: time.Date(2020, 3, 15, 3, 0, 0, 0, loc),
			expected: time.Hour,
		},
		"1 before 2": {
			start1: time.Date(2020, 3, 15, 1, 0, 0, 0, loc),
			end1: time.Date(2020, 3, 15, 2, 0, 0, 0, loc),
			start2: time.Date(2020, 3, 15, 3, 0, 0, 0, loc),
			end2: time.Date(2020, 3, 15, 4, 0, 0, 0, loc),
			expected: 0,
		},
		"2 before 1": {
			start1: time.Date(2020, 3, 15, 5, 0, 0, 0, loc),
			end1: time.Date(2020, 3, 15, 6, 0, 0, 0, loc),
			start2: time.Date(2020, 3, 15, 3, 0, 0, 0, loc),
			end2: time.Date(2020, 3, 15, 4, 0, 0, 0, loc),
			expected: 0,
		},
		"partial overlap ": {
			start1: time.Date(2020, 3, 15, 5, 0, 0, 0, loc),
			end1: time.Date(2020, 3, 15, 6, 0, 0, 0, loc),
			start2: time.Date(2020, 3, 15, 3, 0, 0, 0, loc),
			end2: time.Date(2020, 3, 15, 4, 0, 0, 0, loc),
			expected: 0,
		},

	}
	fmt.Println(cases)
}
