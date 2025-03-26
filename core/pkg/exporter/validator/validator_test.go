package validator

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestWindowValidator(t *testing.T) {
	v := NewWindowValidator[opencost.AllocationSet]()

	end := time.Now().UTC()
	start := end.Add(-time.Hour)

	set := opencost.NewAllocationSet(start, end)

	invalidEnd := opencost.NewWindow(&start, nil)
	invalidStart := opencost.NewWindow(nil, &end)
	valid := opencost.NewWindow(&start, &end)

	// Invalid End
	set.Window = invalidEnd
	isValid, err := v.IsValid(set)
	if isValid || err == nil {
		t.Errorf("Validator returned valid flag for invalid window in set")
	}

	// InValid Start
	set.Window = invalidStart
	isValid, err = v.IsValid(set)
	if isValid || err == nil {
		t.Errorf("Validator returned valid flag for invalid window in set")
	}

	// Valid
	set.Window = valid
	isValid, err = v.IsValid(set)
	if !isValid || err != nil {
		t.Errorf("Validator returned an invalid flag or error for a valid window")
	}

}

func TestResolutionValidator(t *testing.T) {
	v := NewResolutionValidator[opencost.AllocationSet](time.Hour)

	end := time.Now().UTC()
	start := end.Add(-time.Hour)
	start2h := start.Add(-time.Hour)

	set := opencost.NewAllocationSet(start, end)

	invalidEnd := opencost.NewWindow(&start, nil)
	invalidStart := opencost.NewWindow(nil, &end)
	invalidResolution := opencost.NewWindow(&start2h, &end)
	valid := opencost.NewWindow(&start, &end)

	// Invalid End
	set.Window = invalidEnd
	isValid, err := v.IsValid(set)
	if isValid || err == nil {
		t.Errorf("Validator returned valid flag for invalid window in set")
	}

	// Invalid Start
	set.Window = invalidStart
	isValid, err = v.IsValid(set)
	if isValid || err == nil {
		t.Errorf("Validator returned valid flag for invalid window in set")
	}

	// Invalid Resolution
	set.Window = invalidResolution
	isValid, err = v.IsValid(set)
	if isValid || err == nil {
		t.Errorf("Validator returned valid flag for invalid resolution in set")
	}

	// Valid
	set.Window = valid
	isValid, err = v.IsValid(set)
	if !isValid || err != nil {
		t.Errorf("Validator returned an invalid flag or error for a valid window")
	}
}

func TestUTCResolutionValidator(t *testing.T) {
	start := opencost.RoundBack(time.Now().UTC(), timeutil.Week)

	set := opencost.NewAllocationSet(start, start.Add(time.Hour))

	testCases := map[string]struct {
		resolution time.Duration
		window     opencost.Window
		expected   bool
	}{
		"Invalid End": {
			resolution: time.Hour,
			window:     opencost.NewWindow(&start, nil),
			expected:   false,
		},
		"Invalid Start": {
			resolution: time.Hour,
			window:     opencost.NewWindow(nil, &start),
			expected:   false,
		},
		"Hour: Invalid Resolution": {
			resolution: time.Hour,
			window:     opencost.NewClosedWindow(start, start.Add(2*time.Hour)),
			expected:   false,
		},
		"Hour: Invalid UTC position": {
			resolution: time.Hour,
			window:     opencost.NewClosedWindow(start.Add(time.Minute), start.Add(time.Hour).Add(time.Minute)),
			expected:   false,
		},
		"Hour: Valid": {
			resolution: time.Hour,
			window:     opencost.NewClosedWindow(start, start.Add(time.Hour)),
			expected:   true,
		},
		"Day: Invalid UTC position": {
			resolution: timeutil.Day,
			window:     opencost.NewClosedWindow(start.Add(time.Minute), start.Add(timeutil.Day).Add(time.Minute)),
			expected:   false,
		},
		"Day: Valid": {
			resolution: timeutil.Day,
			window:     opencost.NewClosedWindow(start, start.Add(timeutil.Day)),
			expected:   true,
		},
		"Week: Invalid UTC position": {
			resolution: timeutil.Week,
			window:     opencost.NewClosedWindow(start.Add(timeutil.Day), start.Add(timeutil.Week).Add(timeutil.Day)),
			expected:   false,
		},
		"Week: Valid": {
			resolution: timeutil.Week,
			window:     opencost.NewClosedWindow(start, start.Add(timeutil.Week)),
			expected:   true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			v := NewUTCResolutionValidator[opencost.AllocationSet](tc.resolution)
			set.Window = tc.window
			isValid, err := v.IsValid(set)
			if tc.expected != isValid {
				t.Errorf("Validator returned incorrect flag")
			}
			if tc.expected && err != nil {
				t.Errorf("Validator returned unexpected error")
			}
			if !tc.expected && err == nil {
				t.Errorf("Validator did not returned expected error")
			}

		})
	}
}

func TestEmptySetValidator(t *testing.T) {
	v := NewEmptySetValidator[opencost.AllocationSet](time.Hour)

	end := time.Now().UTC()
	start := end.Add(-time.Hour)
	start2h := start.Add(-time.Hour)

	set := opencost.NewAllocationSet(start, end, opencost.NewMockUnitAllocation("", start, time.Hour, nil))

	invalidEnd := opencost.NewWindow(&start, nil)
	invalidStart := opencost.NewWindow(nil, &end)
	invalidResolution := opencost.NewWindow(&start2h, &end)
	valid := opencost.NewWindow(&start, &end)

	//
	// Non-Empty Tests
	//

	// Invalid End
	set.Window = invalidEnd
	isValid, err := v.IsValid(set)
	if isValid || err == nil {
		t.Errorf("Validator returned valid flag for invalid window in set")
	}

	// Invalid Start
	set.Window = invalidStart
	isValid, err = v.IsValid(set)
	if isValid || err == nil {
		t.Errorf("Validator returned valid flag for invalid window in set")
	}

	// Invalid Resolution
	set.Window = invalidResolution
	isValid, err = v.IsValid(set)
	if isValid || err == nil {
		t.Errorf("Validator returned valid flag for invalid resolution in set")
	}

	// Valid
	set.Window = valid
	isValid, err = v.IsValid(set)
	if !isValid || err != nil {
		t.Errorf("Validator returned an invalid flag or error for a valid window")
	}

	//
	// Empty Test
	//

	set = opencost.NewAllocationSet(start, end)
	isValid, err = v.IsValid(set)
	if isValid || err == nil {
		t.Errorf("Validator returned valid flag for empty set")
	}
}
