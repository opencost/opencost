package validator

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestWindowValidator(t *testing.T) {
	v := NewSetValidator[opencost.AllocationSet](time.Hour)

	end := time.Now().UTC()
	start := end.Add(-time.Hour)

	set := opencost.NewAllocationSet(start, end)

	invalidEnd := opencost.NewWindow(&start, nil)
	invalidStart := opencost.NewWindow(nil, &end)

	s := start.Truncate(time.Hour)
	e := end.Truncate(time.Hour)
	valid := opencost.NewWindow(&s, &e)

	// Invalid End
	set.Window = invalidEnd
	err := v.Validate(set.Window, set)
	if err == nil {
		t.Errorf("Validator returned valid flag for invalid window in set")
	}

	// InValid Start
	set.Window = invalidStart
	err = v.Validate(set.Window, set)
	if err == nil {
		t.Errorf("Validator returned valid flag for invalid window in set")
	}

	// Valid
	set.Window = valid
	err = v.Validate(set.Window, set)
	if err != nil {
		t.Errorf("Validator returned an error for a valid window: %v", err)
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
			v := NewSetValidator[opencost.AllocationSet](tc.resolution)
			set.Window = tc.window
			err := v.Validate(tc.window, set)
			isValid := err == nil
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

func TestEmptyAndNil(t *testing.T) {
	v := NewSetValidator[opencost.AllocationSet](time.Hour)

	end := time.Now().UTC().Truncate(time.Hour)
	start := end.Add(-time.Hour)

	window := opencost.NewClosedWindow(start, end)
	emptySet := opencost.NewAllocationSet(start, end)
	nilSet := (*opencost.AllocationSet)(nil)

	err := v.Validate(window, nilSet)
	if err == nil {
		t.Errorf("Validator returned valid flag for nil data")
	}

	isEmpty := !v.IsOverwrite(emptySet)
	if !isEmpty {
		t.Errorf("Validator returned overwrite flag for empty data")
	}
}

type collection struct {
	vs []string
}

func (c *collection) add(v string) {
	c.vs = append(c.vs, v)
}

func (c *collection) clear() {
	c.vs = []string{}
}

type appendingValidator struct {
	tag  string
	tags *collection
	fail bool
}

func newAppendingValidator(tag string, tags *collection) *appendingValidator {
	return &appendingValidator{
		tag:  tag,
		tags: tags,
	}
}

func newFailingValidator(tag string, tags *collection) *appendingValidator {
	return &appendingValidator{
		tag:  tag,
		tags: tags,
		fail: true,
	}
}

func (av *appendingValidator) Validate(window opencost.Window, data *opencost.AllocationSet) error {
	if av.fail {
		return fmt.Errorf("failed validator: %s", av.tag)
	}
	av.tags.add("Validate: " + av.tag)
	return nil
}

func (av *appendingValidator) IsOverwrite(data *opencost.AllocationSet) bool {
	av.tags.add("IsOverwrite: " + av.tag)
	return true
}

func TestChainValidation(t *testing.T) {
	tags := new(collection)

	validators := []ExportValidator[opencost.AllocationSet]{
		newAppendingValidator("a", tags),
		newAppendingValidator("b", tags),
		newAppendingValidator("c", tags),
		newAppendingValidator("d", tags),
	}

	v := NewChainValidator(validators...)

	end := time.Now().UTC().Truncate(time.Hour)
	start := end.Add(-time.Hour)

	window := opencost.NewClosedWindow(start, end)
	set := opencost.NewAllocationSet(start, end)

	err := v.Validate(window, set)
	if err != nil {
		t.Errorf("Validator returned unexpected error: %v", err)
	}

	if !slices.Contains(tags.vs, "Validate: a") {
		t.Errorf("Validator did not call validate on first validator")
	}
	if !slices.Contains(tags.vs, "Validate: b") {
		t.Errorf("Validator did not call validate on second validator")
	}
	if !slices.Contains(tags.vs, "Validate: c") {
		t.Errorf("Validator did not call validate on third validator")
	}
	if !slices.Contains(tags.vs, "Validate: d") {
		t.Errorf("Validator did not call validate on fourth validator")
	}

	tags.clear()

	// Test failing validator
	validators = []ExportValidator[opencost.AllocationSet]{
		newAppendingValidator("a", tags),
		newAppendingValidator("b", tags),
		newFailingValidator("c", tags),
		newAppendingValidator("d", tags),
	}

	v = NewChainValidator(validators...)
	err = v.Validate(window, set)
	if err == nil {
		t.Errorf("Validator did not return expected error")
	}

	if !slices.Contains(tags.vs, "Validate: a") {
		t.Errorf("Validator did not call validate on first validator")
	}
	if !slices.Contains(tags.vs, "Validate: b") {
		t.Errorf("Validator did not call validate on second validator")
	}
}
