package sliceutil

import (
	"maps"
	"slices"
	"testing"
)

func TestSliceMap(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		expected []int
	}{
		{
			name:     "empty slice",
			input:    []int{},
			expected: []int{},
		},
		{
			name:     "single element",
			input:    []int{1},
			expected: []int{2},
		},
		{
			name:     "multiple elements",
			input:    []int{1, 2, 3},
			expected: []int{2, 4, 6},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := Map(test.input, func(i int) int { return i * 2 })
			for i, v := range result {
				if v != test.expected[i] {
					t.Errorf("expected %v, got %v", test.expected[i], v)
				}
			}
		})
	}
}

type seqTestCase[T comparable] struct {
	name  string
	input []T
}

func runSeqTest[T comparable](test seqTestCase[T]) func(*testing.T) {
	return func(t *testing.T) {
		result := AsSeq(test.input)

		i := 0
		for v := range result {
			if v != test.input[i] {
				t.Errorf("expected %v, got %v", test.input[i], v)
			}
			i++
		}
	}
}

func runSeqTests[T comparable](t *testing.T, testCases []seqTestCase[T]) {
	t.Helper()

	for _, test := range testCases {
		t.Run(test.name, runSeqTest(test))
	}
}

func TestToSeq(t *testing.T) {
	intTests := []seqTestCase[int]{
		{
			name:  "int empty slice",
			input: []int{},
		},
		{
			name:  "int single element",
			input: []int{1},
		},
		{
			name:  "int multiple elements",
			input: []int{1, 2, 3},
		},
	}

	floatTests := []seqTestCase[float64]{
		{
			name:  "float64 empty slice",
			input: []float64{},
		},
		{
			name:  "float64 single element",
			input: []float64{1.54},
		},
		{
			name:  "float64 multiple elements",
			input: []float64{52.32, 23.12, 54.123},
		},
	}

	stringTests := []seqTestCase[string]{
		{
			name:  "string empty slice",
			input: []string{},
		},
		{
			name:  "single single element",
			input: []string{"foo"},
		},
		{
			name:  "string multiple elements",
			input: []string{"foo", "bar", "baz"},
		},
	}

	runSeqTests(t, intTests)
	runSeqTests(t, floatTests)
	runSeqTests(t, stringTests)
}

func TestSeqToSlice(t *testing.T) {
	keys := []string{
		"a", "b", "c", "d", "e", "f", "g",
	}
	m := make(map[string]string, len(keys))
	for _, k := range keys {
		m[k] = "value-" + k
	}

	seqKeys := maps.Keys(m)
	seqValues := maps.Values(m)

	// These do *NOT* align on indexes!
	keySlice := SeqToSlice(seqKeys)
	valueSlice := SeqToSlice(seqValues)

	for _, k := range keySlice {
		if !slices.Contains(keys, k) {
			t.Errorf("expected %v to be in %v", k, keys)
		}
	}

	for _, v := range valueSlice {
		if !mapContainsValue(m, v) {
			t.Errorf("expected %v to be in %v", v, m)
		}
	}
}

func mapContainsValue(m map[string]string, value string) bool {
	for _, v := range m {
		if v == value {
			return true
		}
	}
	return false
}
