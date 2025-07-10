package iterutil

import (
	"iter"
	"testing"
)

// toSeq maintains order in the sequence
func toSeq[T any](s []T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, v := range s {
			if !yield(v) {
				return
			}
		}
	}
}

type pair struct {
	first  int
	second string
}

func TestCombine(t *testing.T) {
	type testCase struct {
		name     string
		input1   []int
		input2   []string
		expected []pair
	}

	tests := []testCase{
		{
			name:     "empty slices",
			input1:   []int{},
			input2:   []string{},
			expected: []pair{},
		},
		{
			name:   "different string length slice",
			input1: []int{1, 2, 3},
			input2: []string{"a", "b"},
			expected: []pair{
				{1, "a"},
				{2, "b"},
			},
		},
		{
			name:   "different int length slice",
			input1: []int{1, 2},
			input2: []string{"a", "b", "c"},
			expected: []pair{
				{1, "a"},
				{2, "b"},
			},
		},
		{
			name:   "same length slices",
			input1: []int{1, 2, 3},
			input2: []string{"a", "b", "c"},
			expected: []pair{
				{1, "a"},
				{2, "b"},
				{3, "c"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := Combine(toSeq(test.input1), toSeq(test.input2))
			for f, s := range result {
				if !isPairIn(test.expected, pair{f, s}) {
					t.Errorf("expected %v, got %v", test.expected, pair{f, s})
				}
			}
		})
	}
}

func TestConcat(t *testing.T) {
	type testCase struct {
		name     string
		input1   []int
		input2   []int
		expected []int
	}

	tests := []testCase{
		{
			name:     "empty slices",
			input1:   []int{},
			input2:   []int{},
			expected: []int{},
		},
		{
			name:     "non-empty first slice",
			input1:   []int{1, 2, 3},
			input2:   []int{},
			expected: []int{1, 2, 3},
		},
		{
			name:     "non-empty second slice",
			input1:   []int{},
			input2:   []int{4, 5, 6},
			expected: []int{4, 5, 6},
		},
		{
			name:     "non-empty both slices",
			input1:   []int{1, 2, 3},
			input2:   []int{4, 5, 6},
			expected: []int{1, 2, 3, 4, 5, 6},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			results := Concat(toSeq(test.input1), toSeq(test.input2))

			// it is safe to compare this way due to the way a slice sequence iterator
			// obeys the ordering of the slice
			index := 0
			for result := range results {
				if result != test.expected[index] {
					t.Errorf("expected %v, got %v", test.expected[index], result)
				}
				index++
			}
		})
	}
}

func isPairIn(pairs []pair, p pair) bool {
	for _, pair := range pairs {
		if pair.first == p.first && pair.second == p.second {
			return true
		}
	}
	return false
}
