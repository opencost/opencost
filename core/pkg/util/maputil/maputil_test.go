package maputil

import (
	"testing"
)

type set[T comparable] struct {
	m map[T]struct{}
}

func newSet[T comparable](values ...T) *set[T] {
	s := &set[T]{
		m: make(map[T]struct{}, len(values)),
	}

	for _, v := range values {
		s.m[v] = struct{}{}
	}

	return s
}

func (s *set[T]) contains(value T) bool {
	_, ok := s.m[value]
	return ok
}

func (s *set[T]) remove(value T) {
	delete(s.m, value)
}

func TestFlatten(t *testing.T) {
	m := map[string]map[string]int{
		"A": {
			"b": 1,
			"c": 2,
			"d": 3,
		},
		"B": {
			"e": 4,
			"f": 5,
		},
		"C": {
			"g": 6,
			"h": 7,
			"i": 8,
			"j": 9,
		},
	}

	expected := newSet(1, 2, 3, 4, 5, 6, 7, 8, 9)

	flattened := Flatten(m)
	for value := range flattened {
		if !expected.contains(value) {
			t.Errorf("expected values did not contain the value: %d", value)
		}

		expected.remove(value)
	}
}

func TestAliasedMapFlatten(t *testing.T) {
	type IntMap map[string]int
	type StringIntMap map[string]IntMap

	m := StringIntMap(map[string]IntMap{
		"A": IntMap(map[string]int{
			"b": 1,
			"c": 2,
			"d": 3,
		}),
		"B": IntMap(map[string]int{
			"e": 4,
			"f": 5,
		}),
		"C": IntMap(map[string]int{
			"g": 6,
			"h": 7,
			"i": 8,
			"j": 9,
		}),
	})

	expected := newSet(1, 2, 3, 4, 5, 6, 7, 8, 9)

	flattened := Flatten(m)
	for value := range flattened {
		if !expected.contains(value) {
			t.Errorf("expected values did not contain the value: %d", value)
		}

		expected.remove(value)
	}
}

func TestFlatMap(t *testing.T) {
	m := map[string]map[string]int{
		"A": {
			"b": 1,
			"c": 2,
			"d": 3,
		},
		"B": {
			"e": 4,
			"f": 5,
		},
		"C": {
			"g": 6,
			"h": 7,
			"i": 8,
			"j": 9,
		},
	}

	expected := newSet(2, 4, 6, 8, 10, 12, 14, 16, 18)

	flatMap := FlatMap(m, func(value int) int {
		return value * 2
	})

	for value := range flatMap {
		if !expected.contains(value) {
			t.Errorf("expected values did not contain the value: %d", value)
		}

		expected.remove(value)
	}
}
