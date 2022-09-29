package filter

import (
	"fmt"
)

// AllocationFilterOr is a set of filters that should be evaluated as a logical
// AND.
type And[T any] struct {
	Filters []Filter[T]
}

func (a And[T]) GetFilters() []Filter[T] {
	return a.Filters
}

func (a And[T]) String() string {
	s := "(and"
	for _, f := range a.Filters {
		s += fmt.Sprintf(" %s", f)
	}

	s += ")"
	return s
}

func (a And[T]) Matches(that T) bool {
	filters := a.Filters
	if len(filters) == 0 {
		return true
	}

	for _, filter := range filters {
		if !filter.Matches(that) {
			return false
		}
	}

	return true
}

// Flattened converts a filter into a minimal form, removing unnecessary
// intermediate objects
//
// Flattened returns:
// - nil if filter contains no filters
// - the inner filter if filter contains one filter
// - an equivalent AllocationFilterAnd if filter contains more than one filter
func (a And[T]) Flattened() Filter[T] {
	flattenedFilters := flattenGroup[T](a)
	if len(flattenedFilters) == 0 {
		return nil
	}

	if len(flattenedFilters) == 1 {
		return flattenedFilters[0]
	}

	return And[T]{Filters: flattenedFilters}
}

func (a And[T]) Equals(that Filter[T]) bool {
	// The type cast takes care of right == nil as well
	thatAnd, ok := that.(And[T])
	if !ok {
		return false
	}

	if len(a.Filters) != len(thatAnd.Filters) {
		return false
	}

	sortGroup[T](a)
	sortGroup[T](thatAnd)

	for i := range a.Filters {
		if !a.Filters[i].Equals(thatAnd.Filters[i]) {
			return false
		}
	}
	return true
}
