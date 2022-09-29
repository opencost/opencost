package filter

import (
	"fmt"
)

// Or is a set of filters that should be evaluated as a logical
// OR.
type Or[T any] struct {
	Filters []Filter[T]
}

func (o Or[T]) GetFilters() []Filter[T] {
	return o.Filters
}

func (o Or[T]) String() string {
	s := "(or"
	for _, f := range o.Filters {
		s += fmt.Sprintf(" %s", f)
	}

	s += ")"
	return s
}

// Flattened converts a filter into a minimal form, removing unnecessary
// intermediate objects
//
// Flattened returns:
// - nil if filter contains no filters
// - the inner filter if filter contains one filter
// - an equivalent AllocationOr if filter contains more than one filter
func (o Or[T]) Flattened() Filter[T] {
	flattenedFilters := flattenGroup[T](o)
	if len(flattenedFilters) == 0 {
		return nil
	}

	if len(flattenedFilters) == 1 {
		return flattenedFilters[0]
	}

	return Or[T]{Filters: flattenedFilters}
}

func (o Or[T]) Equals(that Filter[T]) bool {
	// The type cast takes care of that == nil as well
	thatOr, ok := that.(Or[T])
	if !ok {
		return false
	}

	if len(o.Filters) != len(thatOr.Filters) {
		return false
	}

	sortGroup[T](o)
	sortGroup[T](thatOr)

	for i := range o.Filters {
		if !o.Filters[i].Equals(thatOr.Filters[i]) {
			return false
		}
	}
	return true
}

func (o Or[T]) Matches(that T) bool {
	filters := o.Filters
	if len(filters) == 0 {
		return true
	}

	for _, filter := range filters {
		if filter.Matches(that) {
			return true
		}
	}

	return false
}
