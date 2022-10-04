package filter

import (
	"fmt"
	"sort"
)

// AllocationFilterOr is a set of filters that should be evaluated as a logical
// AND.
type And[T any] struct {
	Filters []Filter[T]
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
// - AllPass if filter contains no filters
// - AllCut if filter contains AllCut
// - the inner filter if filter contains one filter
// - an equivalent AllocationFilterAnd if filter contains more than one filter
func (a And[T]) Flattened() Filter[T] {
	var flattenedFilters []Filter[T]
	for _, innerFilter := range a.Filters {
		flattenedInner := innerFilter.Flattened()

		// Ignore AllPass in And
		if flattenedInner.equals(AllPass[T]{}) {
			continue
		}

		// AllCut means the entire And is AllCut
		if flattenedInner.equals(AllCut[T]{}) {
			return AllCut[T]{}
		}

		flattenedFilters = append(flattenedFilters, flattenedInner)
	}

	// Remove duplicates
	flattenedFilters = removeDuplicates(flattenedFilters)

	// Check for negations, if they exist And becomes AllCut
	if checkForNegations(flattenedFilters) {
		return AllCut[T]{}
	}

	// Empty And is an AllPass
	if len(flattenedFilters) == 0 {
		return AllPass[T]{}
	}

	if len(flattenedFilters) == 1 {
		return flattenedFilters[0]
	}

	// If more than half of the children are Not filters And should have DeMorgans rule applied to it
	if percentNot(flattenedFilters) > 0.5 {
		return Not[T]{Or[T]{negateFilters(flattenedFilters)}}
	}

	sort.SliceStable(flattenedFilters, func(i, j int) bool {
		return flattenedFilters[i].String() < flattenedFilters[j].String()
	})

	return And[T]{Filters: flattenedFilters}
}

func (a And[T]) equals(that Filter[T]) bool {
	// The type cast takes care of right == nil as well
	thatAnd, ok := that.(And[T])
	if !ok {
		return false
	}

	if len(a.Filters) != len(thatAnd.Filters) {
		return false
	}

	for i := range a.Filters {
		if !a.Filters[i].equals(thatAnd.Filters[i]) {
			return false
		}
	}
	return true
}
