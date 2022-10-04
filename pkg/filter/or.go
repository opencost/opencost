package filter

import (
	"fmt"
	"sort"
)

// Or is a set of filters that should be evaluated as a logical
// OR.
type Or[T any] struct {
	Filters []Filter[T]
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
// - AllCut if filter contains no filters
// - AllPass if filter contains AllPass
// - the inner filter if filter contains one filter
// - an equivalent AllocationOr if filter contains more than one filter
func (o Or[T]) Flattened() Filter[T] {
	var flattenedFilters []Filter[T]
	for _, innerFilter := range o.Filters {
		flattenedInner := innerFilter.Flattened()

		// Ignore AllCut in Or
		if flattenedInner.equals(AllCut[T]{}) {
			continue
		}

		// AllPass means the entire Or is AllPass
		if flattenedInner.equals(AllPass[T]{}) {
			return AllPass[T]{}
		}

		flattenedFilters = append(flattenedFilters, flattenedInner)
	}

	// Remove duplicates
	flattenedFilters = removeDuplicates(flattenedFilters)

	// Check for negations, if they exist And becomes AllPass
	if checkForNegations(flattenedFilters) {
		return AllPass[T]{}
	}

	// Empty Or is an AllPass
	if len(flattenedFilters) == 0 {
		return AllPass[T]{}
	}

	if len(flattenedFilters) == 1 {
		return flattenedFilters[0]
	}

	// If more than half of the children are Not filters, Or should have DeMorgans rule applied to it
	if percentNot(flattenedFilters) >= 0.5 {
		return Not[T]{And[T]{negateFilters(flattenedFilters)}}
	}

	// Sort after other operations in case, order is changed
	sort.SliceStable(flattenedFilters, func(i, j int) bool {
		return flattenedFilters[i].String() < flattenedFilters[j].String()
	})

	return Or[T]{Filters: flattenedFilters}
}

func (o Or[T]) equals(that Filter[T]) bool {
	// The type cast takes care of that == nil as well
	thatOr, ok := that.(Or[T])
	if !ok {
		return false
	}

	if len(o.Filters) != len(thatOr.Filters) {
		return false
	}

	for i := range o.Filters {
		if !o.Filters[i].equals(thatOr.Filters[i]) {
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
