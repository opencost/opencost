package filter

import "fmt"

// Not negates any filter contained within it
type Not[T any] struct {
	Filter Filter[T]
}

func (n Not[T]) String() string {
	return fmt.Sprintf("(not %s)", n.Filter.String())
}

// Matches inverts the result of the child filter
func (n Not[T]) Matches(that T) bool {
	return !n.Filter.Matches(that)
}

// Flattened flattens callers child filter. Cancel out child not and inverts AllPass and AllCut filters
func (n Not[T]) Flattened() Filter[T] {
	flattenedFilter := n.Filter.Flattened()

	// The negation of no filter (AllPass[T]) is filter everything (AllCut[T])
	if flattenedFilter.equals(AllPass[T]{}) {
		return AllCut[T]{}
	}

	// The negation of filter everything (AllCut[T]) is no filter (AllPass[T])
	if flattenedFilter.equals(AllCut[T]{}) {
		return AllPass[T]{}
	}

	// If child is Not filter, cancel out both filters and return the child of the child
	if childNot, ok := flattenedFilter.(Not[T]); ok {
		return childNot.Filter
	}

	return Not[T]{Filter: flattenedFilter}
}

func (n Not[T]) equals(that Filter[T]) bool {
	// The type cast takes care of right == nil as well
	thatNot, ok := that.(Not[T])
	if !ok {
		return false
	}

	return n.Filter.equals(thatNot.Filter)
}
