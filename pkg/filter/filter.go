package filter

// Filter represents anything that can be used to filter an
// Allocation.
//
// Implement this interface with caution. While it is generic, it
// is intended to be introspectable so query handlers can perform various
// optimizations. These optimizations include:
// - Routing a query to the most optimal cache
// - Querying backing data stores efficiently (e.g. translation to SQL)
//
// Custom implementations of this interface outside of this package should not
// expect to receive these benefits. Passing a custom implementation to a
// handler may in errors.
type Filter[T any] interface {
	String() string

	// Matches is the canonical in-Go function for determing if an Allocation
	// matches a filter.
	Matches(T) bool

	// Flattened converts a filter into a minimal standardized form, removing unnecessary
	// intermediate objects, like single-element or zero-element AND and OR
	// conditions.
	//
	// To achieve a standardized form, flattened preforms opinionated operations on And and OR
	// such as sorting child filters by String() and Applying DeMorgan's rule when more than half of filters are Not.
	// When exactly half of the filters are Not, And is preferred.
	//
	// It returns AllPass if the filter is filtering nothing.
	//
	//
	// Example:
	// (and (or (namespaceequals "kubecost")) (or)) ->
	// (namespaceequals "kubecost")
	//
	// (and (or)) -> nil
	Flattened() Filter[T]

	// equals checks that the input filter is exactly equal. non-flattend filters should not use this method
	equals(Filter[T]) bool
}

// Equals returns true if two filters are logically equivalent. To do this it first flattens the two filters which can
// cause their type to change.
func Equals[T any](this, that Filter[T]) bool {
	flatThis := this.Flattened()
	flatThat := that.Flattened()

	return flatThis.equals(flatThat)
}

// removeDuplicates assumes that []Filter[T] has already been flattend
func removeDuplicates[T any](flattenedFilters []Filter[T]) []Filter[T] {
	filterSet := make(map[string]Filter[T])
	for _, filter := range flattenedFilters {
		filterSet[filter.String()] = filter
	}
	var dedupedFilters []Filter[T]
	for _, filter := range filterSet {
		dedupedFilters = append(dedupedFilters, filter)
	}
	return dedupedFilters
}

// checkForNegations assumes that []Filter[T] has already been flattend
func checkForNegations[T any](flattenedFilters []Filter[T]) bool {
	for i, filter := range flattenedFilters {
		for j := i + 1; j < len(flattenedFilters); j++ {
			thatFilter := flattenedFilters[j]
			if checkNegation(filter, thatFilter) {
				return true
			}
		}
	}
	return false
}

func checkNegation[T any](this, that Filter[T]) bool {
	thisNot, thisIsNot := this.(Not[T])
	thatNot, thatIsNot := that.(Not[T])

	// If both are Not or both are not Not then they cannot be negations
	if thisIsNot == thatIsNot {
		return false
	}

	if thisIsNot {
		return thisNot.Filter.equals(that)
	}

	if thatIsNot {
		return thatNot.Filter.equals(this)
	}

	return false
}

func percentNot[T any](flattenedFilters []Filter[T]) float64 {
	countNot := 0.0
	for _, filter := range flattenedFilters {
		_, isNot := filter.(Not[T])
		if isNot {
			countNot += 1.0
		}
	}
	return countNot / float64(len(flattenedFilters))
}

func negateFilters[T any](flattenedFilters []Filter[T]) []Filter[T] {
	var negatedFilters []Filter[T]
	for _, filter := range flattenedFilters {
		if notFilter, isNot := filter.(Not[T]); isNot {
			negatedFilters = append(negatedFilters, notFilter.Filter)
		} else {
			negatedFilters = append(negatedFilters, Not[T]{filter})
		}
	}
	return negatedFilters
}
