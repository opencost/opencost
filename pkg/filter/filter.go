package filter

// Operation is an enum that represents operations that can be performed
// when filtering (equality, inequality, etc.)
type Operation string

// If you add a FilterOp, MAKE SURE TO UPDATE ALL FILTER IMPLEMENTATIONS! Go
// does not enforce exhaustive pattern matching on "enum" types.
const (
	// FilterEquals is the equality operator
	// "kube-system" FilterEquals "kube-system" = true
	// "kube-syste" FilterEquals "kube-system" = false
	FilterEquals Operation = "equals"

	// FilterNotEquals is the inequality operator
	FilterNotEquals = "notequals"

	// FilterContains is an array/slice membership operator
	// ["a", "b", "c"] FilterContains "a" = true
	FilterContains = "contains"

	// FilterNotContains is an array/slice non-membership operator
	// ["a", "b", "c"] FilterNotContains "d" = true
	FilterNotContains = "notcontains"

	// FilterStartsWith matches strings with the given prefix.
	// "kube-system" StartsWith "kube" = true
	//
	// When comparing with a field represented by an array/slice, this is like
	// applying FilterContains to every element of the slice.
	FilterStartsWith = "startswith"

	// FilterContainsPrefix is like FilterContains, but using StartsWith instead
	// of Equals.
	// ["kube-system", "abc123"] ContainsPrefix ["kube"] = true
	FilterContainsPrefix = "containsprefix"
)

// AllocationFilter represents anything that can be used to filter an
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

	// Flattened converts a filter into a minimal form, removing unnecessary
	// intermediate objects, like single-element or zero-element AND and OR
	// conditions.
	//
	// It returns nil if the filter is filtering nothing.
	//
	// Example:
	// (and (or (namespaceequals "kubecost")) (or)) ->
	// (namespaceequals "kubecost")
	//
	// (and (or)) -> nil
	Flattened() Filter[T]

	// Equals returns true if the two AllocationFilters are logically
	// equivalent.
	Equals(Filter[T]) bool
}
