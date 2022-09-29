package filter

// AllPass is a filter that matches everything and is the same as no filter. It is implemented here as a guard
// against universal operations occurring in the absence of filters. This is useful
// for applications like authorization, where a user/group/role may be disallowed
// from viewing Allocation data entirely.
type AllPass[T any] struct{}

func (n AllPass[T]) String() string { return "(AllPass)" }

func (n AllPass[T]) Matches(T) bool { return true }

func (n AllPass[T]) Flattened() Filter[T] { return n }

func (n AllPass[T]) equals(that Filter[T]) bool {
	_, ok := that.(AllPass[T])
	return ok
}
