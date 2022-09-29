package filter

// None is a filter that matches no allocations. This is useful
// for applications like authorization, where a user/group/role may be disallowed
// from viewing Allocation data entirely.
type None[T any] struct{}

func (n None[T]) String() string { return "(none)" }

func (n None[T]) Matches(T) bool { return false }

func (n None[T]) Flattened() Filter[T] { return n }

func (n None[T]) Equals(that Filter[T]) bool {
	_, ok := that.(None[T])
	return ok
}
