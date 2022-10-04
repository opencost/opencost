package filter

// AllCut is a filter that matches nothing. This is useful
// for applications like authorization, where a user/group/role may be disallowed
// from viewing Allocation data entirely.
type AllCut[T any] struct{}

func (ac AllCut[T]) String() string { return "(AllCut)" }

func (ac AllCut[T]) Matches(T) bool { return false }

func (ac AllCut[T]) Flattened() Filter[T] { return ac }

func (ac AllCut[T]) equals(that Filter[T]) bool {
	_, ok := that.(AllCut[T])
	return ok
}
