package kubecost

// Pair is a generic struct containing a pair of instances, one of each type similar to std::pair
type Pair[T any, U any] struct {
	First  T
	Second U
}

// Creates a new pair struct containing the provided parameters. This is useful for creating types
// capable of representing common paired types (result, error), (result, bool), etc...
func NewPair[T any, U any](first T, second U) Pair[T, U] {
	return Pair[T, U]{
		First:  first,
		Second: second,
	}
}

// DefaultValue[T] returns the default value for any generic type. This is helpful for generic
// types where a type parameter can be a value type or pointer.
func DefaultValue[T any]() T {
	var t T
	return t
}
