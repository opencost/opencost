package maputil

import "iter"

// Map applies a transformation function to each value within a map to get a new map containing the
// transformed values.
func Map[K comparable, V any, T any](m map[K]V, transform func(V) T) map[K]T {
	result := make(map[K]T, len(m))
	for k, v := range m {
		result[k] = transform(v)
	}
	return result
}

// Flatten returns an iterator that will iterate over a nested map.
func Flatten[Map ~map[T]Inner, Inner ~map[T]U, T comparable, U any](m Map) iter.Seq[U] {
	return func(yield func(U) bool) {
		for _, inner := range m {
			for _, value := range inner {
				if !yield(value) {
					return
				}
			}
		}
	}
}

// FlatMap returns an iterator that will iterate over a nested map, and apply a transformation to a different type.
func FlatMap[Map ~map[T]Inner, Inner ~map[T]U, T comparable, U any, V any](m Map, transform func(U) V) iter.Seq[V] {
	return func(yield func(V) bool) {
		for _, inner := range m {
			for _, value := range inner {
				if !yield(transform(value)) {
					return
				}
			}
		}
	}
}
