package maputil

import (
	"iter"
	"maps"
)

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

// Merge accepts two compatible maps and merges them into a single new map instance.
func Merge[M1 ~map[K]V, M2 ~map[K]V, K comparable, V any](m1 M1, m2 M2) map[K]V {
	size := len(m1) + len(m2)
	if size == 0 {
		return map[K]V{}
	}

	result := make(map[K]V, size)
	maps.Copy(result, m1)
	// keys in m2 overwrite keys from m1
	maps.Copy(result, m2)
	return result
}
