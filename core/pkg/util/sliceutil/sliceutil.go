package sliceutil

import (
	"iter"
	"slices"
)

// Map accepts a slice of T and applies a transformation function to each index of a
// slice, which are inserted into a new slice of type U.
func Map[T any, U any](s []T, transform func(T) U) []U {
	result := make([]U, len(s))
	for i := 0; i < len(s); i++ {
		result[i] = transform(s[i])
	}
	return result
}

// AsSeq converts a slice of T into an iterator sequence only yielding the values. This should be used
// to convert a slice into an iterator sequence for APIs that accept iterators only.
func AsSeq[T any](s []T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, v := range s {
			if !yield(v) {
				return
			}
		}
	}
}

// AsSeq2 converts a slice of T into an iterator sequence yielding the index and value. This should be used
// to convert a slice into an iterator sequence for APIs that accept iterators only.
func AsSeq2[T any](s []T) iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		for i, v := range s {
			if !yield(i, v) {
				return
			}
		}
	}
}

// SeqToSlice converts an iterator sequence into a slice of T.
func SeqToSlice[T any](s iter.Seq[T]) []T {
	return slices.Collect(s)
}
