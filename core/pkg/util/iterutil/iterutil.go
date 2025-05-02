package iterutil

import "iter"

// Combine takes two iterator sequences and combines them into a single iterator sequence of pairs.
// This iterator will only yield as many values as the smallest of the two sequences.
func Combine[T any, U any](seq1 iter.Seq[T], seq2 iter.Seq[U]) iter.Seq2[T, U] {
	return func(yield func(T, U) bool) {
		n1, s1 := iter.Pull(seq1)
		n2, s2 := iter.Pull(seq2)

		defer s1()
		defer s2()

		for {
			first, fOk := n1()
			if !fOk {
				return
			}

			second, sOk := n2()
			if !sOk {
				return
			}

			if !yield(first, second) {
				return
			}
		}
	}
}

// Concat takes multiple iterator sequences and concatenates them into a single iterator sequence.
// This iterator will yield all values from the first sequence, followed by all values from the second
// sequence, and so on.
func Concat[T any](seqs ...iter.Seq[T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, seq := range seqs {
			func() {
				n, s := iter.Pull(seq)
				defer s()

				for {
					v, ok := n()
					if !ok || !yield(v) {
						return
					}
				}
			}()
		}
	}
}
