package reader

import (
	"context"
	"io"
)

type SliceReader[T any] struct {
	items []T
	pos   int
}

func NewSliceReader[T any](items []T) *SliceReader[T] {
	return &SliceReader[T]{
		items: items,
		pos:   0,
	}
}

func (r *SliceReader[T]) Read(ctx context.Context, dst []T) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if r.pos >= len(r.items) {
		return 0, io.EOF
	}

	n := copy(dst, r.items[r.pos:])
	r.pos += n

	// Fold the terminal signal into the final data-bearing read rather than
	// requiring a separate trailing call that returns (0, io.EOF).
	if r.pos >= len(r.items) {
		return n, io.EOF
	}

	return n, nil
}

func (r *SliceReader[T]) Close() error {
	return nil
}
