package reader

import (
	"context"
	"errors"
)

type Reader[T any] interface {
	Read(ctx context.Context, dst []T) (int, error)
	Close() error
}

var Done = errors.New("Done")

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
	if r.pos >= len(r.items) {
		return 0, Done
	}

	n := copy(dst, r.items[r.pos:])
	r.pos += n

	return n, nil
}

func (r *SliceReader[T]) Close() error {
	return nil
}
