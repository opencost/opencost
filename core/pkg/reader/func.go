package reader

import (
	"context"
	"io"
)

// nextFunc produces the next item for the FuncReader to read.
//
// It should return:
//  1. (item, nil)    when an item is available and more may remain
//  2. (zero, io.EOF) when there are no more items remaining
//  3. (zero, err)    when there is an error getting the next item
//
// io.EOF is the end-of-stream signal; any other error is a failure. Any error
// is terminal. Once a non-nil error is returned it is latched and re-returned
// by Read.
//
// Do NOT return an item with io.EOF, as it will NOT be read.
type nextFunc[T any] func() (item T, err error)

// FuncReader adapts an arbitrary "next item" function into a Reader[T]. It
// optionally accepts an io.Closer, and will close it in Close(). If a
// terminal error is returned from next() it will hold that error and return
// it on Read() indefinitely (e.g. io.EOF when the next() source is exhausted).
type FuncReader[T any] struct {
	next   nextFunc[T]
	closer io.Closer
	err    error
}

// NewFuncReader returns a Reader[T] driven by next. If a non-nil closer is
// provided, it will be closed by Close().
func NewFuncReader[T any](next nextFunc[T], closer io.Closer) *FuncReader[T] {
	return &FuncReader[T]{next: next, closer: closer}
}

// Read fills dst with up to len(dst) items pulled from next() func, returning
// the number of items read. Returns io.EOF with the final batch, and will
// continue to return n=0 and the terminal error on subsequent reads.
func (r *FuncReader[T]) Read(ctx context.Context, dst []T) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.err != nil {
		return 0, r.err
	}

	n := 0
	for n < len(dst) {
		// Re-check cancellation each iteration.
		if err := ctx.Err(); err != nil {
			return n, err
		}

		item, err := r.next()
		if err != nil {
			// Terminal error (io.EOF for normal exhaustion, or a real failure).
			// Latch it and fold it into the batch gathered so far.
			r.err = err
			return n, err
		}

		// An item is available and more may remain.
		dst[n] = item
		n++
	}

	// dst is full, but more items may remain
	return n, nil
}

// Close closes the underlying source if it implements io.Closer, and is a no-op
// otherwise. It does not disturb the reader's terminal state, so it is safe to
// Close early to abandon a partially-read stream.
func (r *FuncReader[T]) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}
