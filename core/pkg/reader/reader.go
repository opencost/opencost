package reader

import (
	"context"
)

// Reader is a generic, io.Reader-style streaming interface. Read fills dst with
// up to len(dst) items and returns the number read. When the stream is
// exhausted it returns io.EOF, which may accompany a non-zero count on the
// final read; callers must always process the returned items before honoring
// the error.
type Reader[T any] interface {
	Read(ctx context.Context, dst []T) (int, error)
	Close() error
}
