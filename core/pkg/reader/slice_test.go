package reader

import (
	"context"
	"errors"
	"io"
	"testing"
)

// TestSliceReader_FoldedTerminalSignal verifies that the read which exhausts the
// slice returns io.EOF together with the final items, rather than deferring the
// signal to a separate (0, io.EOF) call.
func TestSliceReader_FoldedTerminalSignal(t *testing.T) {
	tests := []struct {
		name    string
		items   int
		bufSize int
		// wantReads is the expected (n, eof) result of each successive Read.
		wantReads []struct {
			n   int
			eof bool
		}
	}{
		{
			name:    "buffer larger than items",
			items:   3,
			bufSize: 10,
			wantReads: []struct {
				n   int
				eof bool
			}{{3, true}},
		},
		{
			name:    "buffer exactly fits items",
			items:   5,
			bufSize: 5,
			wantReads: []struct {
				n   int
				eof bool
			}{{5, true}},
		},
		{
			name:    "buffer evenly divides items",
			items:   6,
			bufSize: 3,
			wantReads: []struct {
				n   int
				eof bool
			}{{3, false}, {3, true}},
		},
		{
			name:    "buffer unevenly divides items",
			items:   7,
			bufSize: 3,
			wantReads: []struct {
				n   int
				eof bool
			}{{3, false}, {3, false}, {1, true}},
		},
		{
			name:    "single item",
			items:   1,
			bufSize: 1,
			wantReads: []struct {
				n   int
				eof bool
			}{{1, true}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewSliceReader(seq(tc.items))
			dst := make([]int, tc.bufSize)

			for i, want := range tc.wantReads {
				n, err := r.Read(context.Background(), dst)
				if n != want.n {
					t.Errorf("read %d: got n=%d, want %d", i, n, want.n)
				}
				gotEOF := errors.Is(err, io.EOF)
				if gotEOF != want.eof {
					t.Errorf("read %d: got eof=%v (err=%v), want eof=%v", i, gotEOF, err, want.eof)
				}
				if !want.eof && err != nil {
					t.Errorf("read %d: unexpected error: %v", i, err)
				}
			}
		})
	}
}

// TestSliceReader_EmptySlice verifies an exhausted-from-the-start reader returns
// (0, io.EOF) immediately.
func TestSliceReader_EmptySlice(t *testing.T) {
	for _, items := range [][]int{nil, {}} {
		r := NewSliceReader(items)
		n, err := r.Read(context.Background(), make([]int, 4))
		if n != 0 {
			t.Errorf("got n=%d, want 0", n)
		}
		if !errors.Is(err, io.EOF) {
			t.Errorf("got err=%v, want io.EOF", err)
		}
	}
}

// TestSliceReader_ReadAfterExhaustion verifies that reads following the folded
// terminal signal continue to return (0, io.EOF).
func TestSliceReader_ReadAfterExhaustion(t *testing.T) {
	r := NewSliceReader(seq(2))
	dst := make([]int, 4)

	// First read drains everything and folds in io.EOF.
	if n, err := r.Read(context.Background(), dst); n != 2 || !errors.Is(err, io.EOF) {
		t.Fatalf("first read: got (%d, %v), want (2, io.EOF)", n, err)
	}

	// Subsequent reads keep reporting io.EOF with no data.
	for i := 0; i < 3; i++ {
		n, err := r.Read(context.Background(), dst)
		if n != 0 || !errors.Is(err, io.EOF) {
			t.Errorf("read after exhaustion %d: got (%d, %v), want (0, io.EOF)", i, n, err)
		}
	}
}

// TestSliceReader_PreservesOrderAndValues drains the reader in small chunks and
// verifies every item is returned exactly once, in order.
func TestSliceReader_PreservesOrderAndValues(t *testing.T) {
	const items = 25
	r := NewSliceReader(seq(items))
	dst := make([]int, 4)

	var got []int
	for {
		n, err := r.Read(context.Background(), dst)
		got = append(got, dst[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if len(got) != items {
		t.Fatalf("got %d items, want %d", len(got), items)
	}
	for i, v := range got {
		if v != i {
			t.Errorf("item %d: got %d, want %d", i, v, i)
		}
	}
}

// TestSliceReader_ContextCancellation verifies a cancelled context short-circuits
// the read even when items remain.
func TestSliceReader_ContextCancellation(t *testing.T) {
	r := NewSliceReader(seq(5))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	n, err := r.Read(ctx, make([]int, 4))
	if n != 0 {
		t.Errorf("got n=%d, want 0", n)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got err=%v, want context.Canceled", err)
	}

	// The reader was not advanced, so it still yields all items afterward.
	got := 0
	for {
		n, err := r.Read(context.Background(), make([]int, 2))
		got += n
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error draining: %v", err)
		}
	}
	if got != 5 {
		t.Errorf("drained %d items after cancellation, want 5", got)
	}
}

// TestSliceReader_ZeroLengthBuffer documents the io.Reader-style behavior: a
// zero-length dst with items remaining reads nothing and returns no error.
func TestSliceReader_ZeroLengthBuffer(t *testing.T) {
	r := NewSliceReader(seq(3))

	n, err := r.Read(context.Background(), []int{})
	if n != 0 {
		t.Errorf("got n=%d, want 0", n)
	}
	if err != nil {
		t.Errorf("got err=%v, want nil", err)
	}
}

// TestSliceReader_Close verifies Close is a no-op that reports no error and does
// not disturb the read position.
func TestSliceReader_Close(t *testing.T) {
	r := NewSliceReader(seq(2))
	if err := r.Close(); err != nil {
		t.Errorf("Close: got err=%v, want nil", err)
	}

	n, err := r.Read(context.Background(), make([]int, 4))
	if n != 2 || !errors.Is(err, io.EOF) {
		t.Errorf("read after Close: got (%d, %v), want (2, io.EOF)", n, err)
	}
}

// TestSliceReader_PointerElements exercises the generic reader with a pointer
// element type, matching how the pricing readers use it.
func TestSliceReader_PointerElements(t *testing.T) {
	items := []*int{ptr(1), ptr(2), ptr(3)}
	r := NewSliceReader(items)
	dst := make([]*int, 3)

	n, err := r.Read(context.Background(), dst)
	if n != 3 || !errors.Is(err, io.EOF) {
		t.Fatalf("got (%d, %v), want (3, io.EOF)", n, err)
	}
	for i, p := range dst {
		if p == nil || *p != i+1 {
			t.Errorf("item %d: got %v, want pointer to %d", i, p, i+1)
		}
	}
}

// seq returns []int{0, 1, ..., n-1}.
func seq(n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return s
}

func ptr[T any](v T) *T { return &v }
