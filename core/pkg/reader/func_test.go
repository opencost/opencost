package reader

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestFuncReader(t *testing.T) {
	// counter yields 0..total-1 then reports io.EOF.
	newCounter := func(total int) nextFunc[int] {
		i := 0
		return func() (int, error) {
			if i >= total {
				return 0, io.EOF
			}
			v := i
			i++
			return v, nil
		}
	}

	t.Run("returns io.EOF with the final batch, then stays terminal", func(t *testing.T) {
		// counter(3) drained through a 2-slot buffer: reads of 2, 1, 0.
		r := NewFuncReader(newCounter(3), nil)
		dst := make([]int, 2)

		// Exact fill: the buffer fills before next() reports io.EOF, so io.EOF
		// is deferred to the following read (Read cannot know it is exhausted
		// without pulling again).
		if n, err := r.Read(context.Background(), dst); n != 2 || err != nil {
			t.Fatalf("read 1: got (%d, %v), want (2, nil)", n, err)
		}
		// The last item and io.EOF come back together: the terminal error is
		// returned alongside the final data-bearing batch, not on a separate call.
		if n, err := r.Read(context.Background(), dst); n != 1 || !errors.Is(err, io.EOF) {
			t.Fatalf("read 2: got (%d, %v), want (1, io.EOF)", n, err)
		}
		// Terminal state is sticky: io.EOF latched, no further items.
		if n, err := r.Read(context.Background(), dst); n != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("read 3: got (%d, %v), want (0, io.EOF)", n, err)
		}
	})

	t.Run("io.EOF at batch start yields (0, io.EOF)", func(t *testing.T) {
		r := NewFuncReader(newCounter(0), nil)
		if n, err := r.Read(context.Background(), make([]int, 4)); n != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("got (%d, %v), want (0, io.EOF)", n, err)
		}
	})

	t.Run("item returned alongside io.EOF is not read", func(t *testing.T) {
		// A nextFunc that violates the contract by returning a real item with
		// io.EOF. io.EOF is authoritative: the item must be dropped, never
		// written to dst. This pins the "Do NOT return an item with io.EOF"
		// rule that lets FuncReader avoid disambiguating zero from placeholder.
		next := func() (int, error) { return 99, io.EOF }

		r := NewFuncReader(next, nil)
		dst := []int{-1, -1}

		n, err := r.Read(context.Background(), dst)
		if n != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("got (%d, %v), want (0, io.EOF)", n, err)
		}
		if dst[0] != -1 {
			t.Errorf("dst[0] was written despite io.EOF: got %d, want untouched (-1)", dst[0])
		}
	})

	t.Run("error is returned with prefix and is sticky", func(t *testing.T) {
		wantErr := errors.New("boom")
		calls := 0
		next := func() (int, error) {
			calls++
			switch calls {
			case 1:
				return 10, nil
			case 2:
				return 0, wantErr
			default:
				t.Fatalf("next called %d times; should have latched", calls)
				return 0, nil
			}
		}

		r := NewFuncReader(next, nil)
		dst := make([]int, 4)

		n, err := r.Read(context.Background(), dst)
		if n != 1 || !errors.Is(err, wantErr) {
			t.Fatalf("read 1: got (%d, %v), want (1, %v)", n, err, wantErr)
		}
		if dst[0] != 10 {
			t.Errorf("prefix item: got %d, want 10", dst[0])
		}
		// Sticky: no further calls to next, same error returned.
		if n2, err2 := r.Read(context.Background(), dst); n2 != 0 || !errors.Is(err2, wantErr) {
			t.Errorf("read 2: got (%d, %v), want (0, %v)", n2, err2, wantErr)
		}
	})

	t.Run("nil closer Close is a no-op", func(t *testing.T) {
		r := NewFuncReader(newCounter(1), nil)
		if err := r.Close(); err != nil {
			t.Errorf("Close: got %v, want nil", err)
		}
	})
}
