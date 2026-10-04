package reader

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// jsonLinesOf marshals each of vals to its own line, returning a JSONL reader.
func jsonLinesOf[T any](t *testing.T, vals []T, delimiter string) io.Reader {
	t.Helper()
	var sb strings.Builder
	for _, v := range vals {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshaling test data: %v", err)
		}
		sb.Write(b)
		if delimiter != "" {
			sb.WriteString(delimiter)
		}
	}
	return strings.NewReader(sb.String())
}

// recordingCloser wraps a reader and records whether Close was called, returning
// a configurable error from Close. Used to exercise the io.Closer branch.
type recordingCloser struct {
	io.Reader
	closed   bool
	closeErr error
}

func (rc *recordingCloser) Close() error {
	rc.closed = true
	return rc.closeErr
}

// TestJSONLinesReader_FoldedTerminalSignal proves the JSONL reader honors the
// same folded-io.EOF contract as the array reader (folds when the final batch is
// short; defers to (0, io.EOF) on an exact fill).
func TestJSONLinesReader_FoldedTerminalSignal(t *testing.T) {
	tests := []struct {
		name      string
		items     int
		bufSize   int
		wantReads []struct {
			n   int
			eof bool
		}
	}{
		{"buffer larger than items", 3, 10, []struct {
			n   int
			eof bool
		}{{3, true}}},
		{"buffer exactly fits items", 5, 5, []struct {
			n   int
			eof bool
		}{{5, false}, {0, true}}},
		{"buffer unevenly divides items", 7, 3, []struct {
			n   int
			eof bool
		}{{3, false}, {3, false}, {1, true}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewJSONLinesReader[int](jsonLinesOf(t, seq(tc.items), "\n"))
			dst := make([]int, tc.bufSize)
			for i, want := range tc.wantReads {
				n, err := r.Read(context.Background(), dst)
				if n != want.n {
					t.Errorf("read %d: got n=%d, want %d", i, n, want.n)
				}
				if gotEOF := errors.Is(err, io.EOF); gotEOF != want.eof {
					t.Errorf("read %d: got eof=%v (err=%v), want eof=%v", i, gotEOF, err, want.eof)
				}
				if !want.eof && err != nil {
					t.Errorf("read %d: unexpected error: %v", i, err)
				}
			}
		})
	}
}

// TestJSONLinesReader_Whitespace verifies newline separation, blank lines,
// trailing whitespace, CRLF, and empty input are all handled.
func TestJSONLinesReader_Whitespace(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []int
	}{
		{"one per line", "1\n2\n3\n", []int{1, 2, 3}},
		{"no trailing newline", "1\n2\n3", []int{1, 2, 3}},
		{"space separated", "1 2 3", []int{1, 2, 3}},
		{"crlf", "1\r\n2\r\n3\r\n", []int{1, 2, 3}},
		{"blank lines", "1\n\n\n2\n", []int{1, 2}},
		{"trailing whitespace", "1\n2\n   \n", []int{1, 2}},
		{"empty", "", nil},
		{"all whitespace", "   \n  ", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewJSONLinesReader[int](strings.NewReader(tc.input))
			dst := make([]int, 2)
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
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("item %d: got %d, want %d", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestJSONLinesReader_TruncatedValue verifies a truncated trailing value is a
// terminal, sticky, non-EOF error returned after the valid prefix — unlike the
// array reader, whose lookahead swallows truncation.
func TestJSONLinesReader_TruncatedValue(t *testing.T) {
	r := NewJSONLinesReader[int](strings.NewReader("1\n2\n{\"id\":"))
	dst := make([]int, 2)

	if n, err := r.Read(context.Background(), dst); n != 2 || err != nil {
		t.Fatalf("first read: got (%d, %v), want (2, nil)", n, err)
	}

	n, err := r.Read(context.Background(), dst)
	if n != 0 || err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("second read: got (%d, %v), want (0, non-EOF error)", n, err)
	}
	// Sticky.
	if _, err2 := r.Read(context.Background(), dst); err2 != err {
		t.Errorf("third read: err=%v, want sticky %v", err2, err)
	}
}

// TestJSONLinesReader_MalformedValue verifies a wrong-type value mid-stream is a
// terminal, sticky, non-EOF error.
func TestJSONLinesReader_MalformedValue(t *testing.T) {
	r := NewJSONLinesReader[int](strings.NewReader("1\n{}\n3"))
	dst := make([]int, 4)

	// The batch stops at the malformed second value, returning the prefix.
	n, err := r.Read(context.Background(), dst)
	if n != 1 || err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("read: got (%d, %v), want (1, non-EOF error)", n, err)
	}
	if dst[0] != 1 {
		t.Errorf("prefix item: got %d, want 1", dst[0])
	}
	if _, err2 := r.Read(context.Background(), dst); err2 != err {
		t.Errorf("next read: err=%v, want sticky %v", err2, err)
	}
}

// TestJSONLinesReader_StructValues exercises the realistic case: one JSON object
// per line decoded into pointer elements across multiple batches.
func TestJSONLinesReader_StructValues(t *testing.T) {
	type item struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	src := []*item{{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"}}

	r := NewJSONLinesReader[*item](jsonLinesOf(t, src, "\n"))
	dst := make([]*item, 2)

	var got []*item
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

	if len(got) != len(src) {
		t.Fatalf("got %d items, want %d", len(got), len(src))
	}
	for i, p := range got {
		if p == nil || p.ID != src[i].ID || p.Name != src[i].Name {
			t.Errorf("item %d: got %v, want %+v", i, p, *src[i])
		}
	}
}

// TestJSONLinesReader_StructValues_NoNewline tests a file without newlines
// between streamed JSON objects
func TestJSONLinesReader_StructValues_NoNewline(t *testing.T) {
	type item struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	src := []*item{{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"}}

	r := NewJSONLinesReader[*item](jsonLinesOf(t, src, ""))
	dst := make([]*item, 2)

	var got []*item
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

	if len(got) != len(src) {
		t.Fatalf("got %d items, want %d", len(got), len(src))
	}
	for i, p := range got {
		if p == nil || p.ID != src[i].ID || p.Name != src[i].Name {
			t.Errorf("item %d: got %v, want %+v", i, p, *src[i])
		}
	}
}

// TestJSONLinesReader_Close verifies the io.Closer source is closed.
func TestJSONLinesReader_Close(t *testing.T) {
	rc := &recordingCloser{Reader: strings.NewReader("1\n2\n")}
	r := NewJSONLinesReader[int](rc)
	if err := r.Close(); err != nil {
		t.Errorf("Close: got err=%v, want nil", err)
	}
	if !rc.closed {
		t.Error("underlying source was not closed")
	}
}

// Simple struct with various data types for benchmarking JSONLinesReader
type benchRecord struct {
	ID     string            `json:"id"`
	Name   string            `json:"name"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels"`
}

// writeBenchJSONL writes n JSON-Lines records to path (one value per line).
func writeBenchJSONL(tb testing.TB, path string, n int) {
	tb.Helper()

	f, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w) // Encode appends a newline after each value.
	for i := 0; i < n; i++ {
		rec := benchRecord{
			ID:     uuid.NewString(),
			Name:   fmt.Sprintf("name-%d", i),
			Value:  rand.Float64(),
			Labels: map[string]string{"foo": "bar", "baz": "bat"},
		}
		if err := enc.Encode(&rec); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		tb.Fatal(err)
	}
}

// streamAll reads path in batches of batchSize, discarding every batch (dst is
// reused, nothing retained), and returns the number of records seen.
func streamAll(tb testing.TB, path string, batchSize int) int {
	tb.Helper()

	f, err := os.Open(path)
	if err != nil {
		tb.Fatal(err)
	}

	r := NewJSONLinesReader[benchRecord](f)
	dst := make([]benchRecord, batchSize)
	total := 0
	for {
		n, err := r.Read(context.Background(), dst)
		total += n
		// Discard: the next Read overwrites dst; no record outlives its batch.
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			tb.Fatal(err)
		}
	}
	if err := r.Close(); err != nil {
		tb.Fatal(err)
	}
	return total
}

const (
	benchRecords   = 100_000
	benchBatchSize = 1_000
)

// BenchmarkJSONLinesReader measures wall time and allocation churn to stream a
// 50k-line JSONL file 1000 records at a time. With -benchmem, B/op is the TOTAL
// bytes allocated per pass (churn, scales with record count) — not resident
// memory. See BenchmarkJSONLinesReaderResident for the live-heap bound.
func BenchmarkJSONLinesReader(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.jsonl") // disposable; auto-removed
	writeBenchJSONL(b, path, benchRecords)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if got := streamAll(b, path, benchBatchSize); got != benchRecords {
			b.Fatalf("streamed %d records, want %d", got, benchRecords)
		}
	}
}

// BenchmarkJSONLinesReaderResident measures PEAK live heap (HeapInuse) while
// streaming, which is what the streaming design is meant to bound: it should
// stay ~flat at one batch's worth of records regardless of file size. Note the
// per-batch runtime.ReadMemStats sampling perturbs timing, so read ns/op from
// BenchmarkJSONLinesReader, not this one.
func BenchmarkJSONLinesReaderResident(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.jsonl")
	writeBenchJSONL(b, path, benchRecords)

	b.ResetTimer()

	var peakHeapInuse uint64
	for i := 0; i < b.N; i++ {
		f, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		r := NewJSONLinesReader[benchRecord](f)
		dst := make([]benchRecord, benchBatchSize)
		for {
			n, err := r.Read(context.Background(), dst)
			_ = n

			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			if ms.HeapInuse > peakHeapInuse {
				peakHeapInuse = ms.HeapInuse
			}

			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		r.Close()
	}

	b.ReportMetric(float64(peakHeapInuse)/1024, "peakHeapKB")
}
