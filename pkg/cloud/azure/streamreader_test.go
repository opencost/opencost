package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const (
	testContainer = "billing"
	testBlobName  = "export/myExport/20260501-20260531/export_000019.csv"
)

// blobServer is a minimal stand-in for the Azure Blob REST surface that
// StreamReader depends on: HEAD for GetProperties and ranged GET for
// DownloadStream. It serves real byte ranges out of content so the tests
// exercise the actual SDK request/response path rather than a stubbed reader.
type blobServer struct {
	content []byte

	mu          sync.Mutex
	ranges      []string       // x-ms-range values, in arrival order
	attempts    map[string]int // per-range attempt count, for retry tests
	inFlight    int
	maxInFlight int

	// failGET is consulted per request with the requested range and how many
	// times that range has been requested. A non-zero status injects a failure;
	// truncate sends a partial body and aborts instead.
	failGET func(rangeHeader string, attempt int) (status int, truncate bool)
	// onGet runs on every GET before the response is produced.
	onGet func()
	// delay is held before responding, so overlapping requests are observable.
	delay    time.Duration
	failHEAD bool
	// blockForever makes GETs hang until teardown, to exercise stall handling.
	blockForever bool
	done         chan struct{}
}

func newBlobServer(t *testing.T, content []byte) (*blobServer, *azblob.Client) {
	t.Helper()
	bs := &blobServer{
		content:  content,
		attempts: map[string]int{},
		done:     make(chan struct{}),
	}
	var closeOnce sync.Once
	t.Cleanup(func() { closeOnce.Do(func() { close(bs.done) }) })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			if bs.failHEAD {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(bs.content)))
			w.Header().Set("ETag", `"test-etag"`)
			w.Header().Set("Last-Modified", "Mon, 25 May 2026 12:19:00 GMT")
			w.WriteHeader(http.StatusOK)
			return
		}

		rangeHeader := r.Header.Get("x-ms-range")

		bs.mu.Lock()
		bs.ranges = append(bs.ranges, rangeHeader)
		bs.attempts[rangeHeader]++
		attempt := bs.attempts[rangeHeader]
		bs.inFlight++
		if bs.inFlight > bs.maxInFlight {
			bs.maxInFlight = bs.inFlight
		}
		failFn, onGet, delay, blockForever := bs.failGET, bs.onGet, bs.delay, bs.blockForever
		bs.mu.Unlock()

		defer func() {
			bs.mu.Lock()
			bs.inFlight--
			bs.mu.Unlock()
		}()

		if onGet != nil {
			onGet()
		}
		if blockForever {
			select {
			case <-bs.done:
			case <-r.Context().Done():
			}
			return
		}
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}

		start, end, err := parseRange(rangeHeader, len(bs.content))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		chunk := bs.content[start : end+1]

		if failFn != nil {
			if status, truncate := failFn(rangeHeader, attempt); status != 0 {
				if !truncate {
					w.WriteHeader(status)
					return
				}
				// Announce the full length, send a prefix, then abort the
				// connection so the SDK's RetryReader must re-request.
				w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
				w.Header().Set("ETag", `"test-etag"`)
				w.WriteHeader(http.StatusPartialContent)
				w.Write(chunk[:len(chunk)/2])
				panic(http.ErrAbortHandler)
			}
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
		w.Header().Set("ETag", `"test-etag"`)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(bs.content)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(chunk)
	}))
	t.Cleanup(srv.Close)

	client, err := azblob.NewClientWithNoCredential(srv.URL, nil)
	if err != nil {
		t.Fatalf("creating azblob client: %v", err)
	}
	return bs, client
}

// uniqueSortedRanges returns the distinct ranges requested, sorted by start
// offset. Prefetching means arrival order is not deterministic.
func (bs *blobServer) uniqueSortedRanges() []string {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	seen := map[string]bool{}
	out := []string{}
	for _, r := range bs.ranges {
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		si, _, _ := parseRange(out[i], len(bs.content))
		sj, _, _ := parseRange(out[j], len(bs.content))
		return si < sj
	})
	return out
}

func (bs *blobServer) peakConcurrency() int {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return bs.maxInFlight
}

// parseRange parses an Azure "bytes=start-end" range header, where end is inclusive.
func parseRange(header string, size int) (int, int, error) {
	if header == "" {
		return 0, size - 1, nil
	}
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, fmt.Errorf("unsupported range header %q", header)
	}
	startStr, endStr, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, fmt.Errorf("malformed range %q", header)
	}
	start, err := strconv.Atoi(startStr)
	if err != nil {
		return 0, 0, err
	}
	if endStr == "" {
		return start, size - 1, nil
	}
	end, err := strconv.Atoi(endStr)
	if err != nil {
		return 0, 0, err
	}
	if end > size-1 {
		end = size - 1
	}
	if start > end {
		return 0, 0, fmt.Errorf("range %q outside content of %d bytes", header, size)
	}
	return start, end, nil
}

// testContent returns deterministic, position-dependent bytes so that a block
// assembled from the wrong offset is detectable rather than accidentally equal.
func testContent(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func TestStreamReader_ReadsBlobExactly(t *testing.T) {
	const blockSize = 1024

	testCases := map[string]struct {
		size           int
		expectedBlocks int
	}{
		"empty blob":                  {size: 0, expectedBlocks: 0},
		"single byte":                 {size: 1, expectedBlocks: 1},
		"smaller than one block":      {size: blockSize - 1, expectedBlocks: 1},
		"exactly one block":           {size: blockSize, expectedBlocks: 1},
		"one block plus one byte":     {size: blockSize + 1, expectedBlocks: 2},
		"exactly three blocks":        {size: blockSize * 3, expectedBlocks: 3},
		"three blocks plus remainder": {size: blockSize*3 + 7, expectedBlocks: 4},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			content := testContent(tc.size)
			bs, client := newBlobServer(t, content)

			sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize, defaultPrefetchDepth)
			if err != nil {
				t.Fatalf("newStreamReader() error = %v", err)
			}

			got, err := io.ReadAll(sr)
			if err != nil {
				t.Fatalf("io.ReadAll() error = %v", err)
			}
			if len(got) != tc.size {
				t.Fatalf("read %d bytes, want %d", len(got), tc.size)
			}
			if string(got) != string(content) {
				t.Error("streamed content does not match the blob")
			}

			if n := len(bs.uniqueSortedRanges()); n != tc.expectedBlocks {
				t.Errorf("issued %d ranged GETs (%v), want %d", n, bs.uniqueSortedRanges(), tc.expectedBlocks)
			}
		})
	}
}

// The reader must work when the caller's buffer is not aligned to the block
// size, which is the case for csv.Reader via bufio.
func TestStreamReader_HandlesUnalignedReads(t *testing.T) {
	const blockSize = 512
	content := testContent(blockSize*4 + 33)

	for _, readSize := range []int{1, 7, 511, 512, 513, 4096} {
		t.Run(fmt.Sprintf("read buffer %d", readSize), func(t *testing.T) {
			_, client := newBlobServer(t, content)

			sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize, defaultPrefetchDepth)
			if err != nil {
				t.Fatalf("newStreamReader() error = %v", err)
			}

			var out []byte
			buf := make([]byte, readSize)
			for {
				n, err := sr.Read(buf)
				out = append(out, buf[:n]...)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("Read() error = %v", err)
				}
			}

			if string(out) != string(content) {
				t.Errorf("content mismatch with read buffer of %d bytes", readSize)
			}
		})
	}
}

// Ranges must tile the blob exactly: contiguous, non-overlapping, and stopping
// at the final byte. An off-by-one here silently corrupts billing CSVs.
func TestStreamReader_RequestsContiguousRanges(t *testing.T) {
	const blockSize = 256
	content := testContent(blockSize*3 + 100)
	bs, client := newBlobServer(t, content)

	sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize, defaultPrefetchDepth)
	if err != nil {
		t.Fatalf("newStreamReader() error = %v", err)
	}
	if _, err := io.ReadAll(sr); err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}

	want := []string{
		"bytes=0-255",
		"bytes=256-511",
		"bytes=512-767",
		"bytes=768-867",
	}
	got := bs.uniqueSortedRanges()
	if len(got) != len(want) {
		t.Fatalf("requested ranges = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("range %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestStreamReader_PropertiesFailurePropagates(t *testing.T) {
	bs, client := newBlobServer(t, testContent(10))
	bs.failHEAD = true

	if _, err := newStreamReader(context.Background(), client, testContainer, testBlobName, 1024, defaultPrefetchDepth); err == nil {
		t.Error("newStreamReader() error = nil, want an error when the blob properties cannot be read")
	}
}

func TestStreamReader_DownloadFailurePropagates(t *testing.T) {
	const blockSize = 128
	bs, client := newBlobServer(t, testContent(blockSize*3))
	// Fail every GET with a non-retriable status.
	bs.failGET = func(string, int) (int, bool) { return http.StatusBadRequest, false }

	sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize, defaultPrefetchDepth)
	if err != nil {
		t.Fatalf("newStreamReader() error = %v", err)
	}

	if _, err := io.ReadAll(sr); err == nil {
		t.Error("io.ReadAll() error = nil, want the download failure to surface")
	}
}

// A connection dropped mid-block must be retried transparently by the SDK's
// RetryReader rather than truncating the CSV.
func TestStreamReader_RetriesTruncatedBlock(t *testing.T) {
	const blockSize = 256
	content := testContent(blockSize * 2)
	bs, client := newBlobServer(t, content)
	// Abort the first GET partway through; later attempts succeed.
	bs.failGET = func(_ string, attempt int) (int, bool) {
		if attempt == 1 {
			return http.StatusPartialContent, true
		}
		return 0, false
	}

	sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize, defaultPrefetchDepth)
	if err != nil {
		t.Fatalf("newStreamReader() error = %v", err)
	}

	got, err := io.ReadAll(sr)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v, want the truncated block to be retried", err)
	}
	if string(got) != string(content) {
		t.Error("content mismatch after a retried block")
	}
}

// Regression: block downloads previously ran on context.Background(), so they
// could neither be cancelled nor bounded by the caller's deadline.
func TestStreamReader_HonoursContextCancellation(t *testing.T) {
	const blockSize = 128
	content := testContent(blockSize * 8)

	ctx, cancel := context.WithCancel(context.Background())
	bs, client := newBlobServer(t, content)

	// Cancel as soon as the first block download is in flight.
	var once sync.Once
	bs.onGet = func() { once.Do(cancel) }

	sr, err := newStreamReader(ctx, client, testContainer, testBlobName, blockSize, defaultPrefetchDepth)
	if err != nil {
		// Cancellation during GetProperties is also an acceptable outcome.
		if errors.Is(err, context.Canceled) {
			return
		}
		t.Fatalf("newStreamReader() error = %v", err)
	}

	_, err = io.ReadAll(sr)
	if err == nil {
		t.Fatal("io.ReadAll() error = nil, want the cancelled context to abort the stream")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want it to wrap context.Canceled", err)
	}
	_ = cancel
}

// NewStreamReader is the exported constructor and must default to the 8MB
// block size rather than a zero-length block.
func TestNewStreamReader_UsesDefaults(t *testing.T) {
	_, client := newBlobServer(t, testContent(16))

	sr, err := NewStreamReader(context.Background(), client, testContainer, testBlobName)
	if err != nil {
		t.Fatalf("NewStreamReader() error = %v", err)
	}
	if sr.blockSize != int64(defaultBlockSize) {
		t.Errorf("blockSize = %d, want %d", sr.blockSize, defaultBlockSize)
	}
	if sr.depth != defaultPrefetchDepth {
		t.Errorf("depth = %d, want %d", sr.depth, defaultPrefetchDepth)
	}
	if sr.blockTimeout != blockFetchTimeout {
		t.Errorf("blockTimeout = %v, want %v", sr.blockTimeout, blockFetchTimeout)
	}
}

// StreamBlob is the entry point the billing parser uses; it must bind the
// connection's container and honour the caller's context.
func TestStorageConnection_StreamBlob(t *testing.T) {
	content := testContent(4096)
	bs, client := newBlobServer(t, content)

	sc := &StorageConnection{
		StorageConfiguration: StorageConfiguration{Container: testContainer},
	}

	sr, err := sc.StreamBlob(context.Background(), testBlobName, client)
	if err != nil {
		t.Fatalf("StreamBlob() error = %v", err)
	}
	if sr.container != testContainer {
		t.Errorf("container = %q, want %q", sr.container, testContainer)
	}

	got, err := io.ReadAll(sr)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}
	if string(got) != string(content) {
		t.Error("streamed content does not match the blob")
	}
	if len(bs.uniqueSortedRanges()) == 0 {
		t.Error("no ranged GETs were issued")
	}
}

func TestStorageConnection_StreamBlobRespectsCancelledContext(t *testing.T) {
	_, client := newBlobServer(t, testContent(4096))

	sc := &StorageConnection{
		StorageConfiguration: StorageConfiguration{Container: testContainer},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := sc.StreamBlob(ctx, testBlobName, client); !errors.Is(err, context.Canceled) {
		t.Errorf("StreamBlob() error = %v, want context.Canceled", err)
	}
}

// End-to-end proof that CSV rows are not required to fit within a block.
// StreamReader returns a short read at each block boundary and csv.Reader's
// bufio wrapper keeps calling Read, so records, fields and the header line all
// reassemble across boundaries. Block sizes here are far smaller than the
// fixture's ~959 byte header and ~716 byte rows, so every record is split
// several times, frequently mid-field.
func TestStreamReader_ParsesCSVRecordsSpanningBlocks(t *testing.T) {
	const expectedRows = 5
	start := time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 11, 30, 0, 0, 0, 0, time.UTC)

	for _, fileName := range []string{"test_azure_billing.csv", "test_azure_billing.csv.gz"} {
		content, err := os.ReadFile(valueCasesPath + fileName)
		if err != nil {
			t.Fatalf("reading fixture %s: %v", fileName, err)
		}

		// 1 byte forces a boundary between every single byte; the larger sizes
		// land boundaries at varied offsets within records and fields.
		for _, blockSize := range []int64{1, 7, 64, 100, 512, 1000, 4096} {
			t.Run(fmt.Sprintf("%s block %d", fileName, blockSize), func(t *testing.T) {
				_, client := newBlobServer(t, content)

				sr, err := newStreamReader(context.Background(), client, testContainer, fileName, blockSize, defaultPrefetchDepth)
				if err != nil {
					t.Fatalf("newStreamReader() error = %v", err)
				}

				asbp := &AzureStorageBillingParser{}
				var rows int
				err = asbp.processStreamBillingData(sr, fileName, start, end, func(abv *BillingRowValues) error {
					if abv == nil {
						t.Error("received nil BillingRowValues")
					}
					rows++
					return nil
				})
				if err != nil {
					t.Fatalf("processStreamBillingData() error = %v", err)
				}

				if rows != expectedRows {
					t.Errorf("parsed %d rows, want %d — records did not survive block boundaries", rows, expectedRows)
				}
			})
		}
	}
}

// Prefetching is the whole point of the double buffer: without it a multi-GB
// blob is fetched one block at a time on a single connection, which is far
// slower than the parallel download it replaces.
func TestStreamReader_PrefetchesConcurrently(t *testing.T) {
	const blockSize = 128
	const depth = 4
	content := testContent(blockSize * 12)

	bs, client := newBlobServer(t, content)
	// Hold each response briefly so overlapping requests are observable.
	bs.delay = 25 * time.Millisecond

	sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize, depth)
	if err != nil {
		t.Fatalf("newStreamReader() error = %v", err)
	}
	got, err := io.ReadAll(sr)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}
	if string(got) != string(content) {
		t.Fatal("content mismatch under concurrent prefetch")
	}

	peak := bs.peakConcurrency()
	if peak < 2 {
		t.Errorf("peak concurrent fetches = %d, want > 1 (blocks are not being prefetched)", peak)
	}
	if peak > depth {
		t.Errorf("peak concurrent fetches = %d, want <= depth %d (prefetch is unbounded)", peak, depth)
	}
}

// Prefetch depth bounds memory: at most depth blocks are in flight, so the
// reader holds at most depth+1 buffers regardless of blob size.
func TestStreamReader_PrefetchDepthIsBounded(t *testing.T) {
	const blockSize = 64
	const depth = 2
	content := testContent(blockSize * 20)

	bs, client := newBlobServer(t, content)
	bs.delay = 10 * time.Millisecond

	sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize, depth)
	if err != nil {
		t.Fatalf("newStreamReader() error = %v", err)
	}
	if _, err := io.ReadAll(sr); err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}

	if peak := bs.peakConcurrency(); peak > depth {
		t.Errorf("peak concurrent fetches = %d, want <= %d", peak, depth)
	}
}

// A stalled block must fail on its own deadline rather than hanging the
// ingestion cycle. The deadline is per block, so total runtime scales with
// blob size instead of being capped by a single whole-blob timeout.
func TestStreamReader_BlockFetchTimeout(t *testing.T) {
	const blockSize = 128
	bs, client := newBlobServer(t, testContent(blockSize*4))
	bs.blockForever = true

	sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize, defaultPrefetchDepth)
	if err != nil {
		t.Fatalf("newStreamReader() error = %v", err)
	}
	sr.blockTimeout = 150 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(sr)
		done <- readErr
	}()

	select {
	case readErr := <-done:
		if readErr == nil {
			t.Fatal("io.ReadAll() error = nil, want the stalled block to time out")
		}
		if !errors.Is(readErr, context.DeadlineExceeded) {
			t.Errorf("error = %v, want it to wrap context.DeadlineExceeded", readErr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Read did not return; the per-block deadline is not being applied")
	}
}
