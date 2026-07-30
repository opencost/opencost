package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

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

	mu       sync.Mutex
	ranges   []string // x-ms-range values, in the order received
	gets     int
	failGET  func(n int) (status int, truncate bool) // nth GET (1-based) -> injected failure
	onGet    func()
	failHEAD bool
}

func newBlobServer(t *testing.T, content []byte) (*blobServer, *azblob.Client) {
	t.Helper()
	bs := &blobServer{content: content}

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
		bs.gets++
		n := bs.gets
		bs.ranges = append(bs.ranges, rangeHeader)
		failFn := bs.failGET
		onGet := bs.onGet
		bs.mu.Unlock()

		if onGet != nil {
			onGet()
		}

		start, end, err := parseRange(rangeHeader, len(bs.content))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		chunk := bs.content[start : end+1]

		if failFn != nil {
			if status, truncate := failFn(n); status != 0 {
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

func (bs *blobServer) requestedRanges() []string {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return append([]string(nil), bs.ranges...)
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

			sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize)
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

			if n := len(bs.requestedRanges()); n != tc.expectedBlocks {
				t.Errorf("issued %d ranged GETs (%v), want %d", n, bs.requestedRanges(), tc.expectedBlocks)
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

			sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize)
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

	sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize)
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
	got := bs.requestedRanges()
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

	if _, err := newStreamReader(context.Background(), client, testContainer, testBlobName, 1024); err == nil {
		t.Error("newStreamReader() error = nil, want an error when the blob properties cannot be read")
	}
}

func TestStreamReader_DownloadFailurePropagates(t *testing.T) {
	const blockSize = 128
	bs, client := newBlobServer(t, testContent(blockSize*3))
	// Fail every GET with a non-retriable status.
	bs.failGET = func(int) (int, bool) { return http.StatusBadRequest, false }

	sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize)
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
	bs.failGET = func(n int) (int, bool) {
		if n == 1 {
			return http.StatusPartialContent, true
		}
		return 0, false
	}

	sr, err := newStreamReader(context.Background(), client, testContainer, testBlobName, blockSize)
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

	sr, err := newStreamReader(ctx, client, testContainer, testBlobName, blockSize)
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
func TestNewStreamReader_UsesDefaultBlockSize(t *testing.T) {
	_, client := newBlobServer(t, testContent(16))

	sr, err := NewStreamReader(context.Background(), client, testContainer, testBlobName)
	if err != nil {
		t.Fatalf("NewStreamReader() error = %v", err)
	}
	if sr.blockSize != int64(defaultBlockSize) {
		t.Errorf("blockSize = %d, want %d", sr.blockSize, defaultBlockSize)
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
	if len(bs.requestedRanges()) == 0 {
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
