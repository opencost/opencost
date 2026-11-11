package azure

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const (
	defaultBlockSize = int(8 * 1024 * 1024) // 8MB

	// defaultPrefetchDepth is how many blocks may be downloading at once. The
	// reader holds at most depth+1 buffers, so the memory ceiling is
	// (depth+1) * blockSize -- 40MB at the defaults -- regardless of blob size.
	// Fetching a single block at a time leaves a large blob bound to one
	// connection's throughput, which is markedly slower than the parallel
	// download this reader replaces.
	defaultPrefetchDepth = 4

	// blockFetchTimeout bounds a single block download rather than the whole
	// blob, so it acts as a stall detector while leaving total transfer time
	// proportional to blob size. At the default 8MB block this tolerates
	// sustained throughput down to roughly 27KB/s before failing.
	blockFetchTimeout = 5 * time.Minute
)

// StreamReader is a prefetching streaming reader for Azure Blob Storage. It
// fetches the blob as a series of ranged reads, keeping up to depth blocks in
// flight so that downloading overlaps with parsing.
//
// Blocks are fetched lazily from Read, which cannot accept a context because
// it must satisfy io.Reader. The context supplied to the constructor is
// therefore retained for the lifetime of the reader and used for the downloads
// it triggers; a StreamReader is a single-use, request-scoped object and must
// not outlive that context.
type StreamReader struct {
	ctx       context.Context
	client    *azblob.Client
	container string
	blobName  string

	block   *bytes.Buffer     // current block, being consumed by Read
	pending []*streamingBlock // blocks in flight, in blob order
	free    []*bytes.Buffer   // drained buffers available for reuse

	position     int64 // bytes returned to the caller so far
	scheduled    int64 // offset of the next block to schedule
	size         int64
	blockSize    int64
	depth        int
	blockTimeout time.Duration
}

// NewStreamReader creates a new streaming reader for the specified blob.
// Cancelling ctx aborts any in-flight block download.
func NewStreamReader(ctx context.Context, client *azblob.Client, container string, blobName string) (*StreamReader, error) {
	return newStreamReader(ctx, client, container, blobName, int64(defaultBlockSize), defaultPrefetchDepth)
}

// newStreamReader is NewStreamReader with a caller-supplied block size and
// prefetch depth, which lets tests exercise block boundaries and concurrency
// without moving 8MB per block.
func newStreamReader(ctx context.Context, client *azblob.Client, container string, blobName string, blockSize int64, depth int) (*StreamReader, error) {
	if depth < 1 {
		depth = 1
	}

	sar := &StreamReader{
		ctx:          ctx,
		client:       client,
		container:    container,
		blobName:     blobName,
		blockSize:    blockSize,
		depth:        depth,
		blockTimeout: blockFetchTimeout,
	}

	// get the size of the blob
	blobClient := client.ServiceClient().NewContainerClient(container).NewBlobClient(blobName)
	gr, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return nil, err
	}

	sar.size = *gr.ContentLength

	return sar, nil
}

// See io.Reader.Read
func (r *StreamReader) Read(p []byte) (n int, err error) {
	if r.position >= r.size {
		return 0, io.EOF
	}

	// fetch the blocks on demand
	if r.block == nil || r.block.Len() == 0 {
		err := r.nextBlock()
		if err != nil {
			return 0, err
		}
	}

	// block.Next() constrains the bytes read even if len(p) is larger
	// than the rest of the block
	copied := copy(p, r.block.Next(len(p)))
	r.position += int64(copied)

	return copied, nil
}

// schedule tops the in-flight queue up to the prefetch depth, starting each
// block where the previous one ended. Buffers drained by Read are recycled so
// steady-state operation does not allocate.
func (r *StreamReader) schedule() {
	for len(r.pending) < r.depth && r.scheduled < r.size {
		var buffer *bytes.Buffer
		if last := len(r.free) - 1; last >= 0 {
			buffer = r.free[last]
			r.free = r.free[:last]
		}

		block := newStreamBlock(
			r.ctx,
			r.blockTimeout,
			r.client,
			r.container,
			r.blobName,
			buffer,
			r.scheduled,
			r.blockSize,
			r.size,
		)

		// capacity is the number of bytes this block will actually cover, which
		// is short of blockSize for the final block.
		r.scheduled += block.capacity
		r.pending = append(r.pending, block)
	}
}

// nextBlock waits for the oldest in-flight block, makes it current, and
// refills the queue behind it.
func (r *StreamReader) nextBlock() error {
	r.schedule()

	if len(r.pending) == 0 {
		// Read guards on position >= size, so the queue is only empty here if
		// the blob was truncated underneath us.
		return io.ErrUnexpectedEOF
	}

	block := r.pending[0]
	r.pending = r.pending[1:]

	if err := block.Wait(); err != nil {
		return err
	}

	// recycle the buffer we just finished with
	if r.block != nil {
		r.free = append(r.free, r.block)
	}
	r.block = block.buffer

	r.schedule()

	return nil
}

// streamingBlock is a buffered block of data that runs in a separate goroutine
// to allow the next block to download while the current block is being read.
type streamingBlock struct {
	client    *azblob.Client
	container string
	blob      string

	done   chan struct{}
	buffer *bytes.Buffer
	err    error

	start    int64
	capacity int64
}

// newStreamBlock creates a new buffered block of data the down the specific
// range of the blob. While the block download runs in a separate goroutine,
// we will never attempt to access the passed buffer until after the Wait()
// returns. This just ensures that we will never attempt to swap buffers
// mid-download.
func newStreamBlock(
	ctx context.Context,
	timeout time.Duration,
	client *azblob.Client,
	container string,
	blob string,
	buffer *bytes.Buffer,
	start int64,
	capacity int64,
	max int64,
) *streamingBlock {
	sb := &streamingBlock{
		client:    client,
		container: container,
		blob:      blob,
		done:      make(chan struct{}),
		buffer:    buffer,
		start:     start,
		capacity:  capacity,
	}

	// determine if we need to reallocate a new block buffer or if we can re-use the existing storage
	blockSize := capacity
	if start+blockSize > max {
		blockSize = max - start
	}

	// if the provided buffer is nil or the blockSize is different than the provided capacity, we need to reallocate
	// reallocation will likely happen once at the end of the stream
	if sb.buffer == nil || blockSize != capacity {
		sb.buffer = bytes.NewBuffer(make([]byte, 0, blockSize))
		sb.capacity = blockSize
	} else {
		sb.buffer.Reset()
	}

	// start a goroutine to fetch the block of data, close the done channel when the block
	// is fetched or an error occurs
	go func(block *streamingBlock) {
		// Bound this block rather than the whole blob, so a stalled transfer is
		// caught without capping how long a large blob may legitimately take.
		blockCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		opts := azblob.DownloadStreamOptions{
			Range: azblob.HTTPRange{
				Offset: block.start,
				Count:  block.capacity,
			},
		}

		resp, err := block.client.DownloadStream(blockCtx, block.container, block.blob, &opts)
		if err != nil {
			block.err = err
			close(block.done)
			return
		}

		retryOpts := &azblob.RetryReaderOptions{
			MaxRetries: 3,
		}

		var body io.ReadCloser = resp.NewRetryReader(blockCtx, retryOpts)
		_, err = io.Copy(block.buffer, body)
		if err != nil {
			block.err = err
			close(block.done)
			return
		}

		err = body.Close()
		if err != nil {
			block.err = err
			close(block.done)
			return
		}

		close(block.done)
	}(sb)

	return sb
}

// Wait blocks until the block is downloaded and returns any error that occurred.
func (sb *streamingBlock) Wait() error {
	<-sb.done

	return sb.err
}
