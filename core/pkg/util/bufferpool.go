package util

import (
	"math"
	"math/bits"
	"sync"
)

type bufferPool struct {
	// pools[i] holds buffers of capacity 1<<i.
	// In particular, Get(1) uses pools[0] for 1-byte buffers; larger requests
	// are rounded up to the next power-of-two bucket by poolIndex.
	pools [32]sync.Pool
}

func newBufferPool() *bufferPool {
	bp := new(bufferPool)

	for i := 0; i < 32; i++ {
		length := 1 << i
		bp.pools[i].New = func() any {
			return make([]byte, length)
		}
	}
	return bp
}

// poolIndex returns the pool index for a buffer of the given size.
// It returns bits.Len32(n), which equals ⌊log₂(n)⌋ + 1 for non-zero n.
// This is used to round up to the next power-of-two bucket:
func poolIndex(length int) int {
	return bits.Len32(uint32(length - 1))
}

// putIndex returns the pool index for returning a buffer with the given capacity.
// It is the inverse of poolIndex: given a capacity that was originally handed out
// by Get, it finds the pool that owns it.
//
// Because Get always returns buffers with capacity 1<<i, the capacity here will
// always be a power of two. bits.Len32(1<<i) = i+1, so we subtract 1 to recover i.
func putIndex(capacity int) int {
	return bits.Len32(uint32(capacity)) - 1
}

func (bp *bufferPool) Get(length int) []byte {
	if length <= 0 {
		return nil
	}

	// Beyond our pool range: allocate directly. The MaxInt32 guard also ensures
	// that poolIndex(length) <= 31, keeping all pool array accesses in bounds.
	if length > math.MaxInt32 {
		return make([]byte, length)
	}

	i := poolIndex(length)
	buf := bp.pools[i].Get().([]byte)
	return buf[:length]
}

func (bp *bufferPool) Put(buf []byte) {
	capacity := cap(buf)
	if capacity == 0 || capacity > math.MaxInt32 {
		return
	}

	i := putIndex(capacity)
	bp.pools[i].Put(buf[:cap(buf)])
}
