package util

import (
	"math"
	"math/bits"
	"sync"
)

type bufferPool struct {
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

// index on the min number of bits required to store the byte data up to 32 bits.
func nextIndex(length int) int {
	return bits.Len32(uint32(length))
}

// the previous index for a provided length
func prevIndex(length int) int {
	next := nextIndex(length)
	if uint32(length) == (1 << uint32(next)) {
		return next
	}
	return next - 1
}

func (bp *bufferPool) Get(length int) []byte {
	if length <= 0 {
		return nil
	}

	// if it's beyond our pool bounds, just allocate and return
	if length > math.MaxInt32 {
		return make([]byte, length)
	}

	i := nextIndex(length)
	if entry := bp.pools[i].Get(); entry != nil {
		bytes := entry.([]byte)
		bytes = bytes[:length]
		return bytes
	}

	// should never get here, as there should always be an entry
	// coming from the pool
	return make([]byte, 1<<i)[:length]
}

func (bp *bufferPool) Put(buf []byte) {
	capacity := cap(buf)
	if capacity == 0 || capacity > math.MaxInt32 {
		return
	}

	i := prevIndex(capacity)
	bp.pools[i].Put(buf)
}
