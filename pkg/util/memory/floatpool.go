package memory

import (
	"fmt"
	"sync"
	"unsafe"
)

// FloatPool is a float64 buffer capable of leasing out slices for temporary use.
// This can reduce total heap allocations for critcial code paths. This type is
// thread safe.
type FloatPool struct {
	buf         []*float64
	allocations AllocationList
	pos         int
	lock        *sync.Mutex
}

// Create a new float pool with a default size buffer. The buffer will double size
// each time it's required to grow.
func NewFloatPool(size int) *FloatPool {
	return &FloatPool{
		buf:         make([]*float64, size),
		pos:         0,
		allocations: NewAllocationList(),
		lock:        new(sync.Mutex),
	}
}

// Make creates a new slice allocation from the pool and returns it.
// Any slices created by the pool should be explicitly returned to
// the pool once it is no longer used. Ensure any data that must persist
// is copied before returned. Failure to return a slice can result in
// leaks and unnecessary pooled allocations.
func (fp *FloatPool) Make(length int) []*float64 {
	fp.lock.Lock()
	defer fp.lock.Unlock()

	// find the next allocation location, resize buffer if necessary
	next := fp.allocations.Next(fp.pos, len(fp.buf), length)

	// if the next allocation location + length is larger than the buffer,
	// grow the buffer
	if next+length >= len(fp.buf) {
		newBuf := make([]*float64, len(fp.buf)*2)
		copy(newBuf, fp.buf)
		fp.buf = newBuf
	}

	// create the slice from subset of buf
	sl := fp.buf[next : next+length]

	// insert allocation record, advance search position
	fp.allocations.Add(next, length, fp.addressFor(sl))
	fp.pos = next + length + 1

	return sl
}

// Return accepts a slice allocation that was created by calling Make on
// this pool instance. Ensure any data that must persist from the returned
// slice is copied. Failure to return a slice can result in leaks and
// unnecessary additional pooled allocations.
func (fp *FloatPool) Return(v []*float64) {
	fp.lock.Lock()
	defer fp.lock.Unlock()

	removed := fp.allocations.Remove(fp.addressFor(v))
	if removed == nil {
		fmt.Printf("Error: Failed to locate allocated slice\n")
		return
	}

	// set the search start at the lowest returned
	if removed.Offset < fp.pos {
		fp.pos = removed.Offset
	}

	// nil out returned slice
	fp.clear(v)
}

// nils out indices of the slice parameter
func (fp *FloatPool) clear(v []*float64) {
	for i := range v {
		v[i] = nil
	}
}

// addressFor finds the address for the slice
func (fp *FloatPool) addressFor(v []*float64) uintptr {
	return uintptr(unsafe.Pointer(&v[0]))
}
