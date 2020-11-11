package memory

import (
	"container/list"
	"fmt"
	"sync"
	"unsafe"
)

// AllocatorType is a way to specify or identify the type of allocator to use.
type AllocatorType uint8

const (
	HeapAllocatorType AllocatorType = iota
	PoolAllocatorType
)

//--------------------------------------------------------------------------
//  FloatSliceAllocator
//--------------------------------------------------------------------------

// FloatAllocator defines an implementation prototype for an object capable of
// allocating and deleting *float64 slices.
type FloatSliceAllocator interface {
	// Make allocates a []*float64 using the provided length.
	Make(length int) []*float64

	// Deletes, or recycles the []*float64 parameter.
	Delete(buffer []*float64)
}

//--------------------------------------------------------------------------
//  FloatHeapAllocator
//--------------------------------------------------------------------------

// FloatHeapAllocator is a passthrough to Go's heap allocation.
type FloatHeapAllocator struct{}

// NewFloatHeapAllocator Creates a new float slice allocator which uses the go runtime heap allocation via make().
func NewFloatHeapAllocator() FloatSliceAllocator {
	return &FloatHeapAllocator{}
}

// Make allocates a []*float64 using the provided length.
func (fha *FloatHeapAllocator) Make(length int) []*float64 {
	return make([]*float64, length)
}

// Deletes, or recycles the []*float64 parameter.
func (fha *FloatHeapAllocator) Delete(buffer []*float64) {
	// No-op
}

//--------------------------------------------------------------------------
//  FloatPoolAllocator
//--------------------------------------------------------------------------

// FloatPoolAllocator is a float64 buffer capable of leasing out slices for temporary use.
// This can reduce total heap allocations for critcial code paths. This type is also thread
// safe.
type FloatPoolAllocator struct {
	buf         []*float64
	allocations AllocationList
	pos         int
	start       *list.Element
	lock        *sync.Mutex
}

// NewFloatPoolAllocator Creates a new float pool allocator with an initial size buffer. The buffer will double size
// each time it's required to grow.
func NewFloatPoolAllocator(initialSize int) FloatSliceAllocator {
	return &FloatPoolAllocator{
		buf:         make([]*float64, initialSize),
		pos:         0,
		start:       nil,
		allocations: NewAllocationList(),
		lock:        new(sync.Mutex),
	}
}

// Make creates a new slice allocation from the pool and returns it.
// Any slices created by the pool should be explicitly returned to
// the pool once it is no longer used. Ensure any data that must persist
// is copied before returned. Failure to return a slice can result in
// leaks and unnecessary pooled allocations.
func (fp *FloatPoolAllocator) Make(length int) []*float64 {
	fp.lock.Lock()
	defer fp.lock.Unlock()

	// find the next allocation location, resize buffer if necessary
	next, ele := fp.allocations.Next(fp.start, fp.pos, len(fp.buf), length)

	// if the next allocation location + length is larger than the buffer,
	// grow the buffer
	buffLength := len(fp.buf)
	if next+length >= buffLength {
		for next+length >= buffLength {
			buffLength = buffLength * 2
		}

		newBuf := make([]*float64, buffLength)
		copy(newBuf, fp.buf)
		fp.buf = newBuf
	}

	// create the slice from subset of buf
	sl := fp.buf[next : next+length]

	// insert allocation record, advance search position
	ele = fp.allocations.InsertBefore(&Allocation{
		Offset: next,
		Size:   length,
		Addr:   fp.addressFor(sl),
	}, ele)

	fp.pos = next + length + 1
	fp.start = ele

	return sl
}

// Delete accepts a slice allocation that was created by calling Make on
// this pool instance. Ensure any data that must persist from the returned
// slice is copied. Failure to return a slice can result in leaks and
// unnecessary additional pooled allocations.
func (fp *FloatPoolAllocator) Delete(v []*float64) {
	fp.lock.Lock()
	defer fp.lock.Unlock()

	removed, next := fp.allocations.Remove(fp.addressFor(v))
	if removed == nil {
		fmt.Printf("Error: Failed to locate allocated slice\n")
		return
	}

	// set the search start at the lowest returned
	if removed.Offset < fp.pos {
		fp.pos = removed.Offset
		fp.start = next
	}

	// nil out returned slice
	fp.clear(v)
}

// nils out indices of the slice parameter
func (fp *FloatPoolAllocator) clear(v []*float64) {
	for i := range v {
		v[i] = nil
	}
}

// addressFor finds the address for the slice
func (fp *FloatPoolAllocator) addressFor(v []*float64) uintptr {
	return uintptr(unsafe.Pointer(&v[0]))
}
