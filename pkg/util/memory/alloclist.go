package memory

import (
	"sort"
)

// Allocation represents a subset of a buffer
type Allocation struct {
	Offset int
	Size   int
	Addr   uintptr
}

// AllocationList is an implementation prototype for a list data structure that
// contains. Note that this contract does not guarantee thread-safety.
type AllocationList interface {
	// Add inserts an allocation into the list according to it's offset.
	// Note: This function does not validate the insertion. Next() should be
	// called to find the offset, followed my a call to Add().
	Add(offset, size int, address uintptr)

	// AllocationByAddress finds an Allocation matching the provided address and returns
	// it if it exists. Otherwise, nil is returned.
	Remove(address uintptr) *Allocation

	// ClosestTo returns the first allocation with an offset greater or equal
	// to the provided offset. It returns nil if such an Allocation does not
	// exist.
	ClosestTo(offset int) *Allocation

	// Next walks the allocations assuming an offset of start with a limitation of end. It
	// returns the next available offset and whether or not the next offset + size exceeds
	// the end boundary.
	Next(start, end, size int) int
}

// sliceAllocationList is an implementation of AllocationList using a slice
// of *Allocation
type sliceAllocationList []*Allocation

// NewAllocationList creates a new AllocationList implementation
func NewAllocationList() AllocationList {
	return &sliceAllocationList{}
}

// ClosestTo returns the first allocation with an offset greater or equal
// to the provided offset. It returns nil if such an Allocation does not
// exist.
func (sal *sliceAllocationList) ClosestTo(offset int) *Allocation {
	for _, a := range *sal {
		if a.Offset >= offset {
			return a
		}
	}
	return nil
}

// Add inserts an allocation into the list according to it's offset.
// Note: This function does not validate the insertion. Next() should be
// called to find the offset, followed my a call to Add().
func (sal *sliceAllocationList) Add(offset, size int, address uintptr) {
	alloc := &Allocation{
		Offset: offset,
		Size:   size,
		Addr:   address,
	}

	// TODO: [bolt] Append and sort is actually pretty fast for this specific application,
	// TODO: [bolt] but we could likely do better with insertion and a linked list.
	*sal = append(*sal, alloc)
	sort.Slice(*sal, sal.less)
}

// AllocationByAddress finds an Allocation matching the provided address and returns
// it if it exists. Otherwise, nil is returned.
func (sal *sliceAllocationList) Remove(address uintptr) *Allocation {
	var allocation *Allocation = nil
	var index int = -1

	for i, alloc := range *sal {
		if alloc.Addr == address {
			allocation = alloc
			index = i
			break
		}
	}

	// Not found, return nil allocation
	if index < 0 {
		return allocation
	}

	// remove allocation
	*sal = append((*sal)[:index], (*sal)[index+1:]...)
	return allocation
}

// Next walks the allocations assuming an offset of start with a limitation of end. It
// returns the next available offset and whether or not the next offset + size exceeds
// the end boundary.
func (sal *sliceAllocationList) Next(start, end, size int) (next int) {
	next = start

	// advance next until next+size doesn't collide with an allocation
	for next < end {
		a := sal.ClosestTo(next)
		if a == nil {
			return
		}

		if next+size < a.Offset {
			return
		}
		next = a.Offset + a.Size + 1
	}

	return
}

// less allows sorting on offset
func (sal *sliceAllocationList) less(i, j int) bool {
	return (*sal)[i].Offset < (*sal)[j].Offset
}
