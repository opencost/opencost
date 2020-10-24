package memory

import (
	"container/list"
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
	Add(before *list.Element, offset, size int, address uintptr)

	// AllocationByAddress finds an Allocation matching the provided address and returns
	// it if it exists. Otherwise, nil is returned.
	Remove(address uintptr) *Allocation

	// ClosestTo returns the first allocation with an offset greater or equal
	// to the provided offset. It returns nil if such an Allocation does not
	// exist.
	ClosestTo(offset int) (*list.Element, *Allocation)

	// Next walks the allocations assuming an offset of start with a limitation of end. It
	// returns the next available offset and whether or not the next offset + size exceeds
	// the end boundary.
	Next(start, end, size int) (next int, element *list.Element)
}

// sliceAllocationList is an implementation of AllocationList using a slice
// of *Allocation
type sliceAllocationList struct {
	l *list.List
}

// NewAllocationList creates a new AllocationList implementation
func NewAllocationList() AllocationList {
	return &sliceAllocationList{
		l: list.New(),
	}
}

// ClosestTo returns the first allocation with an offset greater or equal
// to the provided offset. It returns nil if such an Allocation does not
// exist.
func (sal *sliceAllocationList) ClosestTo(offset int) (*list.Element, *Allocation) {
	for e := sal.l.Front(); e != nil; e = e.Next() {
		a := e.Value.(*Allocation)
		if a.Offset >= offset {
			return e, a
		}
	}
	return nil, nil
}

// Add inserts an allocation into the list according to it's offset.
// Note: This function does not validate the insertion. Next() should be
// called to find the offset, followed my a call to Add().
func (sal *sliceAllocationList) Add(before *list.Element, offset, size int, address uintptr) {
	alloc := &Allocation{
		Offset: offset,
		Size:   size,
		Addr:   address,
	}

	if before == nil {
		sal.l.PushBack(alloc)
	} else {
		sal.l.InsertBefore(alloc, before)
	}
}

// AllocationByAddress finds an Allocation matching the provided address and returns
// it if it exists. Otherwise, nil is returned.
func (sal *sliceAllocationList) Remove(address uintptr) *Allocation {
	var allocation *Allocation = nil
	var element *list.Element

	for e := sal.l.Front(); e != nil; e = e.Next() {
		a := e.Value.(*Allocation)
		if a.Addr == address {
			allocation = a
			element = e
			break
		}
	}

	// Not found, return nil allocation
	if element == nil {
		return allocation
	}

	// remove allocation
	sal.l.Remove(element)
	return allocation
}

// Next walks the allocations assuming an offset of start with a limitation of end. It
// returns the next available offset and whether or not the next offset + size exceeds
// the end boundary.
func (sal *sliceAllocationList) Next(start, end, size int) (next int, element *list.Element) {
	var a *Allocation
	next = start

	// advance next until next+size doesn't collide with an allocation
	for next < end {
		element, a = sal.ClosestTo(next)
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
