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
	// Add inserts an allocation into the list before the provided element pointer.
	InsertBefore(allocation *Allocation, element *list.Element) *list.Element

	// AllocationByAddress finds an Allocation matching the provided address and returns
	// it if it exists. Otherwise, nil is returned.
	Remove(address uintptr) (*Allocation, *list.Element)

	// ClosestTo returns the first allocation with an offset greater or equal
	// to the provided offset. It returns nil if such an Allocation does not
	// exist.
	ClosestTo(searchStart *list.Element, offset int) (*Allocation, *list.Element)

	// Next walks the allocations assuming an offset of start with a limitation of end. It
	// returns the next available offset and whether or not the next offset + size exceeds
	// the end boundary.
	Next(searchStart *list.Element, start, end, size int) (next int, element *list.Element)
}

// linkedAllocationList is an implementation of AllocationList using a slice
// of *Allocation
type linkedAllocationList struct {
	l *list.List
}

// NewAllocationList creates a new AllocationList implementation
func NewAllocationList() AllocationList {
	return &linkedAllocationList{
		l: list.New(),
	}
}

// ClosestTo returns the first allocation with an offset greater or equal to the provided offset. It returns
// nil if such an Allocation does not exist.
func (sal *linkedAllocationList) ClosestTo(searchStart *list.Element, offset int) (*Allocation, *list.Element) {
	var start *list.Element
	if searchStart != nil {
		start = searchStart
	} else {
		start = sal.l.Front()
	}

	for e := start; e != nil; e = e.Next() {
		a := e.Value.(*Allocation)
		if a.Offset >= offset {
			return a, e
		}
	}
	return nil, nil
}

// Add inserts an allocation into the list according to it's offset.
// Note: This function does not validate the insertion. Next() should be
// called to find the offset, followed my a call to Add().
func (sal *linkedAllocationList) InsertBefore(alloc *Allocation, element *list.Element) *list.Element {
	if element == nil {
		return sal.l.PushBack(alloc)
	}

	return sal.l.InsertBefore(alloc, element)
}

// AllocationByAddress finds an Allocation matching the provided address and returns
// it if it exists. Otherwise, nil is returned. The element returned represents the
// Next() element after the removed element.
func (sal *linkedAllocationList) Remove(address uintptr) (*Allocation, *list.Element) {
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
		return allocation, nil
	}

	// prev pointer
	prev := element.Prev()

	// remove allocation
	sal.l.Remove(element)
	return allocation, prev
}

// Next walks the allocations assuming an offset of start with a limitation of end. It
// returns the next available offset and whether or not the next offset + size exceeds
// the end boundary.
func (sal *linkedAllocationList) Next(searchStart *list.Element, start, end, size int) (next int, element *list.Element) {
	var a *Allocation
	next = start
	element = searchStart

	// advance next until next+size doesn't collide with an allocation
	for next < end {
		a, element = sal.ClosestTo(element, next)
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
