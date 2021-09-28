package atomic

import "sync/atomic"

type AtomicInt32 int32

// NewAtomicInt32 creates a new atomic int32 instance.
func NewAtomicInt32(value int32) *AtomicInt32 {
	ai := new(AtomicInt32)
	ai.Set(value)
	return ai
}

// Loads the int32 value atomically
func (ai *AtomicInt32) Get() int32 {
	return atomic.LoadInt32((*int32)(ai))
}

// Sets the int32 value atomically
func (ai *AtomicInt32) Set(value int32) {
	atomic.StoreInt32((*int32)(ai), value)
}

// Increments the atomic int and returns the new value
func (ai *AtomicInt32) Increment() int32 {
	return atomic.AddInt32((*int32)(ai), 1)
}

// Decrements the atomint int and returns the new value
func (ai *AtomicInt32) Decrement() int32 {
	return atomic.AddInt32((*int32)(ai), -1)
}

// CompareAndSet sets value to new if current is equal to the current value
func (ai *AtomicInt32) CompareAndSet(current, new int32) bool {
	return atomic.CompareAndSwapInt32((*int32)(ai), current, new)
}
