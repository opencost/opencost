package atomic

import (
	"sync/atomic"
)

// AtomicBool alias leverages a 32-bit integer CAS
type AtomicBool int32

// NewAtomicBool creates an AtomicBool with given default value
func NewAtomicBool(value bool) *AtomicBool {
	ab := new(AtomicBool)
	ab.Set(value)
	return ab
}

// Loads the bool value atomically
func (ab *AtomicBool) Get() bool {
	return atomic.LoadInt32((*int32)(ab)) != 0
}

// Sets the bool value atomically
func (ab *AtomicBool) Set(value bool) {
	if value {
		atomic.StoreInt32((*int32)(ab), 1)
	} else {
		atomic.StoreInt32((*int32)(ab), 0)
	}
}

// CompareAndSet sets value to new if current is equal to the current value. If the new value is
// set, this function returns true.
func (ab *AtomicBool) CompareAndSet(current, new bool) bool {
	var o, n int32
	if current {
		o = 1
	}
	if new {
		n = 1
	}
	return atomic.CompareAndSwapInt32((*int32)(ab), o, n)
}
