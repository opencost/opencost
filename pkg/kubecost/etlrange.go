package kubecost

import (
	"fmt"
	"sync"

	"github.com/opencost/opencost/pkg/util/json"
)

// SetRange is a generic implementation of the SetRanges that act as containers. It covers the basic functionality that
// is shared by the basic types but is meant to be extended by each implementation.
type SetRange[T ETLSet] struct {
	lock sync.RWMutex
	sets []T
}

// Append attaches the given ETLSet to the end of the sets slice.
// currently does not check that the window is correct.
func (r *SetRange[T]) Append(that T) {
	if r == nil {
		return
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	r.sets = append(r.sets, that)
}

// Each invokes the given function for each ETLSet in the SetRange
func (r *SetRange[T]) Each(f func(int, T)) {
	if r == nil {
		return
	}

	for i, set := range r.sets {
		f(i, set)
	}
}

// Get retrieves the given index from the sets slice
func (r *SetRange[T]) Get(i int) (T, error) {
	var set T
	if r == nil {
		return set, fmt.Errorf("SetRange: Get: is nil")
	}
	if i < 0 || i >= len(r.sets) {

		return set, fmt.Errorf("SetRange: Get: index out of range: %d", i)
	}

	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.sets[i], nil
}

// Length returns the length of the sets slice
func (r *SetRange[T]) Length() int {
	if r == nil || r.sets == nil {
		return 0
	}

	r.lock.RLock()
	defer r.lock.RUnlock()
	return len(r.sets)
}

// IsEmpty returns false if SetRange contains a single ETLSet that is not empty
func (r *SetRange[T]) IsEmpty() bool {
	if r == nil || r.Length() == 0 {
		return true
	}
	r.lock.RLock()
	defer r.lock.RUnlock()

	for _, set := range r.sets {
		if !set.IsEmpty() {
			return false
		}
	}
	return true
}

// MarshalJSON converts SetRange to JSON
func (r *SetRange[T]) MarshalJSON() ([]byte, error) {
	if r == nil {
		return json.Marshal([]T{})
	}
	r.lock.RLock()
	defer r.lock.RUnlock()
	return json.Marshal(r.sets)
}
