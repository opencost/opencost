package collections

import (
	"sync"
)

//--------------------------------------------------------------------------
//  BlockingQueue
//--------------------------------------------------------------------------

// BlockingQueue is a queue backed by a slice which blocks if dequeueing while empty.
// This data structure should use a pool of worker goroutines to await work.
type BlockingQueue interface {
	// Enqueue pushes an item onto the queue
	Enqueue(item interface{})

	// Dequeue removes the first item from the queue and returns it.
	Dequeue() interface{}

	// Each blocks modification and allows iteration of the queue.
	Each(f func(int, interface{}))

	// Length returns the length of the queue
	Length() int

	// IsEmpty returns true if the queue is empty
	IsEmpty() bool
}

// blockingSliceQueue is an implementation of BlockingQueue which uses a slice for storage.
type blockingSliceQueue struct {
	q        []interface{}
	l        *sync.Mutex
	nonEmpty *sync.Cond
}

// NewBlockingQueue returns a new BlockingQueue implementation
func NewBlockingQueue() BlockingQueue {
	l := new(sync.Mutex)

	return &blockingSliceQueue{
		q:        []interface{}{},
		l:        l,
		nonEmpty: sync.NewCond(l),
	}
}

// Enqueue pushes an item onto the queue
func (q *blockingSliceQueue) Enqueue(item interface{}) {
	q.l.Lock()
	defer q.l.Unlock()

	q.q = append(q.q, item)
	q.nonEmpty.Broadcast()
}

// Dequeue removes the first item from the queue and returns it.
func (q *blockingSliceQueue) Dequeue() interface{} {
	q.l.Lock()
	defer q.l.Unlock()

	// need to tight loop here to ensure only one thread wins and
	// others wait again
	for len(q.q) == 0 {
		q.nonEmpty.Wait()
	}

	e := q.q[0]

	// nil 0 index to prevent leak
	q.q[0] = nil
	q.q = q.q[1:]
	return e
}

// Each blocks modification and allows iteration of the queue.
func (q *blockingSliceQueue) Each(f func(int, interface{})) {
	q.l.Lock()
	defer q.l.Unlock()

	for i, entry := range q.q {
		f(i, entry)
	}
}

// Length returns the length of the queue
func (q *blockingSliceQueue) Length() int {
	q.l.Lock()
	defer q.l.Unlock()

	return len(q.q)
}

// IsEmpty returns true if the queue is empty
func (q *blockingSliceQueue) IsEmpty() bool {
	return q.Length() == 0
}
