package worker

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/opencost/opencost/pkg/collections"
)

// Worker is a transformation function from input type T to output type U.
type Worker[T any, U any] func(T) U

// WorkerPool is a pool of go routines executing a Worker on supplied inputs via
// the Run function.
type WorkerPool[T any, U any] interface {
	// Run executes a Worker in the pool on the provided input and onComplete receive chanel
	// to get the results. An error is returned if the pool is shutdown, or is in the process
	// of shutting down.
	Run(input T, onComplete chan<- U) error

	// Shutdown stops all of the workers (if running).
	Shutdown()
}

// WorkGroup is a group of inputs that leverage a WorkerPool to run inputs through workers and
// collect the results in a single slice.
type WorkGroup[T any, U any] interface {
	// Push adds a new input to the work group.
	Push(T) error

	// Wait waits for all pending worker tasks to complete, then returns all the results.
	Wait() []U
}

// entry is an internal helper type for pushing payloads to the worker queue
type entry[T any, U any] struct {
	payload    T
	onComplete chan<- U
	close      bool
}

// queuedWorkerPool is a blocking queue based implementation of a WorkerPool
type queuedWorkerPool[T any, U any] struct {
	queue      collections.BlockingQueue[entry[T, U]]
	work       Worker[T, U]
	workers    int
	isShutdown atomic.Bool
}

// ordered is a WorkGroup implementation which enforces ordering based on when
// inputs were pushed onto the group.
type ordered[T any, U any] struct {
	workPool WorkerPool[T, U]
	results  []U
	wg       *sync.WaitGroup
	count    int
}

// NewWorkerPool creates a new worker pool provided the number of workers to run as well as the worker
// func used to transform inputs to outputs.
func NewWorkerPool[T any, U any](workers int, work Worker[T, U]) WorkerPool[T, U] {
	owq := &queuedWorkerPool[T, U]{
		workers: workers,
		work:    work,
		queue:   collections.NewBlockingQueue[entry[T, U]](),
	}

	// startup the designated workers
	for i := 0; i < workers; i++ {
		go owq.worker()
	}

	return owq
}

// Run executes a Worker in the pool on the provided input and onComplete receive chanel
// to get the results. An error is returned if the pool is shutdown, or is in the process
// of shutting down.
func (wq *queuedWorkerPool[T, U]) Run(input T, onComplete chan<- U) error {
	if wq.isShutdown.Load() {
		return fmt.Errorf("WorkerPoolShutdown")
	}

	wq.queue.Enqueue(entry[T, U]{
		payload:    input,
		onComplete: onComplete,
		close:      false,
	})

	return nil
}

// Shutdown stops all of the workers (if running).
func (wq *queuedWorkerPool[T, U]) Shutdown() {
	if !wq.isShutdown.CompareAndSwap(false, true) {
		return
	}

	for i := 0; i < wq.workers; i++ {
		wq.queue.Enqueue(entry[T, U]{
			close: true,
		})
	}
}

func (wq *queuedWorkerPool[T, U]) worker() {
	for {
		next := wq.queue.Dequeue()

		// shutdown the worker on sentinel value
		if next.close {
			return
		}

		result := wq.work(next.payload)

		// signal on complete if applicable
		if next.onComplete != nil {
			next.onComplete <- result
		}
	}
}

// NewGroup creates a new WorkGroup implementation for processing a group of inputs in the order in which
// they are pushed. Ordered groups do not support concurrent Push() calls.
func NewOrderedGroup[T any, U any](pool WorkerPool[T, U], size int) WorkGroup[T, U] {
	return &ordered[T, U]{
		workPool: pool,
		results:  make([]U, size),
		wg:       new(sync.WaitGroup),
		count:    0,
	}
}

// Push adds a new input to the work group.
func (ow *ordered[T, U]) Push(input T) error {
	current := ow.count
	if current >= len(ow.results) {
		return fmt.Errorf("MaxCapacity")
	}

	onComplete := make(chan U)
	err := ow.workPool.Run(input, onComplete)
	if err != nil {
		return err
	}

	ow.count++
	ow.wg.Add(1)

	go func(index int) {
		defer close(onComplete)

		ow.results[index] = <-onComplete
		ow.wg.Done()
	}(int(current))

	return nil
}

// Wait waits for all pending worker tasks to complete, then returns all the results.
func (ow *ordered[T, U]) Wait() []U {
	ow.wg.Wait()
	return ow.results
}

// these constraints protect against the possibility of unexpected output from runtime.NumCPU()
const (
	defaultMinWorkers = 4
	defaultMaxWorkers = 16
)

// OptimalWorkerCount will return an optimal worker count based on runtime.NumCPU()
func OptimalWorkerCount() int {
	return OptimalWorkerCountInRange(defaultMinWorkers, defaultMaxWorkers)
}

// OptimalWorkerCount will return runtime.NumCPU() constrained to the provided min and max
// range
func OptimalWorkerCountInRange(min int, max int) int {
	cores := runtime.NumCPU()
	if cores < min {
		return min
	}
	if cores > max {
		return max
	}
	return cores
}

// ConcurrentDo runs a pool of workers which concurrently call the provided worker func on each input to get ordered
// output corresponding to the inputs
func ConcurrentDo[T any, U any](worker Worker[T, U], inputs []T) []U {
	workerPool := NewWorkerPool(OptimalWorkerCount(), worker)
	defer workerPool.Shutdown()

	workGroup := NewOrderedGroup(workerPool, len(inputs))
	for _, input := range inputs {
		workGroup.Push(input)
	}

	return workGroup.Wait()
}
