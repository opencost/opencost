package worker

import (
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

type void struct{}

var none = void{}

func waitChannelFor(wg *sync.WaitGroup) <-chan void {
	ch := make(chan void)
	go func() {
		defer close(ch)

		wg.Wait()
		ch <- none
	}()
	return ch
}

func TestWorkerPoolShutdown(t *testing.T) {
	const workers = 3

	// running goroutines
	routines := runtime.NumGoroutine()
	t.Logf("Go Routines Before: %d\n", routines)

	wp := NewWorkerPool(workers, func(any) any { return nil })
	t.Logf("Go Routines After: %d\n", runtime.NumGoroutine())

	wp.Shutdown()
	time.Sleep(time.Second)
	if runtime.NumGoroutine() != routines {
		t.Errorf("Go routines after shutdown: %d != Go routines at start of test: %d\n", runtime.NumGoroutine(), routines)
	}
}

func TestWorkerPoolExactWorkers(t *testing.T) {
	const workers = 3

	// worker func logs start/finish for simulated work
	work := func(i int) void {
		t.Logf("Starting Work: %d\n", i)
		time.Sleep(2 * time.Second)
		t.Logf("Finished Work: %d\n", i)
		return none
	}

	var wg sync.WaitGroup
	wg.Add(workers)

	pool := NewWorkerPool(workers, work)
	for i := 0; i < workers; i++ {
		onComplete := make(chan void)

		go func() {
			defer close(onComplete)

			<-onComplete
			wg.Done()
		}()

		// run work on worker pool
		pool.Run(i+1, onComplete)
	}

	select {
	case <-waitChannelFor(&wg):
	case <-time.After(5 * time.Second):
		t.Errorf("Failed to Complete Run for %d jobs in 5s\n", workers)
	}

}

func TestOrderedWorkGroup(t *testing.T) {
	const workers = 5
	const tasks = 10

	// worker func logs start/finish for simulated work, returns input value
	// for testing resulting group output
	work := func(i int) int {
		t.Logf("Starting Work: %d\n", i)
		time.Sleep(2 * time.Second)
		t.Logf("Finished Work: %d\n", i)
		return i
	}

	pool := NewWorkerPool(workers, work)
	ordered := NewOrderedGroup(pool, tasks)
	input := make([]int, tasks)

	// we create more tasks than workers to test queueing
	for i := 0; i < tasks; i++ {
		input[i] = i + 1
		err := ordered.Push(input[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	// get results and verify they match the recorded inputs
	results := ordered.Wait()
	for i := 0; i < tasks; i++ {
		if results[i] != input[i] {
			t.Errorf("Expected Results[%d](%d) to equal Input[%d](%d)\n", i, results[i], i, input[i])
		}
	}

	// The typical test run will show different tasks starting and stopping out of order (expected),
	// the result collection handles the ordering in the group, which is what we want to ensure in the
	// above assertion
}

func TestConcurrentDoOrdered(t *testing.T) {
	// Perform a similar test to the above ordered test, but use the helper func with pre-built inputs
	const tasks = 50

	// worker func logs start/finish for simulated work, returns input value
	// for testing resulting group output
	work := func(i int) int {
		t.Logf("Starting Work: %d\n", i)
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
		t.Logf("Finished Work: %d\n", i)
		return i
	}

	// pre-build inputs
	input := make([]int, tasks)
	for i := 0; i < tasks; i++ {
		input[i] = i + 1
	}

	// get results and verify they match the recorded inputs
	results := ConcurrentDo(work, input)
	for i := 0; i < tasks; i++ {
		if results[i] != input[i] {
			t.Errorf("Expected Results[%d](%d) to equal Input[%d](%d)\n", i, results[i], i, input[i])
		}
	}

	// The typical test run will show different tasks starting and stopping out of order (expected),
	// the result collection handles the ordering in the group, which is what we want to ensure in the
	// above assertion
}
