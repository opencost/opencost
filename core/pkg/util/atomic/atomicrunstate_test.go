package atomic

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// NOTE: This test uses time.Sleep() in an attempt to specifically schedule concurrent actions for testing
// NOTE: Testing concurrency is hard, so if there are inconsistent results, make sure it's not just the timing
// NOTE: of the test on the testing hardware.
func TestRunState(t *testing.T) {
	t.Parallel()

	var ars AtomicRunState

	if !ars.Start() {
		t.Fatalf("Failed to Start() AtomicRunState")
	}

	if ars.Start() {
		t.Fatalf("Started AtomicRunState a second time")
	}

	success := make(chan bool)

	go func() {
		cycles := 0
		for {
			// Our test expects exactly 1 cycle, so if we exceed that, we fail!
			if cycles >= 2 {
				success <- false
				return
			}
			// create a "work" time before the select
			time.Sleep(1 * time.Second)

			select {
			case <-ars.OnStop():
				t.Logf("Stopped\n")
				ars.Reset()
				success <- true
				return
			case <-time.After(2 * time.Second):
				t.Logf("Tick\n")
			}
			cycles++
		}
	}()

	// Wait for one full work cycle (3 seconds), attempt Stop during "work" phase
	time.Sleep(3500 * time.Millisecond)
	ars.Stop()

	result := <-success
	if !result {
		t.Fatalf("Executed too many work cycles, expected 1 cycle")
	}
}

// leaks goroutines potentially, so only use in testing!
func waitChannelFor(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		ch <- true
	}()
	return ch
}

func TestDoubleWait(t *testing.T) {
	t.Parallel()

	var ars AtomicRunState

	ars.WaitForReset()

	if !ars.Start() {
		t.Fatalf("Failed to Start() AtomicRunState")
	}

	if ars.Start() {
		t.Fatalf("Started AtomicRunState a second time")
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		t.Logf("GoRoutine 1 Waiting....")
		<-ars.OnStop()
		wg.Done()
	}()

	go func() {
		t.Logf("GoRoutine 2 Waiting....")
		<-ars.OnStop()
		wg.Done()
	}()

	time.Sleep(1 * time.Second)
	ars.Stop()
	select {
	case <-time.After(time.Second):
		t.Fatalf("Did not receive signal from both go routines after a second\n")
		return
	case <-waitChannelFor(&wg):
		t.Logf("Received signals from both go routines\n")
	}
	ars.Reset()
}

func TestContinuousConcurrentStartsAndStops(t *testing.T) {
	t.Parallel()

	const cycles = 5

	var ars AtomicRunState
	started := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(cycles)

	// continuously try and start the ars on a tight loop
	// throttled by OnStop and WaitForReset()
	go func() {
		c := cycles
		for c > 0 {
			ars.WaitForReset()
			if ars.Start() {
				t.Logf("Started")
				if c == cycles {
					started <- true
				}
				c--
			}
		}
	}()

	// wait for an initial start
	<-started

	// Loop Stop from other goroutines
	go func() {
		c := cycles
		for c > 0 {
			time.Sleep(100 * time.Millisecond)
			if ars.Stop() {
				t.Logf("Wait for stop")
				c--
			}
		}
	}()

	// Loop OnStop and Resets
	go func() {
		c := cycles

		time.Sleep(150 * time.Millisecond)
		for c > 0 {
			<-ars.OnStop()
			t.Logf("Stopped")
			time.Sleep(500 * time.Millisecond)
			ars.Reset()
			c--
			wg.Done()
		}
	}()

	// Wait for full cycles
	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("Didn't complete %d cycles after 10 seconds", cycles)
	case <-waitChannelFor(&wg):
		t.Logf("Completed!")
	}
}

func TestStopChannelWhenStopped(t *testing.T) {
	t.Parallel()

	// This scenario is a bit odd, but there was a bug where waiting on `OnStop()`
	// before the run state is started will indefinitely block. The problem is resolved by
	// buffering the stop channel with intermediate channels until Start() is called.

	var ars AtomicRunState

	finished := make(chan struct{})
	errors := make(chan error)

	go func() {
		<-ars.OnStop()
		t.Logf("Stopped")
		finished <- struct{}{}
	}()

	// wait a bit, then start and stop the run state -- the OnStop
	// channel should complete.
	go func() {
		time.Sleep(1 * time.Second)
		ars.WaitForReset()

		if !ars.Start() {
			errors <- fmt.Errorf("Failed to Start() AtomicRunState")
		}
		time.Sleep(500 * time.Millisecond)

		if !ars.Stop() {
			errors <- fmt.Errorf("Failed to Stop() AtomicRunState")
		}
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("Didn't complete after 5 seconds")
	case e := <-errors:
		t.Fatalf("Received error from goroutine: %s", e)
	case <-finished:
		t.Logf("Completed!")
	}

}
