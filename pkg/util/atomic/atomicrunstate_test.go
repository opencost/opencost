package atomic

import (
	"testing"
	"time"
)

// NOTE: This test uses time.Sleep() in an attempt to specifically schedule concurrent actions for testing
// NOTE: Testing concurrency is hard, so if there are inconsistent results, make sure it's not just the timing
// NOTE: of the test on the testing hardware.
func TestRunState(t *testing.T) {
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
