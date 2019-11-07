package costmodel_test

import (
	"sync"
	"testing"
	"time"

	"github.com/kubecost/cost-model/costmodel"
)

func TryWait(wg *sync.WaitGroup, t time.Duration) bool {
	done := make(chan bool)
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
		return true
	case <-time.After(t):
		return false
	}
}

func TestTimedJob(t *testing.T) {
	done := make(chan bool)
	wg := new(sync.WaitGroup)
	wg.Add(2)

	fiveSecondJob := costmodel.NewTimedJob(5*time.Second, func() error {
		t.Logf("Completed Five Second Job\n")
		wg.Done()
		return nil
	})

	sevenSecondJob := costmodel.NewTimedJob(7*time.Second, func() error {
		t.Logf("Completed Seven Second Job\n")
		wg.Done()
		return nil
	})

	// Emulate Run Loop
	go func() {
		for {
			fiveSecondJob.Update()
			sevenSecondJob.Update()

			select {
			case <-done:
				return
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	if !TryWait(wg, 10*time.Second) {
		t.Errorf("Failed to trigger wait group within 10 seconds. Test failed")
		return
	}

	close(done)
}
