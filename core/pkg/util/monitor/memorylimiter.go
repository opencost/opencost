package monitor

import (
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/atomic"
	"github.com/opencost/opencost/core/pkg/util/monitor/memory"
)

var (
	once          sync.Once
	memoryLimiter *MemoryLimiter
)

// MemoryLimiter is a heap usage monitor for the go runtime which will attempt to
// dynamically set a GOMEMLIMIT value to best fit the heap usage. It will only
// adjust the GOMEMLIMIT if the usage analysis results in an increase, and won't
// try to "best fit" the current usage. It takes into account the initial GOMEMLIMIT
// value as the baseline.
type MemoryLimiter struct {
	runState atomic.AtomicRunState
	monitor  *memory.MemoryLimitStats
}

// Start begins collecting heap allocation samples for automatically adjusting the go soft memory limit
// for heap usage.
func (ml *MemoryLimiter) Start(interval time.Duration) error {
	ml.runState.WaitForReset()

	if !ml.runState.Start() {
		return fmt.Errorf("memory limiter was already started")
	}

	// main limiter driver
	go func() {
		var memStats runtime.MemStats
		var prevLimit uint64

		// determine if mem limit was set prior by passing a negative
		// value to SetMemoryLimit, which will return the current value
		// without making any changes -- the default is MaxInt64
		goMemLimit := debug.SetMemoryLimit(-1)
		if goMemLimit == math.MaxInt64 {
			prevLimit = 0
		} else {
			prevLimit = uint64(goMemLimit)
		}

		// take initial heap measurement
		runtime.ReadMemStats(&memStats)
		ml.monitor.Record(memStats.HeapAlloc)

		for {
			select {
			case <-ml.runState.OnStop():
				ml.runState.Reset()
				return

			case <-time.After(interval):
			}

			// in the event that someone updates the limit outside of this monitor
			// we want to make sure that we synchronize the correct value
			goMemLimit = debug.SetMemoryLimit(-1)
			if goMemLimit != math.MaxInt64 && goMemLimit != int64(prevLimit) {
				prevLimit = uint64(goMemLimit)
			}

			// record and determine if we should update the memory limit
			runtime.ReadMemStats(&memStats)
			if softLimit, updated := ml.monitor.Record(memStats.HeapAlloc); updated {
				// we only allow the limit to increase for now, as this best reflects a
				// max stable set of samples. Worth observation and potentially updating
				// in the future
				if softLimit != 0 && softLimit > prevLimit {
					prevLimit = softLimit
					log.Debugf("Updating Go Memory Limit: %dmb", int64(softLimit/1024.0/1024.0))
					debug.SetMemoryLimit(int64(softLimit))
				}
			}
		}
	}()

	return nil
}

// Stops automatically adjusting the memory limiter
func (ml *MemoryLimiter) Stop() error {
	if !ml.runState.Stop() {
		return fmt.Errorf("could not stop memory limiter - in the state of stopping or already stopped")
	}
	return nil
}

// returns the singleton instance of the memory limiter
func getMemoryLimiter() *MemoryLimiter {
	once.Do(func() {
		config := memory.DefaultMemoryLimitConfig()
		memoryLimiter = &MemoryLimiter{
			monitor: memory.NewMemoryLimitStats(config),
		}
	})

	return memoryLimiter
}

// DefaultMemoryLimiterSampleInterval is the sample interval in which the auto limiter
// gathers heap usage.
const DefaultMemoryLimiterSampleInterval = time.Second

func StartMemoryLimiter() error {
	return getMemoryLimiter().Start(DefaultMemoryLimiterSampleInterval)
}

func StopMemoryLimiter() error {
	return getMemoryLimiter().Stop()
}
