package collector

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/atomic"
)

type Config struct {
	ScrapeInterval time.Duration
}
type CollectorDriver struct {
	config    Config
	runState  atomic.AtomicRunState
	stop      chan struct{}
	collector MetricsCollector
}

func NewCollectorDriver(config Config) *CollectorDriver {
	return &CollectorDriver{
		collector: NewOpenCostMetricCollector(),
	}
}

func (cd *CollectorDriver) Start() {
	// Before we attempt to start, we must ensure we are not in a stopping state
	cd.runState.WaitForReset()

	// This will atomically check the current state to ensure we can run, then advances the state.
	// If the state is already started, it will return false.
	if !cd.runState.Start() {
		log.Info("collector already running")
		return
	}
	func() {
		for {
			select {
			case <-cd.runState.OnStop():
				cd.runState.Reset()
				return // exit go routine
			default:

			}
			time.Sleep(cd.config.ScrapeInterval)
		}

	}()
}

func (cd *CollectorDriver) Stop() {
	cd.runState.Stop()
}

func (cd *CollectorDriver) scrape() {

}
