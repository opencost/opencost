package collector

import (
	"sync/atomic"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

type Config struct {
	ScrapeInterval time.Duration
}
type CollectorDriver struct {
	config     Config
	isRunning  atomic.Bool
	isStopping atomic.Bool
	stop       chan struct{}
	collector  MetricsCollector
}

func NewCollectorDriver(config Config) *CollectorDriver {
	return &CollectorDriver{
		collector: NewOpenCostMetricCollector(),
	}
}

func (cd *CollectorDriver) Start() {
	wasRunning := cd.isRunning.Swap(true)
	if wasRunning {
		log.Info("collector already running")
		return
	}
	func() {
		for {
			select {
			case <-cd.stop:
				cd.isRunning.Store(false)
				return
			default:

			}
			time.Sleep(cd.config.ScrapeInterval)
		}

	}()
}

func (cd *CollectorDriver) Stop() {
	if !cd.isRunning.Load() {
		log.Info("collector already stopped")
		return
	}
	wasStopping := cd.isStopping.Swap(true)
	if wasStopping {
		log.Info("collector already stopping")
		return
	}
	cd.isStopping.Store(true)
	cd.stop <- struct{}{}
	cd.isRunning.Store(false)

}

func (cd *CollectorDriver) scrape() {
	
}
