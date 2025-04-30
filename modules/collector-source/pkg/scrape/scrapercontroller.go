package scrape

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/atomic"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

// ScraperController initializes and holds the scrapers in addition to running the loop that triggers scrapes
type ScraperController struct {
	scrapeInterval time.Duration
	runState       atomic.AtomicRunState
	scrapers       []Scraper
}

func NewScraperController(scrapeInterval time.Duration, updater metric.MetricUpdater) *ScraperController {
	var scrapers []Scraper
	// TODO init scrapers

	sc := &ScraperController{
		scrapeInterval: scrapeInterval,
		scrapers:       scrapers,
	}
	sc.Start()
	return sc
}

func (sc *ScraperController) Start() {
	// Before we attempt to start, we must ensure we are not in a stopping state
	sc.runState.WaitForReset()

	// This will atomically check the current state to ensure we can run, then advances the state.
	// If the state is already started, it will return false.
	if !sc.runState.Start() {
		log.Info("metric already running")
		return
	}
	func() {
		for {
			select {
			case <-sc.runState.OnStop():
				sc.runState.Reset()
				return // exit go routine
			default:

			}
			time.Sleep(sc.scrapeInterval)
		}

	}()
}

func (sc *ScraperController) Stop() {
	sc.runState.Stop()
}
